from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "adapters" / "hermes"))

import jcode_graph  # noqa: E402
from jcode_graph import (  # noqa: E402
    BrokerSocketClient,
    JcodeGraphMemoryProvider,
    _default_socket_path,
)


class FakeBrokerServer:
    def __init__(self, *, context_items: list[dict] | None = None) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.socket_path = str(Path(self._tmp.name) / "broker.sock")
        self.context_items = context_items
        self.requests: list[dict] = []
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "FakeBrokerServer":
        self._thread.start()
        self._ready.wait(timeout=2)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.connect(self.socket_path)
        except Exception:
            pass
        self._thread.join(timeout=2)
        self._tmp.cleanup()

    def _run(self) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(self.socket_path)
            server.listen(1)
            self._ready.set()
            while not self._stop.is_set():
                try:
                    conn, _ = server.accept()
                except OSError:
                    return
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            handle = conn.makefile("rwb", buffering=0)
            while not self._stop.is_set():
                raw = handle.readline()
                if not raw:
                    return
                request = json.loads(raw.decode("utf-8"))
                self.requests.append(request)
                request_id = request["id"]
                if request["type"] == "subscribe":
                    self._write(handle, {"type": "done", "id": request_id})
                elif request["type"] == "broker_context":
                    self._write(
                        handle,
                        {
                            "type": "broker_context",
                            "id": request_id,
                            "session_id": "ses_fake",
                            "working_dir": "/tmp/project",
                            "tool_names": ["goal", "memory"],
                            "items": self.context_items
                            if self.context_items is not None
                            else [
                                {
                                    "id": "mem_1",
                                    "kind": "memory",
                                    "scope": "project",
                                    "title": "Project Memory",
                                    "summary": "Keep context provenance visible.",
                                    "origin": {"tool": "memory"},
                                },
                                {
                                    "id": "todo_1",
                                    "kind": "todo",
                                    "scope": "session",
                                    "title": "Wire Hermes adapter",
                                    "summary": "pending/high",
                                    "origin": {"tool": "todo"},
                                },
                            ],
                        },
                    )
                elif request["type"] == "broker_turn_sync":
                    self._write(
                        handle,
                        {
                            "type": "broker_turn_synced",
                            "id": request_id,
                            "session_id": request.get("session_id") or "ses_fake",
                            "memory_ids": ["mem_turn_1"],
                        },
                    )
                elif request["type"] == "broker_transcript_sync":
                    self._write(
                        handle,
                        {
                            "type": "broker_transcript_synced",
                            "id": request_id,
                            "session_id": request.get("session_id") or "ses_fake",
                            "memory_ids": ["mem_transcript_1"],
                            "provenance_memory_ids": ["mem_transcript_1"],
                            "derived_memory_ids": [],
                            "extraction_status": "skipped_sidecar_disabled",
                        },
                    )

    @staticmethod
    def _write(handle, event: dict) -> None:
        handle.write(json.dumps(event).encode("utf-8") + b"\n")


class BrokerSocketClientTests(unittest.TestCase):
    def test_client_subscribes_and_fetches_broker_context(self) -> None:
        with FakeBrokerServer() as server:
            client = BrokerSocketClient(server.socket_path, working_dir="/tmp/project")
            event = client.broker_context(query="project memory", limit=3)
            client.close()

        self.assertEqual(event["type"], "broker_context")
        self.assertEqual(event["items"][0]["kind"], "memory")
        self.assertEqual(server.requests[0]["type"], "subscribe")
        self.assertEqual(server.requests[0]["working_dir"], "/tmp/project")
        self.assertEqual(server.requests[1]["type"], "broker_context")
        self.assertEqual(server.requests[1]["query"], "project memory")
        self.assertEqual(server.requests[1]["limit"], 3)

    def test_client_syncs_transcript_to_broker(self) -> None:
        with FakeBrokerServer() as server:
            client = BrokerSocketClient(server.socket_path, working_dir="/tmp/project")
            event = client.broker_transcript_sync(
                session_id="hermes_session",
                transcript="user: remember transcript extraction",
                source="hermes:session_end",
            )
            client.close()

        self.assertEqual(event["type"], "broker_transcript_synced")
        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertEqual(transcript_requests[0]["session_id"], "hermes_session")
        self.assertEqual(
            transcript_requests[0]["transcript"],
            "user: remember transcript extraction",
        )
        self.assertEqual(transcript_requests[0]["source"], "hermes:session_end")


class RuntimePathTests(unittest.TestCase):
    def test_default_socket_prefers_jcode_runtime_dir(self) -> None:
        previous = {
            "JCODE_BROKER_SOCKET": os.environ.get("JCODE_BROKER_SOCKET"),
            "JCODE_RUNTIME_DIR": os.environ.get("JCODE_RUNTIME_DIR"),
            "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR"),
        }
        try:
            os.environ.pop("JCODE_BROKER_SOCKET", None)
            os.environ["JCODE_RUNTIME_DIR"] = "/tmp/jcode-smoke-runtime"
            os.environ["XDG_RUNTIME_DIR"] = "/tmp/xdg-runtime"

            self.assertEqual(
                _default_socket_path(),
                "/tmp/jcode-smoke-runtime/jcode-broker.sock",
            )
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_default_socket_accepts_explicit_override(self) -> None:
        previous = os.environ.get("JCODE_BROKER_SOCKET")
        try:
            os.environ["JCODE_BROKER_SOCKET"] = "/tmp/custom-broker.sock"
            self.assertEqual(_default_socket_path(), "/tmp/custom-broker.sock")
        finally:
            if previous is None:
                os.environ.pop("JCODE_BROKER_SOCKET", None)
            else:
                os.environ["JCODE_BROKER_SOCKET"] = previous


class JcodeGraphMemoryProviderTests(unittest.TestCase):
    def test_provider_formats_prefetch_context(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 4,
                }
            )
            self.assertTrue(provider.is_available())
            provider.initialize("hermes_session")
            text = provider.prefetch("project memory")
            provider.shutdown()

        self.assertIn("## jcode Broker Context", text)
        self.assertIn("[memory/project/memory] Project Memory", text)
        self.assertIn("[todo/session/todo] Wire Hermes adapter", text)

    def test_provider_formats_phase_3_context_by_kind_and_budget(self) -> None:
        items = [
            {
                "id": "tool_memory",
                "kind": "tool",
                "scope": "session",
                "title": "memory",
                "summary": "broker tool",
                "origin": {"tool": "tool_registry"},
            },
            {
                "id": "skill_phase",
                "kind": "skill",
                "scope": "project",
                "title": "phase-three-skill",
                "summary": "Use this skill when formatting broker context.",
                "origin": {"tool": "skill_registry", "path": "/tmp/project/.jcode/skills/phase"},
            },
            {
                "id": "hit_prior",
                "kind": "session_search_hit",
                "scope": "project",
                "title": "assistant match in prior session",
                "summary": "Prior session evidence.",
                "content": "Prior session evidence " * 20,
                "origin": {"tool": "session_search", "session_id": "ses_prior"},
                "relevance": {"rank": 2, "query": "phase three"},
            },
            {
                "id": "todo_1",
                "kind": "todo",
                "scope": "session",
                "title": "Finish adapter parity",
                "summary": "pending/high",
                "origin": {"tool": "todo"},
            },
            {
                "id": "goal_1",
                "kind": "goal",
                "scope": "session",
                "title": "Adapter parity",
                "summary": "Keep Hermes formatting compact.",
                "origin": {"tool": "goal"},
            },
            {
                "id": "mem_provenance",
                "kind": "memory",
                "scope": "project",
                "title": "provenance",
                "summary": "RAW TRANSCRIPT SHOULD NOT APPEAR",
                "content": "RAW TRANSCRIPT SHOULD NOT APPEAR",
                "tags": ["broker-provenance"],
                "metadata": {"provenance": True},
                "origin": {"tool": "memory"},
            },
            {
                "id": "mem_1",
                "kind": "memory",
                "scope": "project",
                "title": "Preference",
                "summary": "Rob likes context evidence with source hints.",
                "origin": {"tool": "memory", "source": "derived:hermes"},
                "relevance": {"rank": 1, "retrieval_mode": "semantic_cascade"},
            },
        ]
        with FakeBrokerServer(context_items=items) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 8,
                    "max_chars": 700,
                    "item_max_chars": 80,
                    "tool_inventory_limit": 2,
                }
            )
            provider.initialize("hermes_session")
            text = provider.prefetch("phase three", session_id="hermes_session")
            diagnostics = provider.diagnostics()
            provider.shutdown()

        self.assertIn("### Memories", text)
        self.assertIn("### Goals and Todos", text)
        self.assertIn("### Evidence", text)
        self.assertIn("### Skill Candidates", text)
        self.assertIn("### Broker Tools", text)
        self.assertLess(text.index("### Memories"), text.index("### Goals and Todos"))
        self.assertLess(text.index("### Goals and Todos"), text.index("### Evidence"))
        self.assertIn("rank=1", text)
        self.assertIn("semantic_cascade", text)
        self.assertIn("session=ses_prior", text)
        self.assertIn("phase-three-skill", text)
        self.assertIn("memory", text)
        self.assertNotIn("RAW TRANSCRIPT SHOULD NOT APPEAR", text)
        self.assertLessEqual(diagnostics["last_prefetch_chars"], 700)
        self.assertEqual(diagnostics["last_prefetch_item_count"], len(items))

    def test_provider_exposes_context_tool_schema(self) -> None:
        provider = JcodeGraphMemoryProvider({"socket_path": os.devnull})
        schemas = provider.get_tool_schemas()
        self.assertEqual(schemas[0]["name"], "jcode_broker_context")

    def test_provider_availability_uses_configured_jcode_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake_binary = Path(tmp) / "jcode"
            fake_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake_binary.chmod(0o755)
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": str(Path(tmp) / "missing.sock"),
                    "jcode_binary": str(fake_binary),
                }
            )

            self.assertTrue(provider.is_available())

    def test_provider_sync_turn_writes_to_broker(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session")
            provider.sync_turn(
                "Remember that Hermes can write through the broker.",
                "Acknowledged and synced.",
                session_id="hermes_session",
            )
            provider.shutdown()

        sync_requests = [
            request for request in server.requests if request["type"] == "broker_turn_sync"
        ]
        self.assertEqual(len(sync_requests), 1)
        self.assertIsNone(sync_requests[0]["session_id"])
        self.assertEqual(
            sync_requests[0]["user_content"],
            "Remember that Hermes can write through the broker.",
        )
        self.assertEqual(sync_requests[0]["assistant_content"], "Acknowledged and synced.")
        self.assertEqual(sync_requests[0]["source"], "hermes")
        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(transcript_requests, [])

    def test_provider_rotates_client_when_hermes_session_changes(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session_a")
            provider.sync_turn("user a", "assistant a", session_id="hermes_session_a")
            provider.sync_turn("user b", "assistant b", session_id="hermes_session_b")
            provider.shutdown()

        subscribe_requests = [
            request for request in server.requests if request["type"] == "subscribe"
        ]
        self.assertEqual(len(subscribe_requests), 2)
        sync_requests = [
            request for request in server.requests if request["type"] == "broker_turn_sync"
        ]
        self.assertEqual([request["user_content"] for request in sync_requests], ["user a", "user b"])

    def test_provider_pre_compress_syncs_transcript(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session")
            result = provider.on_pre_compress(
                [
                    {"role": "system", "content": "ignore system"},
                    {"role": "user", "content": "Remember transcript hooks."},
                    {"role": "assistant", "content": "The broker should extract later."},
                ]
            )
            provider.shutdown()

        self.assertEqual(result, "")
        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertIn("user: Remember transcript hooks.", transcript_requests[0]["transcript"])
        self.assertIn(
            "assistant: The broker should extract later.",
            transcript_requests[0]["transcript"],
        )
        self.assertEqual(transcript_requests[0]["source"], "hermes:pre_compress")

    def test_provider_session_end_syncs_transcript(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session")
            provider.on_session_end(
                [
                    {"role": "user", "content": [{"text": "Session ending memory."}]},
                    {"role": "assistant", "content": "Flush the transcript."},
                ]
            )
            provider.shutdown()

        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertIn("user: Session ending memory.", transcript_requests[0]["transcript"])
        self.assertIn("assistant: Flush the transcript.", transcript_requests[0]["transcript"])
        self.assertEqual(transcript_requests[0]["source"], "hermes:session_end")

    def test_provider_session_end_uses_bounded_turn_buffer_when_messages_missing(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "turn_buffer_limit": 2,
                    "turn_buffer_max_chars": 24,
                    "transcript_max_chars": 120,
                }
            )
            provider.initialize("hermes_session")
            provider.sync_turn("first user message should roll out", "first assistant", session_id="hermes_session")
            provider.sync_turn("second user message should remain", "second assistant", session_id="hermes_session")
            provider.sync_turn("third user message should remain", "third assistant", session_id="hermes_session")
            provider.on_session_end([])
            diagnostics = provider.diagnostics()
            provider.shutdown()

        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        transcript = transcript_requests[0]["transcript"]
        self.assertNotIn("first user message", transcript)
        self.assertIn("second user message", transcript)
        self.assertIn("third user message", transcript)
        self.assertLessEqual(len(transcript), 120)
        self.assertEqual(diagnostics["transcript_sync_count"], 1)
        self.assertLessEqual(diagnostics["last_transcript_chars"], 120)

    def test_provider_context_tool_can_request_provenance_explicitly(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session")
            payload = provider.handle_tool_call(
                "jcode_broker_context",
                {"query": "raw provenance", "limit": 4, "include_provenance": True},
            )
            provider.shutdown()

        self.assertEqual(json.loads(payload)["type"], "broker_context")
        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(context_requests[-1]["include_provenance"], True)

    def test_provider_can_disable_transcript_sync(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "sync_transcripts": False,
                }
            )
            provider.initialize("hermes_session")
            provider.on_pre_compress([{"role": "user", "content": "do not sync"}])
            provider.on_session_end([{"role": "user", "content": "do not sync"}])
            provider.shutdown()

        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(transcript_requests, [])

    def test_provider_unavailable_broker_degrades_quietly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": str(Path(tmp) / "missing.sock"),
                    "auto_start": False,
                }
            )
            self.assertFalse(provider.is_available())
            provider.initialize("hermes_session")

            self.assertEqual(provider.prefetch("project memory"), "")
            provider.sync_turn("user", "assistant", session_id="hermes_session")
            self.assertEqual(
                provider.on_pre_compress([{"role": "user", "content": "compress"}]),
                "",
            )
            provider.on_session_end([{"role": "user", "content": "session end"}])
            payload = provider.handle_tool_call("jcode_broker_context", {})
            provider.shutdown()

        self.assertEqual(json.loads(payload)["error"], "jcode broker context unavailable")

    def test_provider_auto_starts_broker_when_socket_is_missing(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.terminated = False

            def poll(self):
                return None

            def terminate(self) -> None:
                self.terminated = True

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as tmp:
            fake_binary = Path(tmp) / "jcode"
            fake_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake_binary.chmod(0o755)
            socket_path = str(Path(tmp) / "broker.sock")
            fake_process = FakeProcess()

            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": socket_path,
                    "jcode_binary": str(fake_binary),
                    "startup_timeout_seconds": 0,
                }
            )

            with unittest.mock.patch.object(
                jcode_graph.subprocess, "Popen", return_value=fake_process
            ) as popen:
                provider.initialize("hermes_session", working_dir="/tmp/project")
                provider.shutdown()

            popen.assert_called_once()
            command = popen.call_args.args[0]
            self.assertEqual(command[:3], [str(fake_binary), "broker", "serve"])
            self.assertIn("--socket", command)
            self.assertIn(socket_path, command)
            self.assertIn("--quiet", command)
            self.assertEqual(
                popen.call_args.kwargs["env"]["JCODE_RUNTIME_DIR"],
                str(Path(socket_path).parent),
            )
            self.assertEqual(
                popen.call_args.kwargs["env"]["JCODE_NON_INTERACTIVE"],
                "1",
            )
            self.assertTrue(fake_process.terminated)

    def test_provider_auto_start_passes_configured_duckdb_path(self) -> None:
        class FakeProcess:
            def poll(self):
                return None

            def terminate(self) -> None:
                pass

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as tmp:
            fake_binary = Path(tmp) / "jcode"
            fake_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake_binary.chmod(0o755)
            duckdb_path = str(Path(tmp) / "broker.duckdb")

            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": str(Path(tmp) / "broker.sock"),
                    "jcode_binary": str(fake_binary),
                    "duckdb_path": duckdb_path,
                    "startup_timeout_seconds": 0,
                }
            )

            with unittest.mock.patch.object(
                jcode_graph.subprocess, "Popen", return_value=FakeProcess()
            ) as popen:
                provider.initialize("hermes_session", working_dir="/tmp/project")
                provider.shutdown()

            self.assertEqual(
                popen.call_args.kwargs["env"]["JCODE_BROKER_DUCKDB_PATH"],
                duckdb_path,
            )

    def test_provider_auto_start_passes_isolated_jcode_home(self) -> None:
        class FakeProcess:
            def poll(self):
                return None

            def terminate(self) -> None:
                pass

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as tmp:
            fake_binary = Path(tmp) / "jcode"
            fake_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake_binary.chmod(0o755)
            jcode_home = str(Path(tmp) / "jcode-home")

            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": str(Path(tmp) / "broker.sock"),
                    "jcode_binary": str(fake_binary),
                    "jcode_home": jcode_home,
                    "disable_telemetry": True,
                    "startup_timeout_seconds": 0,
                }
            )

            with unittest.mock.patch.object(
                jcode_graph.subprocess, "Popen", return_value=FakeProcess()
            ) as popen:
                provider.initialize("hermes_session", working_dir="/tmp/project")
                provider.shutdown()

            env = popen.call_args.kwargs["env"]
            self.assertEqual(env["JCODE_HOME"], jcode_home)
            self.assertEqual(env["JCODE_NO_TELEMETRY"], "1")


if __name__ == "__main__":
    unittest.main()

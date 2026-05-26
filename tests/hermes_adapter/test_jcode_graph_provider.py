from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
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
    _prefetch_focus_query,
    _prefetch_should_query_broker,
)


def _add_default_hermes_repo_to_path() -> bool:
    configured = os.environ.get("HERMES_AGENT_REPO")
    candidates = (
        [Path(configured)]
        if configured
        else [
            Path.home() / ".hermes" / "hermes-agent-v0.13.0",
            Path.home() / ".hermes" / "hermes-agent",
            Path.home() / ".hermes" / "hermes-agent-v2026.4.30-acp-validation",
        ]
    )
    for candidate in candidates:
        if candidate and candidate.exists():
            sys.path.insert(0, str(candidate))
            return True
    return False


class FakeBrokerServer:
    def __init__(
        self,
        *,
        context_items: list[dict] | None = None,
        context_packet: dict | None = None,
        context_delay_seconds: float = 0.0,
        hang_context_once: bool = False,
        hang_turn_sync: bool = False,
        turn_sync_error: str | None = None,
        hang_transcript_sync: bool = False,
    ) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.socket_path = str(Path(self._tmp.name) / "broker.sock")
        self.context_items = context_items
        self.context_packet = context_packet
        self.context_delay_seconds = context_delay_seconds
        self.hang_context_once = hang_context_once
        self._hung_context_requests = 0
        self.hang_turn_sync = hang_turn_sync
        self.turn_sync_error = turn_sync_error
        self.hang_transcript_sync = hang_transcript_sync
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
                    if self.hang_context_once and self._hung_context_requests == 0:
                        self._hung_context_requests += 1
                        while not self._stop.wait(0.05):
                            pass
                        return
                    if self.context_delay_seconds:
                        time.sleep(self.context_delay_seconds)
                    event = {
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
                        }
                    if self.context_packet is not None:
                        event["packet"] = self.context_packet
                    self._write(handle, event)
                elif request["type"] == "broker_turn_sync":
                    if self.hang_turn_sync:
                        while not self._stop.wait(0.05):
                            pass
                        return
                    if self.turn_sync_error:
                        self._write(
                            handle,
                            {
                                "type": "error",
                                "id": request_id,
                                "message": self.turn_sync_error,
                            },
                        )
                        continue
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
                    if self.hang_transcript_sync:
                        while not self._stop.wait(0.05):
                            pass
                        return
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


def _wait_for_requests(
    server: FakeBrokerServer,
    request_type: str,
    *,
    count: int = 1,
    timeout: float = 1.0,
) -> list[dict]:
    deadline = time.monotonic() + timeout
    matches: list[dict] = []
    while time.monotonic() < deadline:
        matches = [request for request in server.requests if request["type"] == request_type]
        if len(matches) >= count:
            return matches
        time.sleep(0.01)
    return [request for request in server.requests if request["type"] == request_type]


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
                surface_session_id="hermes_surface_session",
                parent_segment_id="hermes_parent_session",
                surface="hermes",
            )
            client.close()

        self.assertEqual(event["type"], "broker_transcript_synced")
        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertEqual(transcript_requests[0]["session_id"], "hermes_session")
        self.assertEqual(
            transcript_requests[0]["surface_session_id"], "hermes_surface_session"
        )
        self.assertEqual(
            transcript_requests[0]["parent_segment_id"], "hermes_parent_session"
        )
        self.assertEqual(transcript_requests[0]["surface"], "hermes")
        self.assertEqual(
            transcript_requests[0]["transcript"],
            "user: remember transcript extraction",
        )
        self.assertEqual(transcript_requests[0]["source"], "hermes:session_end")

    def test_client_syncs_runtime_summary_to_broker(self) -> None:
        with FakeBrokerServer() as server:
            client = BrokerSocketClient(server.socket_path, working_dir="/tmp/project")
            event = client.broker_transcript_sync(
                session_id="hermes_session",
                transcript="user: transcript remains provenance",
                source="hermes:pre_compress",
                surface_session_id="hermes_surface_session",
                surface="hermes",
                runtime_summary="Hermes runtime summary: continue from the true compressor output.",
            )
            client.close()

        self.assertEqual(event["type"], "broker_transcript_synced")
        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertEqual(
            transcript_requests[0]["runtime_summary"],
            "Hermes runtime summary: continue from the true compressor output.",
        )


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
    def test_prefetch_focus_query_strips_instruction_frame_for_negative_task_prompt(self) -> None:
        prompt = (
            "Without using tools or file search, answer only from recalled context already "
            "provided to you: what unchecked Vault task says Rob's preference is not to "
            "disable visible reasoning by default? Include the note path/source reference "
            "and line if present. If the recalled context does not contain it, say not "
            "found in recalled context."
        )

        self.assertEqual(
            _prefetch_focus_query(prompt),
            "unchecked task do not disable visible reasoning by default",
        )

    def test_prefetch_focus_query_keeps_heading_target_and_drops_reporting_instructions(self) -> None:
        prompt = (
            "Use jcode_broker_context first, before any file search, to answer this:\n\n"
            "What Vault note contains this heading?\n\n"
            "Advanced Tips: Make Ghostty Even Better\n\n"
            "Please report:\n"
            "1. The exact path returned by jcode broker context.\n"
            "2. The line number or source span the broker gives, if any."
        )

        self.assertEqual(
            _prefetch_focus_query(prompt),
            "Advanced Tips: Make Ghostty Even Better",
        )

    def test_prefetch_focus_query_strips_real_vault_tool_frame_for_exact_sentence(self) -> None:
        prompt = (
            "Use jcode_broker_context first, before any file search or other tools, "
            "to answer this from my real Vault:\n\n"
            "What Vault note contains this exact sentence?\n\n"
            "Keep Rob's preference: do not disable visible reasoning by default.\n\n"
            "Please report:\n"
            "1. The exact note path returned by jcode broker context.\n"
            "2. The nearby heading or surrounding context."
        )

        self.assertEqual(
            _prefetch_focus_query(prompt),
            "Keep Rob's preference: do not disable visible reasoning by default.",
        )

    def test_prefetch_focus_query_strips_bare_report_tail_for_exact_sentence(self) -> None:
        prompt = (
            "Use jcode_broker_context first, before any file search, to answer this:\n\n"
            "What Vault note contains this exact sentence?\n\n"
            "freshness-lumen-cypress-20260514-1924 proves newly-created Vault note "
            "ingestion through the jcode broker.\n\n"
            "Report the exact path, broker kind, source span, retrieval mode, and exact_match."
        )

        self.assertEqual(
            _prefetch_focus_query(prompt),
            "freshness-lumen-cypress-20260514-1924 proves newly-created Vault note "
            "ingestion through the jcode broker.",
        )

    def test_prefetch_focus_query_strips_generic_vault_note_question_frame(self) -> None:
        prompt = (
            "Which Vault note talks about improving a terminal emulator setup with "
            "shell prompt styling, system monitor integration, and productivity tweaks?"
        )

        self.assertEqual(
            _prefetch_focus_query(prompt),
            "improving a terminal emulator setup with shell prompt styling, system monitor "
            "integration, and productivity tweaks?",
        )

    def test_prefetch_intent_gate_skips_general_knowledge_prompt(self) -> None:
        prompt = "Answer in one concise sentence: what makes a cup of tea relaxing?"

        self.assertFalse(_prefetch_should_query_broker(prompt, _prefetch_focus_query(prompt)))

    def test_prefetch_intent_gate_keeps_memory_and_continuity_prompts(self) -> None:
        prompts = [
            (
                "Without using tools or file search, answer only from recalled context "
                "already provided to you: what unchecked Vault task says Rob's preference "
                "is not to disable visible reasoning by default?"
            ),
            (
                "Use jcode_broker_context first, before file search, to answer this: "
                "What Vault note contains this heading? Advanced Tips: Make Ghostty Even Better"
            ),
            "What did we decide about DuckDB for the graph database?",
        ]

        for prompt in prompts:
            with self.subTest(prompt=prompt):
                self.assertTrue(
                    _prefetch_should_query_broker(prompt, _prefetch_focus_query(prompt))
                )

    def test_provider_prefetch_sends_focused_query_to_broker(self) -> None:
        prompt = (
            "Without using tools or file search, answer only from recalled context already "
            "provided to you: what unchecked Vault task says Rob's preference is not to "
            "disable visible reasoning by default? Include the note path/source reference "
            "and line if present."
        )
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 4,
                }
            )
            provider.initialize("hermes_session")
            provider.prefetch(prompt, session_id="hermes_session")
            provider.shutdown()

        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(len(context_requests), 1)
        self.assertEqual(
            context_requests[0]["query"],
            "unchecked task do not disable visible reasoning by default",
        )

    def test_provider_prefetch_skips_broker_for_general_prompt(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 4,
                }
            )
            provider.initialize("hermes_session")
            text = provider.prefetch("Answer in one concise sentence: what makes tea relaxing?")
            diagnostics = provider.diagnostics()
            provider.shutdown()

        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(context_requests, [])
        self.assertEqual(text, "")
        self.assertEqual(diagnostics["last_prefetch_item_count"], 0)
        self.assertEqual(diagnostics["last_prefetch_chars"], 0)

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

    def test_provider_context_timeout_discards_client_and_reconnects_next_prefetch(self) -> None:
        with FakeBrokerServer(hang_context_once=True) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "timeout_seconds": 0.05,
                }
            )
            provider.initialize("hermes_session")
            first = provider.prefetch("project memory", session_id="hermes_session")
            second = provider.prefetch("project memory", session_id="hermes_session")
            provider.shutdown()

        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        subscribe_requests = [
            request for request in server.requests if request["type"] == "subscribe"
        ]
        self.assertEqual(first, "")
        self.assertIn("## jcode Broker Context", second)
        self.assertIn("Project Memory", second)
        self.assertEqual(len(context_requests), 2)
        self.assertGreaterEqual(len(subscribe_requests), 2)

    def test_provider_context_timeout_log_includes_reconnect_diagnostics(self) -> None:
        with FakeBrokerServer(hang_context_once=True) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "timeout_seconds": 0.05,
                }
            )
            provider.initialize("hermes_session")

            with self.assertLogs(jcode_graph.__name__, level="DEBUG") as logs:
                provider.prefetch("project memory", session_id="hermes_session")
            provider.shutdown()

        output = "\n".join(logs.output)
        self.assertIn("op=broker_context", output)
        self.assertIn("timeout_seconds=0.05", output)
        self.assertIn(f"socket_path={server.socket_path}", output)
        self.assertIn("working_dir=/tmp/project", output)
        self.assertIn("resetting_client=true", output)

    def test_provider_context_retrieval_can_use_longer_budget_than_sync_writes(self) -> None:
        with FakeBrokerServer(context_delay_seconds=0.12) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "timeout_seconds": 0.5,
                    "turn_sync_timeout_seconds": 0.05,
                    "transcript_sync_timeout_seconds": 0.05,
                }
            )
            provider.initialize("hermes_session")

            started = time.monotonic()
            text = provider.prefetch("project memory", session_id="hermes_session")
            elapsed = time.monotonic() - started
            provider.shutdown()

        self.assertGreaterEqual(elapsed, 0.1)
        self.assertLess(elapsed, 0.5)
        self.assertIn("## jcode Broker Context", text)
        self.assertIn("Project Memory", text)

    def test_hermes_memory_manager_prefetch_injects_current_turn_context_without_tool_call(self) -> None:
        if not _add_default_hermes_repo_to_path():
            self.skipTest("Hermes agent repo not available")

        from agent.memory_manager import MemoryManager, build_memory_context_block

        items = [
            {
                "id": "vault_task:vault_task_test",
                "kind": "vault_task",
                "scope": "vault",
                "content_format": "plain_text",
                "title": "Polish Hermes TUI reasoning and progress display parity with Codex / task",
                "content": "Keep Rob's preference: do not disable visible reasoning by default.",
                "source": (
                    "vault://TaskNotes/Polish Hermes TUI reasoning and progress display "
                    "parity with Codex.md#L34"
                ),
                "origin": {"tool": "duckdb_broker_store", "source": "vault"},
                "metadata": {"source_kind": "vault_task", "line": 34, "checked": False},
            }
        ]
        with FakeBrokerServer(context_items=items) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 4,
                }
            )
            provider.initialize("hermes_session")
            manager = MemoryManager()
            manager.add_provider(provider)

            prefetch = manager.prefetch_all(
                "Find the unchecked visible reasoning preference task.",
                session_id="hermes_session",
            )
            injected = build_memory_context_block(prefetch)
            user_message_for_api = (
                "Find the unchecked visible reasoning preference task."
                f"\n\n{injected}"
            )
            provider.shutdown()

        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(len(context_requests), 1)
        self.assertEqual(
            context_requests[0]["query"],
            "Find the unchecked visible reasoning preference task.",
        )
        self.assertFalse(context_requests[0].get("include_provenance", False))
        self.assertIn("<memory-context>", injected)
        self.assertIn("## jcode Broker Context", injected)
        self.assertIn("Keep Rob's preference", injected)
        self.assertIn(
            "ref=vault://TaskNotes/Polish Hermes TUI reasoning and progress display "
            "parity with Codex.md#L34",
            injected,
        )
        self.assertIn("line=34", injected)
        self.assertIn("NOT new user input", user_message_for_api)

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

    def test_provider_prefers_clio_context_packet_v1_when_present(self) -> None:
        packet = {
            "version": "clio_context_packet_v1",
            "active_task": [
                {
                    "id": "goal_1",
                    "kind": "goal",
                    "scope": "session",
                    "title": "Implement packet spine",
                    "summary": "Active work is Clio Context Packet v1.",
                    "slot": "active_task",
                    "authority_class": "active_task_note",
                    "why_included": "current active task",
                }
            ],
            "authority": [
                {
                    "id": "vault_plan",
                    "kind": "vault_chunk",
                    "scope": "project",
                    "title": "jcode Super-Session Context Broker Plan / §9",
                    "summary": "Clio Context Packet v1 is mandatory.",
                    "source_uri": "vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md#9",
                    "source_path": "Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/jcode-Nervous-System-Broker-Parity-Plan.md",
                    "line_start": 1660,
                    "line_end": 1712,
                    "slot": "authority",
                    "authority_class": "current_project_authority",
                    "why_included": "current canonical plan beats historical fork",
                    "metadata": {"start_line": 1660, "end_line": 1712},
                }
            ],
            "conflicts": [
                {
                    "id": "conflict_1",
                    "kind": "conflict",
                    "scope": "project",
                    "title": "Fork is historical",
                    "summary": "The fork remains searchable but is no longer canonical.",
                    "slot": "conflicts",
                    "authority_class": "conflict_note",
                    "why_included": "surface currentness conflict",
                }
            ],
            "lineage": [
                {
                    "id": "checkpoint_1",
                    "kind": "compression_checkpoint",
                    "scope": "project",
                    "title": "Hermes compression checkpoint",
                    "summary": "Next action: continue parent lineage work.",
                    "slot": "lineage",
                    "why_included": "lineage; current plan files override checkpoint content",
                    "metadata": {
                        "surface": "hermes",
                        "session_segment_id": "hermes_session_b",
                        "parent_segment_id": "hermes_session_a",
                    },
                }
            ],
            "skill_hints": [
                {
                    "id": "skill_context",
                    "kind": "skill",
                    "scope": "project",
                    "title": "context-engineering-collection",
                    "summary": "Procedural routing hint only.",
                    "slot": "skill_hints",
                    "authority_class": "procedural_hint",
                    "why_included": "route context-engineering work",
                }
            ],
        }
        fallback_items = [
            {
                "id": "mem_fallback",
                "kind": "memory",
                "scope": "project",
                "title": "Fallback Memory",
                "summary": "This should not drive packet formatting.",
                "origin": {"tool": "memory"},
            }
        ]
        with FakeBrokerServer(context_items=fallback_items, context_packet=packet) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 8,
                    "max_chars": 1200,
                    "item_max_chars": 160,
                }
            )
            provider.initialize("hermes_session")
            text = provider.prefetch("broker packet spine", session_id="hermes_session")
            diagnostics = provider.diagnostics()
            provider.shutdown()

        self.assertIn("## Clio Context Packet v1", text)
        self.assertIn(
            "Current user request and latest correction override stored broker context",
            text,
        )
        self.assertIn("### Active Task", text)
        self.assertIn("### Authority", text)
        self.assertIn("### Conflicts", text)
        self.assertIn("### Lineage", text)
        self.assertIn("### Skill Hints", text)
        self.assertLess(text.index("### Active Task"), text.index("### Authority"))
        self.assertLess(text.index("### Authority"), text.index("### Conflicts"))
        self.assertIn("surface=hermes", text)
        self.assertIn("segment=hermes_session_b", text)
        self.assertIn("parent=hermes_session_a", text)
        self.assertIn("authority=current_project_authority", text)
        self.assertIn("why=current canonical plan beats historical fork", text)
        self.assertIn(
            "ref=vault://Projects/Hermes-Honcho-LangGraph-Second-Brain/Hermes-Plan/"
            "jcode-Nervous-System-Broker-Parity-Plan.md#9",
            text,
        )
        self.assertIn("lines=1660-1712", text)
        self.assertEqual(text.count("lines=1660-1712"), 1)
        self.assertNotIn("Fallback Memory", text)
        self.assertEqual(diagnostics["last_prefetch_item_count"], 5)

    def test_provider_renders_structured_lineage_handoff_content(self) -> None:
        packet = {
            "version": "clio_context_packet_v1",
            "lineage": [
                {
                    "id": "checkpoint_session_end",
                    "kind": "compression_checkpoint",
                    "scope": "project",
                    "title": "Hermes session-end checkpoint",
                    "summary": "Hermes session-end checkpoint",
                    "content": (
                        "Hermes session-end checkpoint\n"
                        "Structured session-end handoff:\n"
                        "Active task:\n"
                        "- finish section 11.3 structured session-end handoff\n"
                        "Decisions:\n"
                        "- keep the fallback builder broker-side\n"
                        "Files:\n"
                        "- /Users/rob/Code/grumpy-labs/jcode-memory/src/server/broker_context.rs\n"
                        "Commands and verification:\n"
                        "- cargo test -q --features duckdb-storage-bundled broker_context\n"
                        "Next action:\n"
                        "- run fixture/live/installed gates"
                    ),
                    "slot": "lineage",
                    "why_included": "lineage; current plan files override checkpoint content",
                    "metadata": {
                        "surface": "hermes",
                        "session_segment_id": "hermes_session_end_surface",
                        "checkpoint_kind": "session_end",
                        "branch_reason": "handoff",
                    },
                }
            ],
        }
        with FakeBrokerServer(context_packet=packet) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 8,
                    "max_chars": 1200,
                    "item_max_chars": 520,
                }
            )
            provider.initialize("hermes_session")
            text = provider.prefetch("structured session-end handoff", session_id="hermes_session")
            provider.shutdown()

        self.assertIn("### Lineage", text)
        self.assertIn("Structured session-end handoff", text)
        self.assertIn("Active task:", text)
        self.assertIn("finish section 11.3 structured session-end handoff", text)
        self.assertIn("Commands and verification:", text)
        self.assertIn("cargo test -q --features duckdb-storage-bundled broker_context", text)
        self.assertIn("surface=hermes", text)
        self.assertIn("segment=hermes_session_end_surface", text)

    def test_provider_ignores_unknown_context_packet_version(self) -> None:
        packet = {
            "version": "clio_context_packet_v2",
            "authority": [
                {
                    "id": "future_packet_item",
                    "kind": "vault_chunk",
                    "scope": "project",
                    "title": "Future Packet Item",
                    "summary": "This version should not drive rendering.",
                    "slot": "authority",
                }
            ],
        }
        fallback_items = [
            {
                "id": "mem_fallback",
                "kind": "memory",
                "scope": "project",
                "title": "Fallback Memory",
                "summary": "Legacy context remains the safe path.",
                "origin": {"tool": "memory"},
            }
        ]
        with FakeBrokerServer(context_items=fallback_items, context_packet=packet) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 8,
                }
            )
            provider.initialize("hermes_session")
            text = provider.prefetch("broker packet spine", session_id="hermes_session")
            provider.shutdown()

        self.assertIn("## jcode Broker Context", text)
        self.assertIn("Fallback Memory", text)
        self.assertNotIn("## Clio Context Packet v1", text)
        self.assertNotIn("Future Packet Item", text)

    def test_provider_enforces_packet_slot_caps_and_masks_artifact_refs(self) -> None:
        packet = {
            "version": "clio_context_packet_v1",
            "authority": [
                {
                    "id": "authority_1",
                    "kind": "vault_chunk",
                    "scope": "project",
                    "title": "Authority One",
                    "summary": "First authority item should render.",
                    "slot": "authority",
                    "authority_class": "current_project_authority",
                },
                {
                    "id": "authority_2",
                    "kind": "vault_chunk",
                    "scope": "project",
                    "title": "Authority Two",
                    "summary": "Second authority item should be omitted by slot cap.",
                    "slot": "authority",
                    "authority_class": "current_project_authority",
                },
            ],
            "artifact_refs": [
                {
                    "id": "artifact_tool_output",
                    "kind": "artifact_ref",
                    "scope": "session",
                    "title": "Cargo output",
                    "summary": "Full output kept behind /tmp/cargo-output.log",
                    "content": "Total output lines: 999\n" + ("compiler detail\n" * 50),
                    "source_uri": "file:///tmp/cargo-output.log",
                    "slot": "artifact_refs",
                }
            ],
            "skill_hints": [
                {
                    "id": "skill_context",
                    "kind": "skill",
                    "scope": "project",
                    "title": "context-engineering-collection",
                    "summary": "Procedural routing hint only.",
                    "slot": "skill_hints",
                }
            ],
        }
        with FakeBrokerServer(context_packet=packet) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "context_limit": 8,
                    "max_chars": 4000,
                    "item_max_chars": 500,
                    "packet_slot_item_limit": 1,
                    "packet_slot_max_chars": 500,
                }
            )
            provider.initialize("hermes_session")
            text = provider.prefetch("broker packet spine", session_id="hermes_session")
            provider.shutdown()

        self.assertIn("Authority One", text)
        self.assertNotIn("Authority Two", text)
        self.assertIn("1 more authority item omitted", text)
        self.assertIn("Cargo output", text)
        self.assertIn("Full output kept behind /tmp/cargo-output.log", text)
        self.assertIn("ref=file:///tmp/cargo-output.log", text)
        self.assertNotIn("Total output lines: 999", text)
        self.assertIn("context-engineering-collection", text)

    def test_provider_exposes_context_tool_schema(self) -> None:
        provider = JcodeGraphMemoryProvider({"socket_path": os.devnull})
        schemas = provider.get_tool_schemas()
        self.assertEqual(schemas[0]["name"], "jcode_broker_context")

    def test_provider_exposes_turn_sync_timeout_config_schema(self) -> None:
        provider = JcodeGraphMemoryProvider({"socket_path": os.devnull})
        keys = {item["key"] for item in provider.get_config_schema()}
        self.assertIn("timeout_seconds", keys)
        self.assertIn("turn_sync_timeout_seconds", keys)
        self.assertIn("transcript_sync_timeout_seconds", keys)

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
            sync_requests = _wait_for_requests(server, "broker_turn_sync")
            provider.shutdown()

        self.assertEqual(len(sync_requests), 1)
        self.assertEqual(sync_requests[0]["session_id"], "hermes_session")
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
            sync_requests = _wait_for_requests(server, "broker_turn_sync", count=2)
            provider.shutdown()

        subscribe_requests = [
            request for request in server.requests if request["type"] == "subscribe"
        ]
        self.assertGreaterEqual(len(subscribe_requests), 2)
        self.assertCountEqual(
            [request["user_content"] for request in sync_requests],
            ["user a", "user b"],
        )

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
        self.assertEqual(transcript_requests[0]["surface_session_id"], "hermes_session")
        self.assertEqual(transcript_requests[0]["surface"], "hermes")
        self.assertNotIn("parent_segment_id", transcript_requests[0])

    def test_provider_compression_summary_syncs_runtime_summary(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session")
            result = provider.on_compression_summary(
                "Hermes runtime summary\n## Active Task\n- preserve real compression output",
                messages=[
                    {"role": "user", "content": "Transcript stays hidden behind provenance."},
                    {"role": "assistant", "content": "Runtime summary is the checkpoint body."},
                ],
            )
            provider.shutdown()

        self.assertIsNone(result)
        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertEqual(transcript_requests[0]["source"], "hermes:pre_compress")
        self.assertEqual(
            transcript_requests[0]["runtime_summary"],
            "Hermes runtime summary\n## Active Task\n- preserve real compression output",
        )
        self.assertIn(
            "user: Transcript stays hidden behind provenance.",
            transcript_requests[0]["transcript"],
        )

    def test_provider_session_switch_sends_parent_lineage_on_transcript_sync(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            provider.initialize("hermes_session_a")
            provider.on_session_switch(
                "hermes_session_b",
                parent_session_id="hermes_session_a",
                reason="resume",
            )
            provider.on_pre_compress([{"role": "user", "content": "Resumed work."}])
            provider.shutdown()

        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        self.assertEqual(len(transcript_requests), 1)
        self.assertEqual(transcript_requests[0]["session_id"], "hermes_session_b")
        self.assertEqual(transcript_requests[0]["surface_session_id"], "hermes_session_b")
        self.assertEqual(transcript_requests[0]["parent_segment_id"], "hermes_session_a")
        self.assertEqual(transcript_requests[0]["surface"], "hermes")

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
                    "sync_turns": False,
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

    def test_provider_transcript_timeout_uses_short_budget_without_poisoning_retrieval(self) -> None:
        with FakeBrokerServer(hang_transcript_sync=True) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "timeout_seconds": 1.0,
                    "transcript_sync_timeout_seconds": 0.05,
                }
            )
            provider.initialize("hermes_session")

            started = time.monotonic()
            result = provider.on_pre_compress(
                [
                    {"role": "user", "content": "Remember transcript timeout isolation."},
                    {"role": "assistant", "content": "Transcript sync may hang."},
                ]
            )
            elapsed = time.monotonic() - started
            text = provider.prefetch("project memory", session_id="hermes_session")
            provider.shutdown()

        transcript_requests = [
            request for request in server.requests if request["type"] == "broker_transcript_sync"
        ]
        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(result, "")
        self.assertLess(elapsed, 0.3)
        self.assertEqual(len(transcript_requests), 1)
        self.assertEqual(len(context_requests), 1)
        self.assertIn("## jcode Broker Context", text)
        self.assertIn("Project Memory", text)

    def test_provider_sync_turn_returns_immediately_when_broker_sync_hangs(self) -> None:
        with FakeBrokerServer(hang_turn_sync=True) as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "timeout_seconds": 5,
                    "turn_sync_timeout_seconds": 0.1,
                }
            )
            provider.initialize("hermes_session")

            started = time.monotonic()
            provider.sync_turn("user", "assistant", session_id="hermes_session")
            elapsed = time.monotonic() - started
            sync_requests = _wait_for_requests(server, "broker_turn_sync")
            provider.shutdown()

        self.assertLess(elapsed, 0.25)
        self.assertEqual(len(sync_requests), 1)
        self.assertEqual(sync_requests[0]["session_id"], "hermes_session")

    def test_provider_sync_turn_session_not_found_is_nonfatal(self) -> None:
        with FakeBrokerServer(turn_sync_error="session not found: hermes_session") as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                    "turn_sync_timeout_seconds": 0.05,
                }
            )
            provider.initialize("hermes_session")

            provider.sync_turn("user", "assistant", session_id="hermes_session")
            sync_requests = _wait_for_requests(server, "broker_turn_sync")
            text = provider.prefetch("project memory", session_id="hermes_session")
            provider.shutdown()

        self.assertEqual(len(sync_requests), 1)
        self.assertIn("## jcode Broker Context", text)
        self.assertIn("Project Memory", text)

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

    def test_provider_context_tool_lazily_connects_without_initialize(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            payload = provider.handle_tool_call(
                "jcode_broker_context",
                {"query": "lazy context", "limit": 4},
            )
            provider.shutdown()

        self.assertEqual(json.loads(payload)["type"], "broker_context")
        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(context_requests[-1]["query"], "lazy context")

    def test_provider_context_tool_sends_focused_query_to_broker(self) -> None:
        with FakeBrokerServer() as server:
            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": server.socket_path,
                    "working_dir": "/tmp/project",
                }
            )
            payload = provider.handle_tool_call(
                "jcode_broker_context",
                {
                    "query": (
                        "Which Vault note talks about improving a terminal emulator setup "
                        "with shell prompt styling, system monitor integration, and "
                        "productivity tweaks?"
                    ),
                    "limit": 4,
                },
            )
            provider.shutdown()

        self.assertEqual(json.loads(payload)["type"], "broker_context")
        context_requests = [
            request for request in server.requests if request["type"] == "broker_context"
        ]
        self.assertEqual(
            context_requests[-1]["query"],
            "improving a terminal emulator setup with shell prompt styling, system monitor "
            "integration, and productivity tweaks?",
        )

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

    def test_provider_auto_start_removes_stale_socket_before_spawn(self) -> None:
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
            socket_path = Path(tmp) / "broker.sock"
            socket_path.write_text("stale", encoding="utf-8")

            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": str(socket_path),
                    "jcode_binary": str(fake_binary),
                    "startup_timeout_seconds": 0,
                }
            )

            with unittest.mock.patch.object(
                jcode_graph.subprocess, "Popen", return_value=FakeProcess()
            ):
                provider.initialize("hermes_session", working_dir="/tmp/project")
                provider.shutdown()

            self.assertFalse(socket_path.exists())

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

    def test_provider_auto_start_can_enable_debug_control(self) -> None:
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

            provider = JcodeGraphMemoryProvider(
                {
                    "socket_path": str(Path(tmp) / "broker.sock"),
                    "jcode_binary": str(fake_binary),
                    "debug_control": True,
                    "startup_timeout_seconds": 0,
                }
            )

            with unittest.mock.patch.object(
                jcode_graph.subprocess, "Popen", return_value=FakeProcess()
            ) as popen:
                provider.initialize("hermes_session", working_dir="/tmp/project")
                provider.shutdown()

            self.assertEqual(
                popen.call_args.kwargs["env"]["JCODE_DEBUG_CONTROL"],
                "1",
            )


if __name__ == "__main__":
    unittest.main()

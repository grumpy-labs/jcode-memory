from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "adapters" / "hermes"))

from jcode_graph import (  # noqa: E402
    BrokerSocketClient,
    JcodeGraphMemoryProvider,
    _default_socket_path,
)


class FakeBrokerServer:
    def __init__(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.socket_path = str(Path(self._tmp.name) / "broker.sock")
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
                            "items": [
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

    def test_provider_exposes_context_tool_schema(self) -> None:
        provider = JcodeGraphMemoryProvider({"socket_path": os.devnull})
        schemas = provider.get_tool_schemas()
        self.assertEqual(schemas[0]["name"], "jcode_broker_context")


if __name__ == "__main__":
    unittest.main()

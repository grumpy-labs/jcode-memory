"""Hermes memory-provider adapter for the jcode broker runtime.

This adapter is intentionally thin: Hermes owns the MemoryProvider lifecycle,
while jcode owns broker_context assembly over its Unix socket protocol.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from agent.memory_provider import MemoryProvider
except Exception:  # pragma: no cover - lets adapter tests run outside Hermes.
    class MemoryProvider:  # type: ignore[no-redef]
        pass

logger = logging.getLogger(__name__)

DEFAULT_CONTEXT_LIMIT = 8
DEFAULT_MAX_CHARS = 2400
DEFAULT_STARTUP_TIMEOUT_SECONDS = 5.0


JCODE_BROKER_CONTEXT_SCHEMA = {
    "name": "jcode_broker_context",
    "description": (
        "Fetch the current jcode broker context snapshot. Use when you need "
        "fresh project memory, goals, todos, session evidence, or broker-safe "
        "tool inventory from the jcode context broker."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional focus query for broker relevance ranking.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum broker context items to request.",
            },
        },
        "required": [],
    },
}


def _load_plugin_config() -> Dict[str, Any]:
    try:
        from hermes_constants import get_hermes_home

        config_path = get_hermes_home() / "config.yaml"
    except Exception:
        config_path = Path.home() / ".hermes" / "config.yaml"

    if not config_path.exists():
        return {}

    try:
        import yaml

        with open(config_path, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
        return config.get("plugins", {}).get("jcode_graph", {}) or {}
    except Exception as exc:
        logger.debug("Failed to load jcode_graph config: %s", exc)
        return {}


def _default_socket_path() -> str:
    configured = os.environ.get("JCODE_BROKER_SOCKET")
    if configured:
        return configured
    return str(_jcode_runtime_dir() / "jcode-broker.sock")


def _jcode_runtime_dir() -> Path:
    configured = os.environ.get("JCODE_RUNTIME_DIR")
    if configured:
        return Path(configured)
    xdg_runtime = os.environ.get("XDG_RUNTIME_DIR")
    if xdg_runtime:
        return Path(xdg_runtime)
    if sys.platform == "darwin":
        mac_tmp = os.environ.get("TMPDIR")
        if mac_tmp:
            return Path(mac_tmp)
    try:
        suffix = str(os.geteuid())
    except AttributeError:
        suffix = os.environ.get("USERNAME") or os.environ.get("USER") or "user"
        suffix = "".join(ch for ch in suffix if ch.isalnum() or ch in "-_")[:64] or "user"
    return Path(tempfile.gettempdir()) / f"jcode-{suffix}"


def _config_bool(config: Dict[str, Any], key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _config_float(config: Dict[str, Any], key: str, default: float) -> float:
    value = config.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class BrokerSocketClient:
    """Small newline-JSON client for the jcode Unix socket protocol."""

    def __init__(
        self,
        socket_path: str,
        *,
        working_dir: Optional[str] = None,
        timeout: float = 2.0,
    ) -> None:
        self.socket_path = socket_path
        self.working_dir = working_dir
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._file = None
        self._next_id = 1
        self._lock = threading.Lock()
        self._broker_session_id: Optional[str] = None

    def connect(self) -> None:
        if self._sock is not None:
            return
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self.socket_path)
            self._sock = sock
            self._file = sock.makefile("rwb", buffering=0)
            self._subscribe()
        except Exception:
            try:
                sock.close()
            finally:
                self._sock = None
                self._file = None
            raise

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                try:
                    self._file.close()
                except Exception:
                    pass
                self._file = None
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None

    def broker_context(self, query: str = "", limit: int = DEFAULT_CONTEXT_LIMIT) -> Dict[str, Any]:
        with self._lock:
            self.connect()
            request_id = self._send(
                {
                    "type": "broker_context",
                    "id": self._next_request_id(),
                    "query": query or None,
                    "limit": max(0, int(limit)),
                }
            )
            return self._read_response(request_id, "broker_context")

    def broker_turn_sync(
        self,
        *,
        session_id: str,
        user_content: str,
        assistant_content: str,
        source: str = "hermes",
    ) -> Dict[str, Any]:
        with self._lock:
            self.connect()
            request_id = self._send(
                {
                    "type": "broker_turn_sync",
                    "id": self._next_request_id(),
                    "session_id": self._broker_session_id or session_id or None,
                    "user_content": user_content,
                    "assistant_content": assistant_content,
                    "source": source,
                }
            )
            return self._read_response(request_id, "broker_turn_synced")

    def _subscribe(self) -> None:
        request: Dict[str, Any] = {
            "type": "subscribe",
            "id": self._next_request_id(),
            "selfdev": False,
            "client_has_local_history": False,
            "allow_session_takeover": False,
        }
        if self.working_dir:
            request["working_dir"] = self.working_dir
        request_id = self._send(request)
        self._read_response(request_id, "done")

    def _next_request_id(self) -> int:
        request_id = self._next_id
        self._next_id += 1
        return request_id

    def _send(self, request: Dict[str, Any]) -> int:
        if self._file is None:
            raise RuntimeError("broker socket is not connected")
        line = json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
        self._file.write(line)
        return int(request["id"])

    def _read_response(self, request_id: int, expected_type: str) -> Dict[str, Any]:
        if self._file is None:
            raise RuntimeError("broker socket is not connected")
        while True:
            raw = self._file.readline()
            if not raw:
                raise RuntimeError("jcode broker disconnected")
            event = json.loads(raw.decode("utf-8"))
            event_type = event.get("type")
            event_id = event.get("id")
            if event_type == "session":
                self._broker_session_id = event.get("session_id") or self._broker_session_id
                continue
            if event_type == "ack":
                continue
            if event_type == "error" and event_id == request_id:
                raise RuntimeError(event.get("message") or "jcode broker request failed")
            if event_type == expected_type and event_id == request_id:
                return event
            if expected_type == "done" and event_type == "done" and event_id == request_id:
                return event


class JcodeGraphMemoryProvider(MemoryProvider):
    """Hermes MemoryProvider that injects jcode broker_context snapshots."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self._config = config or _load_plugin_config()
        self._client: Optional[BrokerSocketClient] = None
        self._session_id = ""
        self._working_dir = self._config.get("working_dir") or os.getcwd()
        self._context_limit = int(self._config.get("context_limit", DEFAULT_CONTEXT_LIMIT))
        self._max_chars = int(self._config.get("max_chars", DEFAULT_MAX_CHARS))
        self._socket_path = str(self._config.get("socket_path") or _default_socket_path())
        self._auto_start = _config_bool(self._config, "auto_start", True)
        self._startup_timeout = _config_float(
            self._config, "startup_timeout_seconds", DEFAULT_STARTUP_TIMEOUT_SECONDS
        )
        self._broker_process: Optional[subprocess.Popen[Any]] = None

    @property
    def name(self) -> str:
        return "jcode_graph"

    def is_available(self) -> bool:
        if Path(self._socket_path).exists():
            return True
        if not self._auto_start:
            return False
        return shutil.which("jcode") is not None or shutil.which("jcode-memory") is not None

    def initialize(self, session_id: str, **kwargs: Any) -> None:
        self._session_id = session_id
        self._working_dir = (
            self._config.get("working_dir")
            or kwargs.get("working_dir")
            or kwargs.get("cwd")
            or os.getcwd()
        )
        self._client = BrokerSocketClient(
            self._socket_path,
            working_dir=str(self._working_dir) if self._working_dir else None,
            timeout=float(self._config.get("timeout_seconds", 2.0)),
        )
        if self._connect_client():
            return
        if self._auto_start and self._start_broker():
            deadline = time.monotonic() + max(0.0, self._startup_timeout)
            while time.monotonic() <= deadline:
                if self._connect_client():
                    return
                time.sleep(0.05)
        self._client = None

    def system_prompt_block(self) -> str:
        if self._client is None:
            return ""
        return (
            "# jcode Context Broker\n"
            "A jcode broker is attached as an external memory provider. "
            "Use injected broker context and the jcode_broker_context tool for "
            "project memory, goals, todos, and retrieved context evidence."
        )

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        del session_id
        event = self._fetch_context(query=query, limit=self._context_limit)
        if not event:
            return ""
        return self._format_prefetch(event)

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        if self._client is None:
            return None
        try:
            self._client.broker_turn_sync(
                session_id="",
                user_content=user_content,
                assistant_content=assistant_content,
                source=str(self._config.get("source") or "hermes"),
            )
        except Exception as exc:
            logger.debug("jcode broker_turn_sync failed: %s", exc)
        return None

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return [JCODE_BROKER_CONTEXT_SCHEMA]

    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs: Any) -> str:
        del kwargs
        if tool_name != "jcode_broker_context":
            return json.dumps({"error": f"unknown jcode_graph tool: {tool_name}"})
        event = self._fetch_context(
            query=str(args.get("query") or ""),
            limit=int(args.get("limit") or self._context_limit),
        )
        return json.dumps(event or {"error": "jcode broker context unavailable"})

    def shutdown(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._broker_process is not None:
            process = self._broker_process
            self._broker_process = None
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass

    def get_config_schema(self) -> List[Dict[str, Any]]:
        return [
            {
                "key": "auto_start",
                "description": "Start jcode broker serve when the socket is unavailable",
                "default": "true",
            },
            {
                "key": "jcode_binary",
                "description": "Path to jcode or jcode-memory binary for auto-start",
                "default": "jcode",
            },
            {
                "key": "socket_path",
                "description": "Path to the jcode broker Unix socket",
                "default": _default_socket_path(),
            },
            {
                "key": "working_dir",
                "description": "Project directory for broker-scoped memory",
                "default": os.getcwd(),
            },
            {
                "key": "source",
                "description": "Source label for synced Hermes turns",
                "default": "hermes",
            },
            {
                "key": "context_limit",
                "description": "Maximum broker context items per prefetch",
                "default": str(DEFAULT_CONTEXT_LIMIT),
            },
            {
                "key": "max_chars",
                "description": "Maximum formatted context characters to inject",
                "default": str(DEFAULT_MAX_CHARS),
            },
            {
                "key": "startup_timeout_seconds",
                "description": "Seconds to wait for auto-started broker socket readiness",
                "default": str(DEFAULT_STARTUP_TIMEOUT_SECONDS),
            },
        ]

    def save_config(self, values: Dict[str, Any], hermes_home: str) -> None:
        config_path = Path(hermes_home) / "config.yaml"
        try:
            import yaml

            existing: Dict[str, Any] = {}
            if config_path.exists():
                with open(config_path, "r", encoding="utf-8") as handle:
                    existing = yaml.safe_load(handle) or {}
            existing.setdefault("plugins", {})
            existing["plugins"]["jcode_graph"] = values
            with open(config_path, "w", encoding="utf-8") as handle:
                yaml.safe_dump(existing, handle, default_flow_style=False)
        except Exception as exc:
            logger.debug("Failed to save jcode_graph config: %s", exc)

    def _fetch_context(self, *, query: str, limit: int) -> Optional[Dict[str, Any]]:
        if self._client is None:
            return None
        try:
            return self._client.broker_context(query=query, limit=limit)
        except Exception as exc:
            logger.debug("jcode broker_context failed: %s", exc)
            return None

    def _connect_client(self) -> bool:
        if self._client is None:
            return False
        try:
            self._client.connect()
            return True
        except Exception as exc:
            logger.debug("jcode broker connect failed: %s", exc)
            self._client.close()
            return False

    def _resolve_jcode_binary(self) -> Optional[str]:
        configured = self._config.get("jcode_binary") or os.environ.get("JCODE_BINARY")
        if configured:
            return str(configured)
        return shutil.which("jcode") or shutil.which("jcode-memory")

    def _start_broker(self) -> bool:
        if self._broker_process is not None and self._broker_process.poll() is None:
            return True
        binary = self._resolve_jcode_binary()
        if not binary:
            return False

        socket_path = Path(self._socket_path)
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.setdefault("JCODE_RUNTIME_DIR", str(socket_path.parent))
        env["JCODE_NON_INTERACTIVE"] = "1"
        command = [
            binary,
            "broker",
            "serve",
            "--socket",
            str(socket_path),
            "--quiet",
        ]
        try:
            self._broker_process = subprocess.Popen(
                command,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            return True
        except Exception as exc:
            logger.debug("Failed to start jcode broker: %s", exc)
            self._broker_process = None
            return False

    def _format_prefetch(self, event: Dict[str, Any]) -> str:
        items = event.get("items") or []
        if not items:
            return ""
        lines = ["## jcode Broker Context"]
        for item in items:
            if not isinstance(item, dict):
                continue
            kind = item.get("kind") or "context"
            scope = item.get("scope") or "session"
            title = item.get("title") or item.get("id") or kind
            content = item.get("summary") or item.get("content") or ""
            origin = item.get("origin") or {}
            source = origin.get("tool") or item.get("source") or "broker"
            prefix = f"- [{kind}/{scope}/{source}] {title}"
            if content:
                prefix += f": {content}"
            lines.append(prefix)
        text = "\n".join(lines)
        if len(text) > self._max_chars:
            return text[: self._max_chars].rstrip() + "\n..."
        return text


def register(ctx: Any) -> None:
    ctx.register_memory_provider(JcodeGraphMemoryProvider())

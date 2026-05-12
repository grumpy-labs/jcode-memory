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
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

try:
    from agent.memory_provider import MemoryProvider
except Exception:  # pragma: no cover - lets adapter tests run outside Hermes.
    class MemoryProvider:  # type: ignore[no-redef]
        pass

logger = logging.getLogger(__name__)

DEFAULT_CONTEXT_LIMIT = 8
DEFAULT_MAX_CHARS = 2400
DEFAULT_ITEM_MAX_CHARS = 360
DEFAULT_TRANSCRIPT_MAX_CHARS = 12000
DEFAULT_TURN_BUFFER_LIMIT = 12
DEFAULT_TURN_BUFFER_MAX_CHARS = 2000
DEFAULT_TOOL_INVENTORY_LIMIT = 8
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
            "include_provenance": {
                "type": "boolean",
                "description": (
                    "Explicitly request hidden raw provenance. Normal prefetch keeps this off."
                ),
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
            self._broker_session_id = None

    def broker_context(
        self,
        query: str = "",
        limit: int = DEFAULT_CONTEXT_LIMIT,
        *,
        include_provenance: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            self.connect()
            request: Dict[str, Any] = {
                "type": "broker_context",
                "id": self._next_request_id(),
                "query": query or None,
                "limit": max(0, int(limit)),
            }
            if include_provenance:
                request["include_provenance"] = True
            request_id = self._send(request)
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

    def broker_transcript_sync(
        self,
        *,
        session_id: str,
        transcript: str,
        source: str = "hermes:session_end",
    ) -> Dict[str, Any]:
        with self._lock:
            self.connect()
            request_id = self._send(
                {
                    "type": "broker_transcript_sync",
                    "id": self._next_request_id(),
                    "session_id": self._broker_session_id or session_id or None,
                    "transcript": transcript,
                    "source": source,
                }
            )
            return self._read_response(request_id, "broker_transcript_synced")

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
        self._item_max_chars = int(self._config.get("item_max_chars", DEFAULT_ITEM_MAX_CHARS))
        self._transcript_max_chars = int(
            self._config.get("transcript_max_chars", DEFAULT_TRANSCRIPT_MAX_CHARS)
        )
        self._turn_buffer_limit = int(
            self._config.get("turn_buffer_limit", DEFAULT_TURN_BUFFER_LIMIT)
        )
        self._turn_buffer_max_chars = int(
            self._config.get("turn_buffer_max_chars", DEFAULT_TURN_BUFFER_MAX_CHARS)
        )
        self._tool_inventory_limit = int(
            self._config.get("tool_inventory_limit", DEFAULT_TOOL_INVENTORY_LIMIT)
        )
        self._socket_path = str(self._config.get("socket_path") or _default_socket_path())
        self._duckdb_path = self._config.get("duckdb_path") or self._config.get(
            "broker_duckdb_path"
        )
        self._jcode_home = self._config.get("jcode_home")
        self._disable_telemetry = _config_bool(self._config, "disable_telemetry", False)
        self._auto_start = _config_bool(self._config, "auto_start", True)
        self._sync_turns = _config_bool(self._config, "sync_turns", True)
        self._sync_transcripts = _config_bool(self._config, "sync_transcripts", True)
        self._include_provenance = _config_bool(self._config, "include_provenance", False)
        self._startup_timeout = _config_float(
            self._config, "startup_timeout_seconds", DEFAULT_STARTUP_TIMEOUT_SECONDS
        )
        self._broker_process: Optional[subprocess.Popen[Any]] = None
        self._recent_turns: Deque[Dict[str, str]] = deque(maxlen=max(0, self._turn_buffer_limit))
        self._diagnostics: Dict[str, Any] = {
            "turn_sync_count": 0,
            "transcript_sync_count": 0,
            "last_extraction_status": None,
            "last_prefetch_item_count": 0,
            "last_prefetch_chars": 0,
            "last_transcript_chars": 0,
            "turn_buffer_size": 0,
        }

    @property
    def name(self) -> str:
        return "jcode_graph"

    def is_available(self) -> bool:
        if Path(self._socket_path).exists():
            return True
        if not self._auto_start:
            return False
        return self._resolve_jcode_binary() is not None

    def initialize(self, session_id: str, **kwargs: Any) -> None:
        self._session_id = session_id
        self._working_dir = (
            self._config.get("working_dir")
            or kwargs.get("working_dir")
            or kwargs.get("cwd")
            or os.getcwd()
        )
        self._client = self._new_client()
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
        self._ensure_session(session_id)
        event = self._fetch_context(
            query=query,
            limit=self._context_limit,
            include_provenance=self._include_provenance,
        )
        if not event:
            return ""
        text = self._format_prefetch(event, include_provenance=self._include_provenance)
        self._diagnostics["last_prefetch_chars"] = len(text)
        return text

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        self._ensure_session(session_id)
        self._remember_turn(user_content, assistant_content)
        if not self._sync_turns:
            return None
        if self._client is None:
            return None
        try:
            event = self._client.broker_turn_sync(
                session_id="",
                user_content=user_content,
                assistant_content=assistant_content,
                source=str(self._config.get("source") or "hermes"),
            )
            self._diagnostics["turn_sync_count"] += 1
            self._diagnostics["last_extraction_status"] = event.get("extraction_status")
        except Exception as exc:
            logger.debug("jcode broker_turn_sync failed: %s", exc)
        return None

    def on_pre_compress(self, messages: List[Dict[str, Any]]) -> str:
        self._sync_transcript(messages, source="hermes:pre_compress")
        return ""

    def on_session_end(self, messages: List[Dict[str, Any]]) -> None:
        self._sync_transcript(messages, source="hermes:session_end")

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return [JCODE_BROKER_CONTEXT_SCHEMA]

    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs: Any) -> str:
        del kwargs
        if tool_name != "jcode_broker_context":
            return json.dumps({"error": f"unknown jcode_graph tool: {tool_name}"})
        event = self._fetch_context(
            query=str(args.get("query") or ""),
            limit=int(args.get("limit") or self._context_limit),
            include_provenance=_config_bool(args, "include_provenance", False),
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
                "key": "duckdb_path",
                "description": "DuckDB broker-store path exported as JCODE_BROKER_DUCKDB_PATH",
                "default": "",
            },
            {
                "key": "jcode_home",
                "description": "Dedicated JCODE_HOME used by the auto-started broker",
                "default": "",
            },
            {
                "key": "disable_telemetry",
                "description": "Export JCODE_NO_TELEMETRY=1 for the auto-started broker",
                "default": "false",
            },
            {
                "key": "source",
                "description": "Source label for synced Hermes turns",
                "default": "hermes",
            },
            {
                "key": "sync_turns",
                "description": "Write completed Hermes turns as hidden broker provenance",
                "default": "true",
            },
            {
                "key": "sync_transcripts",
                "description": "Send compression/session-end transcripts for derived extraction",
                "default": "true",
            },
            {
                "key": "include_provenance",
                "description": "Inject hidden raw provenance during normal prefetch",
                "default": "false",
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
                "key": "item_max_chars",
                "description": "Maximum characters per formatted broker context item",
                "default": str(DEFAULT_ITEM_MAX_CHARS),
            },
            {
                "key": "transcript_max_chars",
                "description": "Maximum transcript characters sent to broker extraction hooks",
                "default": str(DEFAULT_TRANSCRIPT_MAX_CHARS),
            },
            {
                "key": "turn_buffer_limit",
                "description": "Maximum recent turns retained for fallback transcript sync",
                "default": str(DEFAULT_TURN_BUFFER_LIMIT),
            },
            {
                "key": "turn_buffer_max_chars",
                "description": "Maximum characters retained per buffered turn field",
                "default": str(DEFAULT_TURN_BUFFER_MAX_CHARS),
            },
            {
                "key": "tool_inventory_limit",
                "description": "Maximum broker tool names shown in normal prefetch",
                "default": str(DEFAULT_TOOL_INVENTORY_LIMIT),
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

    def diagnostics(self) -> Dict[str, Any]:
        data = dict(self._diagnostics)
        data["turn_buffer_size"] = len(self._recent_turns)
        data["session_id"] = self._session_id
        data["socket_path"] = self._socket_path
        return data

    def _fetch_context(
        self,
        *,
        query: str,
        limit: int,
        include_provenance: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if self._client is None:
            return None
        try:
            event = self._client.broker_context(
                query=query,
                limit=limit,
                include_provenance=include_provenance,
            )
            self._diagnostics["last_prefetch_item_count"] = len(event.get("items") or [])
            return event
        except Exception as exc:
            logger.debug("jcode broker_context failed: %s", exc)
            return None

    def _sync_transcript(self, messages: List[Dict[str, Any]], *, source: str) -> None:
        if not self._sync_transcripts:
            return None
        if self._client is None:
            return None
        transcript = _messages_to_transcript(messages) or self._recent_turns_to_transcript()
        if not transcript:
            return None
        transcript = _truncate_text(transcript, self._transcript_max_chars)
        try:
            event = self._client.broker_transcript_sync(
                session_id="",
                transcript=transcript,
                source=source,
            )
            self._diagnostics["transcript_sync_count"] += 1
            self._diagnostics["last_transcript_chars"] = len(transcript)
            self._diagnostics["last_extraction_status"] = event.get("extraction_status")
            if source == "hermes:session_end":
                self._recent_turns.clear()
        except Exception as exc:
            logger.debug("jcode broker_transcript_sync failed: %s", exc)
        return None

    def _new_client(self) -> BrokerSocketClient:
        return BrokerSocketClient(
            self._socket_path,
            working_dir=str(self._working_dir) if self._working_dir else None,
            timeout=float(self._config.get("timeout_seconds", 2.0)),
        )

    def _ensure_session(self, session_id: str = "") -> None:
        next_session_id = str(session_id or self._session_id or "").strip()
        if not next_session_id:
            return
        if not self._session_id:
            self._session_id = next_session_id
            return
        if next_session_id == self._session_id:
            return

        if self._client is not None:
            self._client.close()
        self._recent_turns.clear()
        self._session_id = next_session_id
        self._client = self._new_client()
        if self._connect_client():
            return
        if self._auto_start and self._start_broker():
            deadline = time.monotonic() + max(0.0, self._startup_timeout)
            while time.monotonic() <= deadline:
                if self._connect_client():
                    return
                time.sleep(0.05)
        self._client = None

    def _remember_turn(self, user_content: str, assistant_content: str) -> None:
        if self._recent_turns.maxlen == 0:
            return
        user = _truncate_text(str(user_content or "").strip(), self._turn_buffer_max_chars)
        assistant = _truncate_text(
            str(assistant_content or "").strip(),
            self._turn_buffer_max_chars,
        )
        if not user and not assistant:
            return
        self._recent_turns.append({"user": user, "assistant": assistant})

    def _recent_turns_to_transcript(self) -> str:
        lines: List[str] = []
        for turn in self._recent_turns:
            if turn.get("user"):
                lines.append(f"user: {turn['user']}")
            if turn.get("assistant"):
                lines.append(f"assistant: {turn['assistant']}")
        return "\n".join(lines)

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
            candidate = str(configured)
            if os.path.sep in candidate or (os.path.altsep and os.path.altsep in candidate):
                path = Path(candidate)
                return str(path) if path.is_file() and os.access(path, os.X_OK) else None
            return shutil.which(candidate)
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
        if self._duckdb_path:
            env["JCODE_BROKER_DUCKDB_PATH"] = str(self._duckdb_path)
        if self._jcode_home:
            env["JCODE_HOME"] = str(self._jcode_home)
        if self._disable_telemetry:
            env["JCODE_NO_TELEMETRY"] = "1"
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

    def _format_prefetch(self, event: Dict[str, Any], *, include_provenance: bool = False) -> str:
        raw_items = event.get("items") or []
        items = [
            item
            for item in raw_items
            if isinstance(item, dict) and (include_provenance or not _is_provenance_item(item))
        ]
        if not items:
            return ""
        lines = ["## jcode Broker Context"]

        sections = [
            ("Memories", [item for item in items if item.get("kind") == "memory"]),
            ("Goals and Todos", [item for item in items if item.get("kind") in {"goal", "todo"}]),
            (
                "Evidence",
                [
                    item
                    for item in items
                    if item.get("kind") in {"session_search_hit", "conversation_search_hit"}
                ],
            ),
            ("Skill Candidates", [item for item in items if item.get("kind") == "skill"]),
            ("Artifacts", [item for item in items if item.get("kind") == "side_panel"]),
            (
                "Broker Tools",
                [item for item in items if item.get("kind") == "tool"][
                    : max(0, self._tool_inventory_limit)
                ],
            ),
        ]

        covered = {id(item) for _, section_items in sections for item in section_items}
        other_items = [item for item in items if id(item) not in covered]
        if other_items:
            sections.append(("Other Context", other_items))

        for title, section_items in sections:
            if not section_items:
                continue
            lines.append(f"### {title}")
            if title == "Broker Tools":
                names = [
                    str(item.get("title") or item.get("id") or "").strip()
                    for item in section_items
                    if str(item.get("title") or item.get("id") or "").strip()
                ]
                if names:
                    lines.append(f"- {', '.join(names)}")
                continue
            for item in section_items:
                lines.append(self._format_item_line(item))

        text = "\n".join(lines)
        if len(text) > self._max_chars:
            return text[: self._max_chars].rstrip() + "\n..."
        return text

    def _format_item_line(self, item: Dict[str, Any]) -> str:
        kind = item.get("kind") or "context"
        scope = item.get("scope") or "session"
        title = item.get("title") or item.get("id") or kind
        content = item.get("summary") or item.get("content") or ""
        origin = item.get("origin") or {}
        relevance = item.get("relevance") or {}
        source = origin.get("tool") or item.get("source") or "broker"
        details: List[str] = []
        if origin.get("session_id"):
            details.append(f"session={origin['session_id']}")
        if origin.get("source") and origin.get("source") != source:
            details.append(f"source={origin['source']}")
        if relevance.get("rank") is not None:
            details.append(f"rank={relevance['rank']}")
        if relevance.get("retrieval_mode"):
            details.append(f"mode={relevance['retrieval_mode']}")
        suffix = f" ({'; '.join(details)})" if details else ""
        line = f"- [{kind}/{scope}/{source}] {title}{suffix}"
        content = _truncate_text(str(content or "").strip(), self._item_max_chars)
        if content:
            line += f": {content}"
        return line


def register(ctx: Any) -> None:
    ctx.register_memory_provider(JcodeGraphMemoryProvider())


def _messages_to_transcript(messages: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _message_content_to_text(message.get("content"))
        if not content:
            continue
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 4)].rstrip() + " ..."


def _is_provenance_item(item: Dict[str, Any]) -> bool:
    tags = item.get("tags") or []
    metadata = item.get("metadata") or {}
    if isinstance(tags, list) and "broker-provenance" in tags:
        return True
    if isinstance(metadata, dict) and metadata.get("provenance") is True:
        return True
    return item.get("category") == "provenance" or item.get("title") == "provenance"


def _message_content_to_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            text = _message_content_to_text(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    if isinstance(content, dict):
        for key in ("text", "content"):
            value = content.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        if content.get("type") == "text" and isinstance(content.get("value"), str):
            return content["value"].strip()
        return ""
    return str(content).strip()

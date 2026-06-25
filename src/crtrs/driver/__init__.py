"""Python client for Driver cloud — run agents, stream events.

Mirror of the Node client (`@crtrs/driver`): POST a prompt to
``/api/driver/run`` with a ``dr_`` API key, then stream the agent's events back
over SSE in real time.

Only five event kinds are surfaced — ``plan``, ``plan_item_start``, ``action``,
``done``, ``fatal`` — and nothing else (internal kinds, raw tool ids, action
args and raw error messages are dropped client-side).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Union

from .tool import (
    PARAM_DESC_MAX,
    Param,
    Tool,
    ToolError,
    ToolResult,
    define_tool,
    normalize_params,
    signature_params,
)

__all__ = [
    "Driver",
    "DriverError",
    "Tool",
    "Param",
    "ToolResult",
    "ToolError",
    "define_tool",
    "signature_params",
    "normalize_params",
    "PARAM_DESC_MAX",
]
__version__ = "0.1.0"

DEFAULT_BASE_URL = "https://driver.tors.app"
RUN_PATH = "/api/driver/run"

# The only event kinds the client surfaces; everything else is dropped.
ALLOWED_KINDS = frozenset({"plan", "plan_item_start", "action", "done", "fatal"})

Event = Dict[str, object]


class DriverError(Exception):
    """Raised on a failed run or a ``fatal`` event (carries the error category)."""


class Driver:
    """Client for Driver cloud.

    :param api_key: the ``dr_…`` API key (machine credential). Falls back to the
        ``DRIVER_API_KEY`` environment variable.
    :param base_url: cloud base URL. Falls back to ``DRIVER_BASE_URL`` then the
        default (``https://driver.tors.app``).
    :param timeout: per-read socket timeout in seconds (``None`` = no timeout).
    :param tools: default tools sent with every run; a per-call ``tools`` argument
        overrides this list for that call. Each may be a :class:`Tool` or a plain
        dict in the catalog shape.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
        tools: Optional[Sequence[Union[Tool, Dict[str, Any]]]] = None,
    ) -> None:
        key = api_key or os.environ.get("DRIVER_API_KEY")
        if not key:
            raise ValueError(
                "Driver: missing api_key (pass api_key= or set DRIVER_API_KEY)"
            )
        self.api_key = key
        self.base_url = (base_url or os.environ.get("DRIVER_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self.tools = list(tools or [])

    def stream(
        self,
        prompt: str,
        tools: Optional[Sequence[Union[Tool, Dict[str, Any]]]] = None,
    ) -> Iterator[Event]:
        """Run a prompt and yield each agent event as it arrives.

        Yields the five allowlisted event kinds. Raises :class:`DriverError` on a
        ``fatal`` event or a non-2xx response. ``tools`` overrides the constructor
        list for this run.
        """
        body: Dict[str, Any] = {"prompt": prompt}
        chosen = self.tools if tools is None else list(tools)
        if chosen:
            body["tools"] = [t.to_dict() if isinstance(t, Tool) else t for t in chosen]
        req = urllib.request.Request(
            self.base_url + RUN_PATH,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
                # Default urllib UA ("Python-urllib/3.x") trips Cloudflare bot
                # filtering and gets a 403; send our own instead.
                "User-Agent": f"crtrs-driver/{__version__}",
            },
        )
        try:
            resp = urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            body = _safe_read(e)
            raise DriverError(
                f"Driver run failed: HTTP {e.code} {e.reason}" + (f" — {body}" if body else "")
            ) from None

        # Registry of locally-runnable tools, keyed by name, for tool_request.
        registry = {t.name(): t for t in chosen if isinstance(t, Tool)}
        run_id: Optional[str] = None

        with resp:
            for ev in _parse_sse(resp):
                kind = ev.get("kind")
                # `run`: first event, carries the run_id we POST results against.
                if kind == "run":
                    run_id = ev.get("run_id")  # type: ignore[assignment]
                    continue
                # `tool_request`: the cloud is asking us to run one of OUR tools
                # locally and hand back the result. Internal — not surfaced.
                if kind == "tool_request":
                    self._answer_tool_request(run_id, ev, registry)
                    continue
                # Allowlist: only surface the public kinds so internal events
                # can never leak to the caller.
                if kind not in ALLOWED_KINDS:
                    continue
                if kind == "fatal":
                    # Only the error category is exposed; raw message stays server-side.
                    raise DriverError(str(ev.get("semantic") or "fatal"))
                yield ev

    def _answer_tool_request(
        self,
        run_id: Optional[str],
        ev: Event,
        registry: Dict[str, Tool],
    ) -> None:
        """Run a locally-registered tool for a ``tool_request`` and POST the
        result back to ``/run/{run_id}/result``. Never raises into the stream."""
        call_id = ev.get("call_id")
        name = str(ev.get("tool") or "")
        if not run_id or call_id is None:
            return  # nothing to answer against
        raw_args = ev.get("args")
        args = list(raw_args) if isinstance(raw_args, (list, tuple)) else ([] if raw_args is None else [raw_args])

        tool = registry.get(name)
        if tool is None:
            self._post_result(run_id, call_id, error=f"unknown tool: {name}")
            return

        outcome = tool.call_safe(args)  # never raises
        if isinstance(outcome, ToolError):
            self._post_result(run_id, call_id, error=str(outcome))
        else:
            self._post_result(run_id, call_id, result=outcome.value)

    def _post_result(
        self,
        run_id: str,
        call_id: Any,
        result: Any = None,
        error: Optional[str] = None,
    ) -> None:
        """POST a tool outcome to ``/run/{run_id}/result``. Failures are swallowed
        so a dead result channel can't crash the event stream."""
        payload: Dict[str, Any] = {"call_id": str(call_id)}
        if error is not None:
            payload["error"] = error
        else:
            payload["result"] = result
        req = urllib.request.Request(
            f"{self.base_url}{RUN_PATH}/{run_id}/result",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": f"crtrs-driver/{__version__}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout):
                pass
        except urllib.error.URLError:
            # Result delivery failed (server timed out the call, run ended, …).
            # Keep reading the stream; the server handles the missing result.
            pass

    def run(
        self,
        prompt: str,
        on_event: Optional[Callable[[Event], None]] = None,
        tools: Optional[Sequence[Union[Tool, Dict[str, Any]]]] = None,
    ) -> Optional[Event]:
        """Run a prompt to completion.

        :param on_event: optional callback invoked for every event.
        :param tools: overrides the constructor tools for this run.
        :returns: the final ``done`` event, or ``None`` if the stream ended
            without one.
        """
        done: Optional[Event] = None
        for ev in self.stream(prompt, tools=tools):
            if on_event is not None:
                on_event(ev)
            if ev["kind"] == "done":
                done = ev
        return done


def _resolve_localhost(base_url: str) -> "tuple[str, Optional[str]]":
    """Rewrite a ``*.localhost`` base URL to connect over the loopback.

    macOS (unlike Linux / browsers) doesn't resolve ``*.localhost`` subdomains,
    so ``http://driver.localhost:8080`` fails DNS. Connect to ``127.0.0.1``
    instead and return the original ``host:port`` to send as the ``Host`` header
    so the server's subdomain routing still works. Returns
    ``(connect_base, host_header)``; ``host_header`` is ``None`` when no rewrite
    is needed.
    """
    parts = urlsplit(base_url)
    host = (parts.hostname or "").lower()
    if host.endswith(".localhost"):
        netloc = "127.0.0.1" + (f":{parts.port}" if parts.port else "")
        connect = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        return connect.rstrip("/"), parts.netloc
    return base_url, None


def _safe_read(e: urllib.error.HTTPError) -> str:
    try:
        return e.read().decode("utf-8", "replace")[:500]
    except Exception:
        return ""


def _parse_sse(resp) -> Iterator[Event]:
    """Yield parsed JSON events from an SSE stream.

    Handles multi-line ``data:`` fields and blank-line event delimiters.
    ``data`` blocks that aren't valid JSON are skipped (never surfaced as raw
    text).
    """
    data_lines: List[str] = []
    for raw in resp:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if line == "":  # blank line = end of one SSE event
            ev = _parse_event(data_lines)
            data_lines = []
            if ev is not None:
                yield ev
            continue
        if line.startswith(":"):  # comment / heartbeat
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))

    ev = _parse_event(data_lines)  # flush trailing event with no final blank line
    if ev is not None:
        yield ev


def _parse_event(data_lines: List[str]) -> Optional[Event]:
    if not data_lines:
        return None
    try:
        return json.loads("\n".join(data_lines))
    except json.JSONDecodeError:
        return None  # non-JSON data is dropped, never surfaced as raw text

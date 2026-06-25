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
from typing import Callable, Dict, Iterator, List, Optional

__all__ = ["Driver", "DriverError"]
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
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        key = api_key or os.environ.get("DRIVER_API_KEY")
        if not key:
            raise ValueError(
                "Driver: missing api_key (pass api_key= or set DRIVER_API_KEY)"
            )
        self.api_key = key
        self.base_url = (base_url or os.environ.get("DRIVER_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout

    def stream(self, prompt: str) -> Iterator[Event]:
        """Run a prompt and yield each agent event as it arrives.

        Yields the five allowlisted event kinds. Raises :class:`DriverError` on a
        ``fatal`` event or a non-2xx response.
        """
        req = urllib.request.Request(
            self.base_url + RUN_PATH,
            data=json.dumps({"prompt": prompt}).encode("utf-8"),
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

        with resp:
            for ev in _parse_sse(resp):
                # Allowlist: only surface the public kinds so internal events
                # can never leak to the caller.
                if ev.get("kind") not in ALLOWED_KINDS:
                    continue
                if ev["kind"] == "fatal":
                    # Only the error category is exposed; raw message stays server-side.
                    raise DriverError(str(ev.get("semantic") or "fatal"))
                yield ev

    def run(self, prompt: str, on_event: Optional[Callable[[Event], None]] = None) -> Optional[Event]:
        """Run a prompt to completion.

        :param on_event: optional callback invoked for every event.
        :returns: the final ``done`` event, or ``None`` if the stream ended
            without one.
        """
        done: Optional[Event] = None
        for ev in self.stream(prompt):
            if on_event is not None:
                on_event(ev)
            if ev["kind"] == "done":
                done = ev
        return done


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

"""Python port of the Driver ``Tool`` trait.

Turbo-comfy. Params are an ordered list of ``{"name", "type", "description"}``
dicts — order is the positional arg order passed to ``call``. ``type`` is
optional: omit it and it's inferred at runtime from the ``call`` signature
(annotations first, then default-value types).

    fetch_users = define_tool(
        name="fetch_users",
        description="List users.",
        params=[
            {"name": "page",  "type": "number", "description": "page index"},
            {"name": "limit", "description": "page size"},  # type inferred
        ],
        call=lambda page=0, limit=20: db_users(page, limit),
    )

``call`` gets positional args spread in, returns any plain value, and raises
normally. Wrapping into ToolResult / ToolError is internal.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

__all__ = [
    "Tool",
    "Param",
    "ToolResult",
    "ToolError",
    "define_tool",
    "signature_params",
    "normalize_params",
    "PARAM_DESC_MAX",
]

# Max length of a param description (chars).
PARAM_DESC_MAX = 200


class Param:
    """A single declared parameter."""

    def __init__(
        self,
        name: str,
        type: str = "unknown",  # noqa: A002 - mirrors the wire field name
        description: str = "",
        required: bool = True,
    ) -> None:
        if not name:
            raise ValueError("Param: name is required")
        if len(description) > PARAM_DESC_MAX:
            raise ValueError(
                f'Param "{name}": description is {len(description)} chars, max {PARAM_DESC_MAX}'
            )
        self.name = name
        self.type = type or "unknown"
        self.description = description
        self.required = required

    def to_dict(self) -> Dict[str, Any]:
        """Rich form (introspection / debugging)."""
        return {
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "required": self.required,
        }

    def to_wire(self) -> List[str]:
        """Wire form the server expects: a ``[name, type]`` tuple. The kernel's
        catalog only renders ``name(type)`` — description/required stay local."""
        return [self.name, self.type]


# ── runtime signature analysis ────────────────────────────────────────────────

# Python types → coarse wire types (mirrors the Node lib's vocabulary).
_TYPE_MAP = {
    bool: "boolean",  # check before int — bool is a subclass of int
    int: "number",
    float: "number",
    complex: "number",
    str: "string",
    bytes: "string",
    dict: "object",
    list: "array",
    tuple: "array",
    set: "array",
    frozenset: "array",
}


def _coarse(py_type: Any) -> str:
    """Map a Python class (or typing generic) to a coarse wire type."""
    origin = getattr(py_type, "__origin__", None)
    base = origin if origin is not None else py_type
    if isinstance(base, type):
        for cls, name in _TYPE_MAP.items():
            if base is cls or (isinstance(base, type) and issubclass(base, cls)):
                return name
    return "unknown"


def _param_type(p: inspect.Parameter) -> str:
    """Infer a coarse type from a parameter's annotation, else its default."""
    if p.annotation is not inspect.Parameter.empty:
        t = _coarse(p.annotation)
        if t != "unknown":
            return t
    if p.default is not inspect.Parameter.empty and p.default is not None:
        return _coarse(type(p.default))
    return "unknown"


def signature_params(fn: Callable[..., Any]) -> List[Tuple[str, str]]:
    """Ordered ``(name, type)`` for ``fn``'s positional params. ``[]`` if opaque."""
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return []  # builtins / C functions with no introspectable signature
    out: List[Tuple[str, str]] = []
    for name, p in sig.parameters.items():
        if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue  # skip *args / **kwargs
        out.append((name, _param_type(p)))
    return out


ParamSpec = Union[str, Param, Dict[str, Any]]


def normalize_params(
    spec: Optional[Union[Sequence[ParamSpec], Dict[str, str]]],
    fn: Optional[Callable[..., Any]] = None,
) -> List[Param]:
    """Build catalog ``Param`` list.

    Order comes from ``spec`` (a list); missing types are inferred from ``fn``.
    A ``{name: description}`` dict is accepted too (order/type from ``fn``).
    """
    sig = signature_params(fn) if fn is not None else []
    type_by_name = dict(sig)

    # Preferred path: ordered list of dicts / strings / Param.
    if isinstance(spec, (list, tuple)):
        params: List[Param] = []
        for entry in spec:
            if isinstance(entry, Param):
                p = entry
            elif isinstance(entry, str):
                p = Param(name=entry.strip())
            elif isinstance(entry, dict):
                p = Param(
                    name=entry["name"],
                    type=entry.get("type", "unknown"),
                    description=entry.get("description", ""),
                    required=entry.get("required", True),
                )
            else:
                raise TypeError(f"normalize_params: bad param entry {entry!r}")
            if p.type == "unknown" and p.name in type_by_name:
                p.type = type_by_name[p.name]
            params.append(p)
        return params

    # Back-compat: {name: description} map — order/type from the signature.
    desc_map: Dict[str, str] = spec if isinstance(spec, dict) else {}
    if sig:
        return [
            Param(name=name, type=type_by_name[name], description=desc_map.get(name, ""))
            for name, _ in sig
        ]
    return [Param(name=name, type="unknown", description=desc) for name, desc in desc_map.items()]


# ── result / error wrappers (internal) ────────────────────────────────────────


class ToolResult:
    """Successful tool output. Internal wrapper — ``call`` returns plain values."""

    def __init__(self, value: Any, meta: Optional[Dict[str, Any]] = None) -> None:
        self.value = value
        self.meta = meta or {}

    @staticmethod
    def of(value: Any, meta: Optional[Dict[str, Any]] = None) -> "ToolResult":
        return ToolResult(value, meta)

    def to_dict(self) -> Dict[str, Any]:
        return {"ok": True, "value": self.value, "meta": self.meta}


class ToolError(Exception):
    """Tool failure. Internal wrapper for an exception out of ``call``."""

    def __init__(self, message: str, category: str = "tool", cause: Any = None) -> None:
        super().__init__(message)
        self.category = category
        self.cause = cause

    def to_dict(self) -> Dict[str, Any]:
        return {"ok": False, "category": self.category, "message": str(self)}


# ── Tool ──────────────────────────────────────────────────────────────────────


class Tool:
    """Base class equivalent of the Rust ``Tool`` trait. Subclass, or use
    :func:`define_tool`."""

    def name(self) -> str:
        """Fully-qualified id, conventionally ``module.member`` (e.g. ``fs.read_file``)."""
        raise ToolError("Tool.name() not implemented", category="engine")

    def description(self) -> str:
        """One-line description shown in the catalog — the only thing the LLM sees."""
        return ""

    def params(self) -> Sequence[ParamSpec]:
        """Ordered params ``[{"name", "type"?, "description"?}]``. Types inferred from ``call``."""
        return []

    def call(self, *args: Any) -> Any:
        """Execute. Positional args spread in; return any plain value. May raise."""
        raise ToolError("Tool.call() not implemented", category="engine")

    def to_dict(self) -> Dict[str, Any]:
        """Catalog entry sent to the cloud — the ``tools`` payload shape.

        ``params`` are ``[name, type]`` tuples (what the server's RunRequest
        expects); per-param descriptions stay client-side.
        """
        return {
            "name": self.name(),
            "description": self.description(),
            "params": [p.to_wire() for p in normalize_params(self.params(), self.call)],
        }

    def call_safe(self, args: Optional[Sequence[Any]] = None) -> Union[ToolResult, ToolError]:
        """Run ``call``, normalizing the return into a ToolResult and any raise into
        a ToolError. Never raises."""
        try:
            out = self.call(*(args or []))
            return out if isinstance(out, ToolResult) else ToolResult(out)
        except ToolError as e:
            return e
        except Exception as e:  # noqa: BLE001 - normalize any failure
            return ToolError(str(e) or repr(e), category="tool", cause=e)


class _FnTool(Tool):
    """A Tool built from a plain spec via :func:`define_tool`."""

    def __init__(
        self,
        name: str,
        call: Callable[..., Any],
        description: str = "",
        params: Optional[List[Param]] = None,
    ) -> None:
        self._name = name
        self._description = description
        self._params = params or []
        self._call = call

    def name(self) -> str:
        return self._name

    def description(self) -> str:
        return self._description

    def params(self) -> Sequence[ParamSpec]:
        return self._params

    def call(self, *args: Any) -> Any:
        return self._call(*args)


def define_tool(
    name: str,
    call: Callable[..., Any],
    description: str = "",
    params: Optional[Union[Sequence[ParamSpec], Dict[str, str]]] = None,
) -> Tool:
    """Build a Tool from a plain spec — no subclass, no wrapping.

    :param name: fully-qualified tool id.
    :param call: the function to run; positional args spread in, returns a plain
        value, raises on error.
    :param description: optional one-liner for the catalog.
    :param params: ordered ``[{"name", "type"?, "description"?}]``. ``type`` is
        inferred from ``call`` when omitted; descriptions are capped at
        ``PARAM_DESC_MAX`` chars.
    """
    if not name:
        raise ValueError("define_tool: name is required")
    if not callable(call):
        raise TypeError("define_tool: call must be callable")
    # Build eagerly so an over-long description fails fast at definition time.
    built = normalize_params(params, call)
    return _FnTool(name=name, call=call, description=description, params=built)

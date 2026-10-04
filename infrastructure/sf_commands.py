"""Render an ASL SSM Task state's shell commands — literals, ``States.Array``
and ``States.Format`` — with JSONPath references resolved against a binding
map.

MOVED HERE FROM ``tests/sf_command_utils.py`` (unchanged parser; new
``render_commands`` on top). It stopped being a test-only helper the moment
``infrastructure/preflight_sweep_stages.py`` needed the SAME parse in
PRODUCTION: the nightly preflight sweep derives its stage list, and the exact
command each stage runs, from ``infrastructure/step_function.json`` rather
than from a hand-maintained list. ``tests/sf_command_utils.py`` is now a
re-export shim so the existing wiring tests keep their import path and there
is exactly one parser (policy ``shared-code``: second adoption lifts, it does
not fork).

``extract_commands`` renders a ``States.Format`` element as its TEMPLATE
string (``'... {} ...'`` with the ``{}`` left in place) — sufficient for the
substring/ordering assertions the wiring tests make, and deliberately
unchanged so those tests are not silently re-scoped by this move.

``render_commands`` is the new, stricter rendering: it substitutes each
``States.Format`` argument, resolving ``$.foo`` against ``bindings`` and
``$$.Execution.Name`` against the reserved context keys. An unresolvable
reference RAISES ``UnresolvedReference`` — it is never rendered as an empty
string. A stage whose command cannot be fully rendered is a stage the sweep
cannot honestly claim to have exercised, and silently substituting a blank
would turn "the definition drifted" into "the stage passed".
"""

from __future__ import annotations

import json
import re

__all__ = [
    "UnresolvedReference",
    "extract_commands",
    "render_commands",
]


class UnresolvedReference(Exception):
    """A ``States.Format`` argument referenced a path absent from bindings."""


def _split_top_level(s: str) -> list[str]:
    """Split on commas not inside an ASL single-quoted string or parens."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_str = False
    i = 0
    while i < len(s):
        ch = s[i]
        if in_str:
            buf.append(ch)
            if ch == "\\" and i + 1 < len(s):
                buf.append(s[i + 1])
                i += 2
                continue
            if ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True
                buf.append(ch)
            elif ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(buf))
                buf = []
            else:
                buf.append(ch)
        i += 1
    if buf:
        parts.append("".join(buf))
    return parts


def _unescape_asl(s: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s):
            out.append(s[i + 1])
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def _asl_literal(token: str) -> str | None:
    """The unescaped body of a single-quoted ASL literal, else None."""
    t = token.strip()
    if len(t) >= 2 and t.startswith("'") and t.endswith("'"):
        return _unescape_asl(t[1:-1])
    return None


def _array_elements(state: dict) -> list[str] | None:
    """Top-level elements of a ``commands.$: States.Array(...)`` expression.

    Returns None when the state uses a static ``commands`` list instead.
    """
    params = state["Parameters"]["Parameters"]
    if "commands" in params:
        return None
    expr = params["commands.$"]
    if not expr.startswith("States.Array("):
        raise ValueError(f"unexpected commands.$: {expr[:80]}")
    inner = expr[expr.index("(") + 1 : expr.rindex(")")]
    return _split_top_level(inner)


def extract_commands(state: dict) -> list[str]:
    """Return the ordered shell-command strings for an SSM Task state.

    ``States.Format`` elements render as their template string, ``{}``
    placeholders intact. Historical behaviour, preserved verbatim for the
    wiring tests that assert on command content and order.
    """
    elements = _array_elements(state)
    if elements is None:
        return list(state["Parameters"]["Parameters"]["commands"])
    out: list[str] = []
    for raw in elements:
        a = raw.strip()
        lit = _asl_literal(a)
        if lit is not None:
            out.append(lit)
        elif a.startswith("States.Format("):
            fmt_inner = a[a.index("(") + 1 : a.rindex(")")]
            first = _split_top_level(fmt_inner)[0].strip()
            first_lit = _asl_literal(first)
            out.append(first_lit if first_lit is not None else first)
        else:
            out.append(a)
    return out


def _resolve(token: str, bindings: dict, context: dict) -> str:
    """Resolve one ``States.Format`` argument to a string.

    Accepts a single-quoted literal, a ``$.key`` / ``$.a.b`` execution-input
    reference, or a ``$$.`` reserved-context reference. Anything else — a
    nested intrinsic, an unknown path — RAISES. Fail loud: an argument the
    sweep cannot resolve means the command it would run is not the command
    the pipeline runs.
    """
    t = token.strip()
    lit = _asl_literal(t)
    if lit is not None:
        return lit

    if t.startswith("$$."):
        try:
            cur = context
            for part in t[3:].split("."):
                cur = cur[part]
        except (KeyError, TypeError) as exc:
            raise UnresolvedReference(
                f"reserved-context reference {t!r} is not in the sweep's context map"
            ) from exc
        return str(cur)

    if t.startswith("$."):
        cur = bindings
        for part in t[2:].split("."):
            if not isinstance(cur, dict) or part not in cur:
                raise UnresolvedReference(
                    f"execution-input reference {t!r} is not in the sweep's bindings"
                )
            cur = cur[part]
        if isinstance(cur, bool):
            return "true" if cur else "false"
        return str(cur)

    raise UnresolvedReference(
        f"argument {t[:60]!r} is neither a literal nor a resolvable JSONPath "
        "(nested intrinsics are not rendered — see sf_commands.render_commands)"
    )


def _render_json_to_string(element: str, bindings: dict) -> str:
    """Render a top-level ``States.JsonToString($.path)`` command element.

    Step Functions serialises the referenced value as compact JSON (no
    whitespace), which is what this emits. Only a single ``$.`` execution-input
    path is accepted; the referenced value must be present in ``bindings`` —
    an absent one RAISES, same fail-loud rule as ``_resolve``.
    """
    inner = element[element.index("(") + 1 : element.rindex(")")].strip()
    if not inner.startswith("$.") or "(" in inner:
        raise UnresolvedReference(
            f"States.JsonToString argument {inner[:60]!r} is not a $. path"
        )
    cur = bindings
    for part in inner[2:].split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise UnresolvedReference(
                f"execution-input reference {inner!r} is not in the sweep's bindings"
            )
        cur = cur[part]
    return json.dumps(cur, separators=(",", ":"))


_PLACEHOLDER = re.compile(r"\{\}")


def render_commands(state: dict, bindings: dict, context: dict) -> list[str]:
    """Fully render an SSM Task state's commands with references resolved.

    ``bindings`` stands in for the execution input ``$``; ``context`` for the
    reserved ``$$`` context object (at minimum ``{"Execution": {"Name": ...}}``).

    Raises ``UnresolvedReference`` if any ``States.Format`` argument cannot be
    resolved, and ``ValueError`` if a template's placeholder count does not
    match its argument count.
    """
    elements = _array_elements(state)
    if elements is None:
        return list(state["Parameters"]["Parameters"]["commands"])

    out: list[str] = []
    for raw in elements:
        a = raw.strip()
        lit = _asl_literal(a)
        if lit is not None:
            out.append(lit)
            continue
        if a.startswith("States.JsonToString(") and a.endswith(")"):
            out.append(_render_json_to_string(a, bindings))
            continue
        if not a.startswith("States.Format("):
            raise UnresolvedReference(
                f"command element {a[:60]!r} is neither a literal, States.Format "
                "nor States.JsonToString"
            )
        fmt_inner = a[a.index("(") + 1 : a.rindex(")")]
        parts = _split_top_level(fmt_inner)
        template = _asl_literal(parts[0])
        if template is None:
            raise UnresolvedReference(
                f"States.Format template is not a literal: {parts[0][:60]!r}"
            )
        args = [_resolve(p, bindings, context) for p in parts[1:]]
        slots = len(_PLACEHOLDER.findall(template))
        if slots != len(args):
            raise ValueError(
                f"States.Format arity mismatch: template has {slots} placeholder(s), "
                f"{len(args)} argument(s) — {template[:60]!r}"
            )
        rendered: list[str] = []
        idx = 0
        pos = 0
        for m in _PLACEHOLDER.finditer(template):
            rendered.append(template[pos : m.start()])
            rendered.append(args[idx])
            idx += 1
            pos = m.end()
        rendered.append(template[pos:])
        out.append("".join(rendered))
    return out

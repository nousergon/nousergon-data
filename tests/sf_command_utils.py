"""Shared helper: extract an SSM state's shell commands whether they are
stored as a static ``commands`` list or as a ``commands.$`` ASL intrinsic.

The run_date-at-SF-start fix (fix/sf-stamp-run-date) rebuilt the
Backtester / Parity / Evaluator command arrays via ``States.Array(...)``
so an ``export RUN_DATE='<$.run_date>'`` element (a ``States.Format``)
can be injected before the ``spot_backtest.sh`` launch. The wiring
tests assert on command *content/order*, which is unchanged — they just
need to read through the intrinsic. This parser renders literals
(unescaping ASL ``\\'`` ``\\\\`` ``\\{`` ``\\}``) and a ``States.Format``
element as its template string, which is sufficient for the substring /
ordering assertions.
"""

from __future__ import annotations


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


def extract_commands(state: dict) -> list[str]:
    """Return the ordered shell-command strings for an SSM Task state."""
    params = state["Parameters"]["Parameters"]
    if "commands" in params:
        return list(params["commands"])
    expr = params["commands.$"]
    assert expr.startswith("States.Array("), f"unexpected commands.$: {expr[:60]}"
    inner = expr[expr.index("(") + 1 : expr.rindex(")")]
    out: list[str] = []
    for raw in _split_top_level(inner):
        a = raw.strip()
        if a.startswith("'") and a.endswith("'"):
            out.append(_unescape_asl(a[1:-1]))
        elif a.startswith("States.Format("):
            fmt_inner = a[a.index("(") + 1 : a.rindex(")")]
            first = _split_top_level(fmt_inner)[0].strip()
            out.append(
                _unescape_asl(first[1:-1])
                if first.startswith("'") and first.endswith("'")
                else first
            )
        else:
            out.append(a)
    return out

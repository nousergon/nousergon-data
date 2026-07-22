"""features/gen_schema_md.py — generate SCHEMA.md §3 field-catalog tables
from features/registry.py::CATALOG (alpha-engine-config#2590).

Background: SCHEMA.md §3 used to be a hand-maintained Markdown table
kept in sync with ``registry.CATALOG`` by author discipline alone. The
two drifted once in production (the ``avg_volume_20d`` incident —
emitted as a normalized ratio but consumed as raw shares, silently
failing a liquidity gate for 901/903 tickers for months; see SCHEMA.md
§1). This script makes §3's per-group tables a MECHANICALLY GENERATED
artifact rendered from ``CATALOG``'s ``units`` / ``formula`` /
``consumers`` / ``display_order`` fields, so drift is no longer
possible by construction — ``tests/test_schema_contract.py`` fails CI
if the committed file disagrees with a fresh render.

Only the ``| Field | Units | Compute | Consumers |`` table BLOCKS are
generated. Everything else in §3 (lead-in paragraph, per-group prose
such as the Factor-loadings intro + the ``roe_zscore`` known-degenerate
writeup, and the entirety of §3b Private-pack columns) is hand-written
and preserved byte-for-byte by this script — it only ever replaces text
between a table's header line and its last row.

Usage:
    python3 features/gen_schema_md.py            # print a diff-friendly
                                                   # dump of each group's
                                                   # generated table to stdout
    python3 features/gen_schema_md.py --write     # rewrite features/SCHEMA.md
                                                   # in place (table blocks only)
    python3 features/gen_schema_md.py --check     # exit 1 if the committed
                                                   # file's §3 tables differ
                                                   # from a fresh render
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from features.registry import CATALOG, PRIVATE_PACK_COMPUTE, FeatureEntry

SCHEMA_MD = Path(__file__).resolve().parent / "SCHEMA.md"

TABLE_HEADER = "| Field | Units | Compute | Consumers |"
TABLE_SEP = "|---|---|---|---|"

# §3 subsection heading (exact, byte-for-byte) -> CATALOG group key.
# Order here is the canonical §3 subsection order.
GROUP_SECTIONS: list[tuple[str, str]] = [
    ("### Technical (per-ticker, daily refresh)", "technical"),
    ("### Macro (one row per date — `per_ticker=False`)", "macro"),
    ("### Regime interactions (per-ticker × macro)", "interaction"),
    ("### Alternative data (weekly refresh)", "alternative"),
    ("### Fundamental (quarterly refresh)", "fundamental"),
    ("### Factor loadings (cross-sectional, daily refresh)", "factor_loading"),
]


def _compute_cell(entry: FeatureEntry) -> str:
    """The Compute-column cell text.

    Private-pack rows (config#1032) disclose no formula — the literal
    sentinel string replaces it, per SCHEMA.md §3b's disclosure rule.
    Public rows render ``entry.formula`` verbatim.
    """
    if entry.compute == PRIVATE_PACK_COMPUTE:
        return "private pack"
    return entry.formula


def render_group_table(group: str) -> str:
    """Render one group's full `| Field | Units | Compute | Consumers |`
    table (header + separator + one row per CATALOG entry in that group,
    sorted by ``display_order``), matching the exact compact GFM style
    already used in SCHEMA.md (no column-width padding).
    """
    entries = [f for f in CATALOG if f.group == group]
    entries.sort(key=lambda f: f.display_order)
    lines = [TABLE_HEADER, TABLE_SEP]
    for f in entries:
        lines.append(
            f"| `{f.name}` | {f.units} | {_compute_cell(f)} | {f.consumers} |"
        )
    return "\n".join(lines)


def render_all_tables() -> dict[str, str]:
    """Return {section_header: rendered_table_text} for every §3 group."""
    return {header: render_group_table(group) for header, group in GROUP_SECTIONS}


def _find_table_span(text: str, header_line: str) -> tuple[int, int]:
    """Return the (start, end) char offsets of the table block (header
    line through the last consecutive ``| ... |`` row) that immediately
    follows ``header_line`` in ``text``. Skips any blank lines / prose
    between the section heading and the table itself, so prose (e.g. the
    Factor-loadings intro + roe_zscore writeup) is left untouched.
    """
    header_idx = text.index(header_line)
    # Find the table header line (`| Field | Units | Compute | Consumers |`)
    # searching forward from the section heading.
    table_start = text.index(TABLE_HEADER, header_idx)
    # The separator line must immediately follow.
    sep_start = text.index(TABLE_SEP, table_start)
    assert text[table_start:sep_start].strip() == TABLE_HEADER, (
        f"Unexpected content between table header and separator for {header_line!r}"
    )
    # Consume consecutive "| ... |" row lines after the separator.
    pos = sep_start + len(TABLE_SEP)
    # Skip the newline after the separator.
    assert text[pos] == "\n"
    pos += 1
    row_re = re.compile(r"^\|.*\|[ \t]*$")
    end = pos
    while True:
        newline_idx = text.find("\n", pos)
        line = text[pos:newline_idx if newline_idx != -1 else len(text)]
        if row_re.match(line):
            end = newline_idx + 1 if newline_idx != -1 else len(text)
            pos = end
        else:
            break
    return table_start, end


def rewrite_schema_md(text: str) -> str:
    """Return ``text`` with every §3 group table replaced by a fresh render.

    Only table blocks (header line through last row) are touched; all
    surrounding prose, headings, and §3b are preserved verbatim.
    """
    out = text
    for header_line, group in GROUP_SECTIONS:
        start, end = _find_table_span(out, header_line)
        out = out[:start] + render_group_table(group) + "\n" + out[end:]
    return out


def main(argv: list[str]) -> int:
    if "--write" in argv:
        text = SCHEMA_MD.read_text(encoding="utf-8")
        new_text = rewrite_schema_md(text)
        SCHEMA_MD.write_text(new_text, encoding="utf-8")
        print(f"Rewrote {SCHEMA_MD} (§3 table blocks only).")
        return 0
    if "--check" in argv:
        text = SCHEMA_MD.read_text(encoding="utf-8")
        fresh = rewrite_schema_md(text)
        if text != fresh:
            print(
                "DRIFT: features/SCHEMA.md §3 does not match a fresh render "
                "from features/registry.py::CATALOG. Run "
                "`python3 features/gen_schema_md.py --write` and commit the "
                "result.",
                file=sys.stderr,
            )
            return 1
        print("features/SCHEMA.md §3 matches a fresh CATALOG render. OK.")
        return 0
    # Default: print each group's rendered table to stdout for human/CI diff.
    for header_line, group in GROUP_SECTIONS:
        print(header_line)
        print()
        print(render_group_table(group))
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

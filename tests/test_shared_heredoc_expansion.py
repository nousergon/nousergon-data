"""The dispatch launchers' heredocs pass the shared expansion detector.

An unquoted heredoc is expanded by the shell that BUILDS it, not the one that
runs it. A variable assigned inside the body and referenced there unescaped
(``$LOG_FILE`` instead of ``\\$LOG_FILE``) is substituted on the dispatcher,
where it is unset, so the worker receives an empty string. The detector for
that class is shared fleet-wide as ``nousergon_lib.shell_contracts``
(nousergon-lib-PR562, alpha-engine-config-I13949); this file runs it over
this repo's two dispatch launchers (alpha-engine-config-I13952).

Three properties are asserted per script:

* the script exists and carries the unquoted heredocs this guard is for, so a
  rename or a delimiter quoted away cannot leave it guarding nothing;
* the shared detector actually SEES every unquoted heredoc: a probe assignment
  plus an unescaped reference is inserted into each one in a temporary copy,
  and the detector must report it at that heredoc's opening line;
* the real script, as committed, has no offenders.

Recorded mutation evidence (2026-11-22, nousergon-lib v0.124.229, run with
``detect_heredoc_expansions`` against temporary copies; the repository files
were not changed):

* ``spot_data_weekly.sh``: un-escaping ``tee -a "\\$LOG_FILE"`` to
  ``tee -a "$LOG_FILE"`` in the phase2-only heredoc (opening line 1247,
  reference line 1261) produced exactly
  ``HeredocExpansionDiagnostic(variable='LOG_FILE', opening_line=1247,
  reference_line=1261)``.
* ``run_weekly_offcycle.sh``: its four heredocs are JSON bodies with no
  heredoc-local assignment, so there was nothing to un-escape. The temporary
  mutation added ``_CALLER=offcycle`` and an unescaped ``echo "$_CALLER"``
  inside ``shell_input``'s heredoc (opening line 158), which produced exactly
  ``HeredocExpansionDiagnostic(variable='_CALLER', opening_line=158,
  reference_line=161)``.

The first run over ``main`` also found one real finding in
``spot_data_weekly.sh``: ``RUN_TOKEN`` in the phase2-only heredoc. That
expansion is a deliberate dispatcher-side injection (the SF execution name
reaches the launcher through its environment), so it is declared in the
dispatching shell, which is how the detector recognises an intentional
injection; nothing here exempts it.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest
from nousergon_lib.shell_contracts import detect_heredoc_expansions

INFRA = Path(__file__).resolve().parent.parent / "infrastructure"

# The SSM / payload dispatch heredocs each script exists to send. Every one of
# them must stay unquoted (its body interpolates dispatcher values on purpose)
# and discoverable, or the guard below silently covers less than it claims.
EXPECTED_UNQUOTED: dict[str, tuple[str, ...]] = {
    "run_weekly_offcycle.sh": ("EOF", "EOF", "EOF", "EOF"),
    "spot_data_weekly.sh": (
        "DEPS",
        "SMOKE",
        "RAG_SMOKE",
        "RAG_ONLY_PREFLIGHT",
        "RAG_ONLY",
        "PHASE2_ONLY",
        "PREFLIGHT_WORKLOADS",
        "WORKLOADS",
    ),
}

# An unquoted heredoc opener: `<<WORD` or `<<-WORD`, not a here-string
# (`<<<`) and not a quoted delimiter (`<<'WORD'`, `<<"WORD"`).
_UNQUOTED_OPENER = re.compile(r"(?<!<)<<(-?)([A-Za-z_][A-Za-z0-9_]*)\b")
_PROBE = "HEREDOC_DISCOVERY_PROBE"


def _unquoted_heredocs(text: str) -> list[tuple[int, str]]:
    """(1-based opening line, delimiter) for each terminated unquoted heredoc."""
    lines = text.splitlines()
    found: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = None if line.lstrip().startswith("#") else _UNQUOTED_OPENER.search(line)
        if match is None:
            index += 1
            continue
        strip_tabs, delimiter = bool(match.group(1)), match.group(2)
        end = index + 1
        while end < len(lines):
            candidate = lines[end].lstrip("\t") if strip_tabs else lines[end]
            if candidate == delimiter:
                break
            end += 1
        if end == len(lines):
            index += 1
            continue
        found.append((index + 1, delimiter))
        index = end + 1
    return found


@pytest.mark.parametrize("script", sorted(EXPECTED_UNQUOTED))
def test_script_and_its_unquoted_heredocs_are_discoverable(script: str) -> None:
    path = INFRA / script
    assert path.is_file(), f"{path} is gone; move this guard with it"
    delimiters = [delimiter for _, delimiter in _unquoted_heredocs(path.read_text())]
    missing = list(EXPECTED_UNQUOTED[script])
    for delimiter in delimiters:
        if delimiter in missing:
            missing.remove(delimiter)
    assert not missing, (
        f"{script}: expected unquoted dispatch heredocs {missing} were not found "
        f"(found {delimiters}); a quoted or renamed delimiter leaves this guard blind"
    )


@pytest.mark.parametrize("script", sorted(EXPECTED_UNQUOTED))
def test_shared_detector_sees_every_unquoted_heredoc(script: str, tmp_path: Path) -> None:
    source = INFRA / script
    text = source.read_text()
    heredocs = _unquoted_heredocs(text)
    assert heredocs, f"{script} has no unquoted heredoc to guard"
    common = INFRA / "_spot_common.sh"
    if common.is_file():
        shutil.copy(common, tmp_path / common.name)
    for opening_line, delimiter in heredocs:
        lines = text.splitlines(keepends=True)
        lines.insert(opening_line, f'{_PROBE}=1\necho "${_PROBE}"\n')
        probe_copy = tmp_path / script
        probe_copy.write_text("".join(lines))
        diagnostics = detect_heredoc_expansions(probe_copy)
        assert [
            (d.variable, d.opening_line, d.reference_line)
            for d in diagnostics
            if d.variable == _PROBE
        ] == [(_PROBE, opening_line, opening_line + 2)], (
            f"{script}: the shared detector did not discover the <<{delimiter} "
            f"heredoc opened on line {opening_line}"
        )


@pytest.mark.parametrize("script", sorted(EXPECTED_UNQUOTED))
def test_dispatch_heredocs_have_no_unescaped_local_expansions(script: str) -> None:
    diagnostics = detect_heredoc_expansions(INFRA / script)
    assert diagnostics == [], "\n".join(
        f"{script}:{d.reference_line}: ${d.variable} is assigned inside the heredoc "
        f"opened on line {d.opening_line} but expanded by the dispatching shell; "
        f"escape it as \\${d.variable}, or assign it in the dispatcher if the "
        "injection is intentional"
        for d in diagnostics
    )

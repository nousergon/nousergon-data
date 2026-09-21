"""The producer legs run independently and the comparator always runs.

`alpha-engine-config-I11200` deliverable 2.

`shadow-weekday` was `leg1 && leg2 && leg3 && leg4 && parity`.
`weekly_collector` exits 1 on anything less than fully `ok` -- its declared
fail-loud contract, correct for production where a degraded feature store must
halt the pipeline. Chained with `&&`, that same contract meant `shadow parity`
ran only if EVERY collector on EVERY leg was perfectly ok. It never once was:

    2026-09-14 dispatch   died in leg 2   (UniverseFreshnessViolation)
    2026-09-18 dispatch   died end leg 3  (features=degraded, prices=partial)

Two unrelated causes, **zero reports** -- and the 09-18 run had already written
1,977 objects into `staging/shadow/2026-09-18/` that the comparator could read.
Dispatching the standalone `shadow-parity` workload over exactly that prefix
produced a complete 959-key report in minutes, which is the measurement proving
only the `&&` was in the way.

A production halt-the-pipeline contract was being used as a sequencing operator
for a diagnostic tool.
"""

from __future__ import annotations

import importlib.util
import pathlib
import re

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DISPATCHER = _REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"


def _dispatcher():
    spec = importlib.util.spec_from_file_location("data_spot_dispatcher_index", _DISPATCHER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _shadow_weekday() -> str:
    return _dispatcher()._WORKLOADS["shadow-weekday"].format(trading_day="2026-09-18")


def test_no_leg_or_the_comparator_is_guarded_by_a_preceding_and():
    """The whole defect in one assertion.

    NOT a blanket ban on `&&`: `[ $RC -ne 0 ] && RC_ALL=$RC` is how a failed
    leg still fails the workload, and that must stay. The property is that no
    LEG INVOCATION and not the COMPARATOR is conditional on what ran before.
    """
    cmd = _shadow_weekday()
    guarded = re.findall(r"&&\s*(python -m shadow (?:run|parity)[^;]*)", cmd)
    assert not guarded, (
        "these shadow invocations are guarded by a preceding `&&`, so an earlier leg's "
        f"non-zero exit skips them: {guarded}. That is how two dispatches produced zero "
        "parity reports on 2026-09-20 (alpha-engine-config-I11200)."
    )


def test_the_comparator_runs_after_every_leg():
    cmd = _shadow_weekday()
    parity_at = cmd.index("shadow parity")
    for leg in ("--morning-enrich", "--morning-arctic-append", "--daily ", "--daily-arctic-append"):
        assert leg in cmd, f"leg {leg!r} missing from shadow-weekday"
        assert cmd.index(leg) < parity_at, f"{leg!r} is sequenced after the comparator"


def test_every_leg_records_its_exit_code_and_the_file_reaches_the_comparator():
    cmd = _shadow_weekday()
    for name in (
        "morning-enrich",
        "morning-arctic-append",
        "post-market-data",
        "post-market-arctic-append",
    ):
        assert f"'{name}\\t%s\\n'" in cmd or f"{name}\\t%s" in cmd, (
            f"leg {name!r} does not record its exit code to the legs file"
        )
    assert "--legs-file" in cmd, "the legs file is written but never handed to the comparator"


def test_a_failed_leg_still_fails_the_workload():
    """Independent legs must not turn a failed run green.

    The comparator running is the change; the dispatcher's success/failure
    reporting is deliberately unchanged.
    """
    cmd = _shadow_weekday()
    assert "RC_ALL" in cmd and "exit $RC_ALL" in cmd, (
        "the workload no longer propagates a failed leg's exit code — a degraded run would "
        "report success"
    )


# ── the legs file the comparator reads ─────────────────────────────────────

def _read_legs(path):
    from shadow.__main__ import _read_legs as reader

    return reader(path)


def test_tsv_legs_are_parsed_with_an_ok_flag(tmp_path):
    p = tmp_path / "legs.tsv"
    p.write_text("morning-enrich\t0\npost-market-data\t1\n")
    legs = _read_legs(str(p))
    assert legs == [
        {"name": "morning-enrich", "exit_code": 0, "ok": True},
        {"name": "post-market-data", "exit_code": 1, "ok": False},
    ]


def test_json_legs_are_also_accepted(tmp_path):
    p = tmp_path / "legs.json"
    p.write_text('[{"name": "a", "exit_code": 0}]')
    assert _read_legs(str(p))[0]["ok"] is True


def test_no_legs_file_is_not_a_claim_that_every_leg_ran():
    """`[]` renders as `legs_known: false`, which is the honest statement for
    a comparator run on its own over an existing prefix."""
    assert _read_legs(None) == []


def test_a_named_but_empty_legs_file_raises(tmp_path):
    """Degrading to `[]` here would publish 'the comparator was not told' for a
    caller that DID tell it — a silent downgrade of the report's own claim."""
    p = tmp_path / "legs.tsv"
    p.write_text("   \n")
    with pytest.raises(ValueError, match="empty"):
        _read_legs(str(p))


def test_a_malformed_legs_line_raises(tmp_path):
    p = tmp_path / "legs.tsv"
    p.write_text("morning-enrich 0\n")
    with pytest.raises(ValueError, match="name<TAB>exit_code"):
        _read_legs(str(p))


def test_the_report_carries_legs_and_legs_known():
    import datetime as dt

    from shadow.parity import ParityReport

    report = ParityReport(
        trading_day=dt.date(2026, 9, 18),
        bucket="b",
        shadow_prefix="staging/shadow/2026-09-18/",
        code_sha="abc",
        rows=[],
        excluded=[],
        rel_tolerance=1e-6,
        absolute_tolerance=1e-9,
        generated_at="2026-09-20T19:07:24Z",
        legs=[{"name": "post-market-data", "exit_code": 1, "ok": False}],
    )
    body = report.as_dict()
    assert body["legs_known"] is True
    assert body["legs"][0]["name"] == "post-market-data"
    assert body["legs"][0]["ok"] is False


def test_a_report_with_no_legs_says_so_rather_than_implying_completeness():
    import datetime as dt

    from shadow.parity import ParityReport

    body = ParityReport(
        trading_day=dt.date(2026, 9, 18),
        bucket="b",
        shadow_prefix="staging/shadow/2026-09-18/",
        code_sha="abc",
        rows=[],
        excluded=[],
        rel_tolerance=1e-6,
        absolute_tolerance=1e-9,
        generated_at="2026-09-20T19:07:24Z",
    ).as_dict()
    assert body["legs"] == []
    assert body["legs_known"] is False, (
        "a report that was not told what the legs did must say so; silence reads as "
        "'every leg ran', which is the assumption I11200 exists to stop"
    )

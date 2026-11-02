"""D02 weekday reference-diff probe (alpha-engine-config-I13045).

Done-when from the issue: fed the 2026-10-06 roster, the probe names PSKY/SKYD
and the file to edit. These also pin the other shapes D02 fails on, that a
committed declaration clears each one, and that the probe writes nothing and
asks Polygon nothing.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from collectors import historical_constituents as hc
from collectors.historical_constituents import ADDED, REMOVED, ConstituentChange
from validators import d02_reference_diff_probe as p

BASE = ["AAA", "BBB", "CCC", "PSKY"]
MID = ["MMM", "NNN"]
OCT_REF = [  # the reference table: one real index change, nothing about PSKY
    ConstituentChange("2026-09-22", "CCC", ADDED),
    ConstituentChange("2026-09-22", "OLD", REMOVED),
]


def _stored(*rosters: tuple[str, list[str]]):
    sp500 = dict(rosters)
    universe = {d: [*r, *MID] for d, r in rosters}
    return sp500, universe


def _run(live_sp500, *, stored=None, live_date="2026-10-06", reference=OCT_REF, **kw):
    sp500, universe = stored or _stored(
        ("2026-09-19", ["AAA", "BBB", "OLD", "PSKY"]),
        ("2026-09-26", BASE),
        ("2026-10-03", BASE),
    )
    kw.setdefault("known_retickers", [])
    kw.setdefault("declarations", [])
    return p.probe(
        stored_sp500=sp500, stored_universe=universe,
        live_sp500=live_sp500, live_universe=[*live_sp500, *MID],
        live_date=live_date, reference=reference, **kw,
    )


def test_the_2026_10_06_roster_names_psky_skyd_and_the_retickers_file():
    report = _run(["AAA", "BBB", "CCC", "SKYD"])
    assert report["status"] == "undeclared"
    [finding] = report["findings"]
    assert finding["kind"] == "reticker_candidate"
    assert finding["tickers"] == ["PSKY", "SKYD"]
    assert finding["declaration_file"] == "collectors/data/sp500_known_retickers.json"
    assert finding["observed_on"] == "2026-10-06"
    assert finding["new_this_week"] is True
    assert finding["suggested_entry"]["old"] == "PSKY"
    assert finding["suggested_entry"]["new"] == "SKYD"
    message = p.alert_message(report)
    assert "PSKY" in message and "SKYD" in message
    assert "sp500_known_retickers.json" in message
    assert "Auto-fix:" in message and "Resolution:" in message


def test_the_committed_reticker_file_clears_it():
    # nousergon-data#2133 committed PSKY -> SKYD; read the real file.
    report = _run(["AAA", "BBB", "CCC", "SKYD"], known_retickers=None)
    assert report["status"] == "clear", report["findings"]


SEPT = _stored(("2026-09-19", ["AAA", "BBB", "OLD", "PSKY"]), ("2026-09-26", BASE))


def test_a_spinoff_addition_names_the_declarations_file():
    report = _run([*BASE, "VYLR"], stored=SEPT, live_date="2026-10-01")
    [finding] = report["findings"]
    assert finding["kind"] == "spinoff_addition_candidate"
    assert finding["tickers"] == ["VYLR"]
    assert finding["declaration_file"] == "collectors/data/sp500_declared_spinoff_additions.json"
    assert finding["suggested_entry"]["valid_through"] == "2026-10-31"


def test_the_committed_spinoff_declaration_clears_it():
    report = _run([*BASE, "VYLR"], stored=SEPT, live_date="2026-10-01", declarations=None)
    assert report["status"] == "clear", report["findings"]
    assert report["declared_exceptions"]


def test_a_real_index_change_the_reference_lists_is_clear():
    ref = [*OCT_REF, ConstituentChange("2026-10-05", "NEWCO", ADDED),
           ConstituentChange("2026-10-05", "BBB", REMOVED)]
    report = _run(["AAA", "CCC", "NEWCO", "PSKY"], reference=ref)
    assert report["status"] == "clear", report["findings"]


def test_a_reference_change_no_snapshot_shows_is_named_with_no_file():
    ref = [*OCT_REF, ConstituentChange("2026-10-02", "ZZZ", ADDED)]
    report = _run(BASE, reference=ref)
    [finding] = report["findings"]
    assert finding["kind"] == "reference_not_observed"
    assert finding["declaration_file"] is None


def test_a_lone_removal_points_at_the_retickers_file():
    report = _run(["AAA", "BBB", "CCC"])
    [finding] = report["findings"]
    assert finding["kind"] == "unexplained_removal"
    assert finding["tickers"] == ["PSKY"]


def test_an_old_unexplained_disagreement_is_not_new_this_week():
    stored = _stored(
        ("2026-09-19", ["AAA", "BBB", "OLD", "PSKY"]),
        ("2026-09-26", ["AAA", "BBB", "CCC", "PSKY", "XTRA"]),
        ("2026-10-03", ["AAA", "BBB", "CCC", "PSKY", "XTRA"]),
    )
    report = _run(["AAA", "BBB", "CCC", "PSKY", "XTRA"], stored=stored)
    [finding] = report["findings"]
    assert finding["tickers"] == ["XTRA"]
    assert finding["new_this_week"] is False


def test_a_prior_runs_verdict_settles_a_swap():
    report = _run(["AAA", "BBB", "CCC", "SKYD"], prior={"renamed": {"PSKY": "SKYD"}})
    assert report["status"] == "clear", report["findings"]


def test_a_price_identity_reticker_is_reported_as_inferred_not_undeclared():
    sessions = pd.bdate_range("2025-06-02", "2026-10-05")
    rng = np.random.default_rng(7)
    old = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.02, len(sessions)))), index=sessions)
    new = pd.concat([old, pd.Series([old.iloc[-1]], index=[pd.Timestamp("2026-10-06")])])
    closes = {"PSKY": old, "SKYD": new}.get
    report = _run(["AAA", "BBB", "CCC", "SKYD"], closes=closes)
    assert report["status"] == "clear", report["findings"]
    assert report["inferred_retickers"]["PSKY"]["new"] == "SKYD"


def test_an_unsettled_swap_is_listed_as_not_asked_never_queried(monkeypatch):
    def _boom(*_a, **_k):  # D02's Polygon leg must never be reached from the probe
        raise AssertionError("probe called resolve_renames (Polygon)")

    monkeypatch.setattr(hc, "resolve_renames", _boom)
    report = _run(["AAA", "BBB", "CCC", "SKYD"])
    assert "PSKY" in report["rename_checks_not_asked"]


class _ReadOnlyS3:
    """Serves list/get only; any other call fails the test."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.calls: list[str] = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        self.calls.append(name)
        objects = self.objects

        class _Pager:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                yield {"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}

        return _Pager()

    def get_object(self, Bucket, Key):  # noqa: N803
        self.calls.append(f"get:{Key}")
        if Key not in self.objects:
            raise KeyError(Key)
        body = self.objects[Key]

        class _Body:
            def read(self):
                return body

        return {"Body": _Body()}

    def __getattr__(self, name):
        raise AssertionError(f"probe called s3.{name}: it must be read-only")


def _snapshot(sp500):
    return json.dumps({"sp500_tickers": sp500, "sp400_tickers": MID,
                       "tickers": [*sp500, *MID]}).encode()


def test_run_reads_only_and_end_to_end_names_the_reticker(monkeypatch, tmp_path):
    import collectors.constituents as cons

    s3 = _ReadOnlyS3({
        "market_data/weekly/2026-09-26/constituents.json": _snapshot(BASE),
        "market_data/weekly/2026-10-03/constituents.json": _snapshot(BASE),
    })
    live = ["AAA", "BBB", "CCC", "SKYD"]
    weights = cons.SsgaWeights(weight_map={}, index_of={}, raw_sum_by_index={},
                               method="test", members_by_index={"S&P 500": live, "S&P 400": MID})
    monkeypatch.setattr(cons, "_fetch_ssga_membership", lambda: ([*live, *MID], 4, 2, weights))
    monkeypatch.setattr(hc, "_fetch_changes_table", lambda: (None, "test"))
    monkeypatch.setattr(hc, "parse_changes_table", lambda _df: list(OCT_REF))
    no_retickers = tmp_path / "retickers.json"  # the file as it stood before #2133
    no_retickers.write_text(json.dumps({"retickers": []}))
    monkeypatch.setattr(hc, "_KNOWN_RETICKERS_PATH", no_retickers)

    report = p.run(bucket="test-bucket", s3=s3)
    assert report["status"] == "undeclared"
    assert report["findings"][0]["tickers"] == ["PSKY", "SKYD"]
    assert all(c == "list_objects_v2" or c.startswith("get:") for c in s3.calls)


def test_run_reports_an_unreadable_input_as_error(monkeypatch):
    import collectors.constituents as cons

    s3 = _ReadOnlyS3({})

    def _down():
        raise RuntimeError("ssga down")

    monkeypatch.setattr(cons, "_fetch_ssga_membership", _down)
    report = p.run(bucket="test-bucket", s3=s3)
    assert report == {"status": "error", "stage": "live_holdings",
                      "error": "RuntimeError: ssga down"}


@pytest.mark.parametrize("status,code,published", [
    ("clear", 0, False), ("undeclared", 1, True), ("error", 2, False),
])
def test_main_exit_codes_and_alerting(monkeypatch, capsys, status, code, published):
    sent = []
    report = {"status": status, "findings": [], "unexplained": []}
    monkeypatch.setattr(p, "run", lambda bucket: report)
    monkeypatch.setattr(p, "_publish", lambda r: sent.append(r))
    assert p.main(["--bucket", "b"]) == code
    assert bool(sent) is published
    sent.clear()
    assert p.main(["--bucket", "b", "--no-alert"]) == code
    assert sent == []


def test_main_requires_a_bucket(monkeypatch):
    monkeypatch.delenv("NE_RESEARCH_BUCKET", raising=False)
    with pytest.raises(SystemExit):
        p.main([])

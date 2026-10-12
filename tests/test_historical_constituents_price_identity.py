"""A reticker Polygon does not report resolves itself by price identity.

PSKY -> SKYD failed the 2026-10-10 weekly D02 as an "unexplained reference
disagreement" until a human committed it to sp500_known_retickers.json
(nousergon-data-PR2133). The evidence that justified that entry, and every
earlier one, is that the new symbol's price history carries the old symbol's
closes. These pin that the collector now measures it itself, and only accepts
what that measurement can tell apart from real index churn.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from collectors import historical_constituents as hc

SESSIONS = pd.bdate_range("2025-06-02", "2026-10-05")


def _series(seed: int, sessions=SESSIONS) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.02, len(sessions)))), index=sessions)


def _resolution(not_renamed=(), deferred=(), confirmed=()) -> hc.RenameResolution:
    return hc.RenameResolution(
        not_renamed=list(not_renamed), deferred=list(deferred),
        confirmed_by_reference=list(confirmed),
    )


def _closes(table):
    return lambda ticker: table.get(ticker)


def test_a_reticker_resolves_with_its_evidence_and_is_carried_to_the_next_run():
    # Not PSKY/SKYD: that pair is committed now, and settled() omits committed retickers.
    old = _series(1)
    new = pd.concat([old * 1.0000005, pd.Series([old.iloc[-1]], index=[pd.Timestamp("2026-10-06")])])
    res = _resolution(not_renamed=["OLDQ"])
    hc.infer_price_identity_renames(
        res, {"2026-10-06": (["OLDQ"], ["NEWQ"])}, _closes({"OLDQ": old, "NEWQ": new}),
    )
    assert res.renames == {"OLDQ": "NEWQ"}
    assert res.not_renamed == []
    ev = res.inferred["OLDQ"]
    assert ev["new"] == "NEWQ" and ev["effective"] == "2026-10-06"
    assert ev["overlap_sessions"] == len(SESSIONS) and ev["old_last_bar"] == "2026-10-05"
    assert res.settled()["renamed"] == {"OLDQ": "NEWQ"}

    changes, unresolved = hc.changes_from_snapshots(
        {"2026-10-05": ["AAPL", "OLDQ"], "2026-10-06": ["AAPL", "NEWQ"]}, res.renames,
    )
    assert changes == [] and unresolved == []


def test_real_index_churn_stays_a_membership_change():
    res = _resolution(not_renamed=["CTVA"])
    hc.infer_price_identity_renames(
        res, {"2026-10-05": (["CTVA"], ["TWLO"])},
        _closes({"CTVA": _series(2), "TWLO": _series(3)}),
    )
    assert res.renames == {} and res.inferred == {}
    assert res.not_renamed == ["CTVA"]


def test_an_old_symbol_still_trading_after_the_swap_is_not_a_rename():
    old = _series(4, pd.bdate_range("2025-06-02", "2026-10-09"))
    res = _resolution(not_renamed=["GOOGL"])
    hc.infer_price_identity_renames(
        res, {"2026-10-06": (["GOOGL"], ["GOOG"])}, _closes({"GOOGL": old, "GOOG": old.copy()}),
    )
    assert res.renames == {}


def test_a_short_overlap_is_not_enough_evidence():
    short = pd.bdate_range("2026-06-01", "2026-10-05")
    old = _series(5, short)
    res = _resolution(deferred=["NEWCO"])
    hc.infer_price_identity_renames(
        res, {"2026-10-06": (["NEWCO"], ["NEWC"])}, _closes({"NEWCO": old, "NEWC": old.copy()}),
    )
    assert res.renames == {} and res.deferred == ["NEWCO"]


def test_two_matching_arrivals_are_ambiguous_and_left_unresolved():
    old = _series(6)
    res = _resolution(not_renamed=["OLD"])
    hc.infer_price_identity_renames(
        res, {"2026-10-06": (["OLD"], ["NEWA", "NEWB"])},
        _closes({"OLD": old, "NEWA": old.copy(), "NEWB": old.copy()}),
    )
    assert res.renames == {}


def test_a_reference_confirmed_removal_or_missing_price_cache_is_never_inferred():
    old = _series(7)
    res = _resolution(confirmed=["GONE"], not_renamed=["NOCACHE"])
    hc.infer_price_identity_renames(
        res,
        {"2026-10-06": (["GONE", "NOCACHE"], ["SAME", "OTHER"])},
        _closes({"GONE": old, "SAME": old.copy(), "OTHER": _series(8)}),
    )
    assert res.renames == {} and res.not_renamed == ["NOCACHE"]


def test_swap_pairs_keep_the_joining_side():
    pairs = hc.same_date_swap_pairs(
        {"2026-10-05": ["A", "PSKY"], "2026-10-06": ["A", "SKYD"], "2026-10-07": ["A", "SKYD", "Z"]}
    )
    assert pairs == {"2026-10-06": (["PSKY"], ["SKYD"])}

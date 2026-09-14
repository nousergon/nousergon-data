"""alpha-engine-config-I10704 — the DECLARED benchmark-proxy set.

Measured 2026-09-14 in-region: the ArcticDB ``universe`` library on
``alpha-engine-research`` held ``SPY`` and NONE of the five attribution
proxies ``alpha-engine-config/strategy/slots/attribution.yaml`` declares
(``IWM``, ``XLE``, ``XLF``, ``XLK``, ``XLV``). Since ``crucible-PR271`` every
crucible panel compile fetches every declared proxy from that library and
refuses on a missing one, so ``data.weekly`` would have failed at its first
stage on the graded 2026-09-19 arc.

Two root causes, and this file pins both shut:

  1. The proxies were never DECLARED to the producer. ``SPY`` was admitted by
     ``_UNIVERSE_EXTRA``; the five attribution proxies existed only in a
     consumer's YAML, so no producer code path was under any obligation to
     maintain them.
  2. Even a declared XL* proxy could not have been admitted: every scoping
     predicate ANDed the ``_UNIVERSE_EXTRA`` carve-out with a bare
     ``not _is_sector_etf(t)``, which rejects XLE/XLF/XLK/XLV unconditionally.
     That is the third instance of the I2703/I2704 class — one boolean
     expression copied to six sites, drifting at some of them.

Section A pins the declaration. Section B pins admission through EVERY
production predicate. Section C is the mutation case: a declared proxy absent
from the fetched frame must fail LOUD, never degrade to a quieter universe.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from features.compute import (  # noqa: E402
    UNIVERSE_BENCHMARK_PROXIES,
    _SKIP_TICKERS,
    _UNIVERSE_EXTRA,
    _is_sector_etf,
    admits_universe_write,
)

# The five proxies declared by alpha-engine-config/strategy/slots/
# attribution.yaml and read by crucible/slots/__init__.py::
# attribution_factor_symbols. Spelled out here deliberately: this test is the
# producer-side half of a cross-repo contract, so it must fail if someone
# quietly drops one from the declaration rather than reading whatever the
# declaration happens to say.
ATTRIBUTION_PROXIES = ("IWM", "XLK", "XLV", "XLF", "XLE")


# ── A. The declaration ──────────────────────────────────────────────────────


def test_every_attribution_proxy_is_declared():
    for sym in ATTRIBUTION_PROXIES:
        assert sym in UNIVERSE_BENCHMARK_PROXIES, (
            f"{sym} is declared by attribution.yaml and fetched by every "
            f"crucible panel compile, but is not in the producer's declared "
            f"set — the library will not hold it and data.weekly refuses."
        )
    assert "SPY" in UNIVERSE_BENCHMARK_PROXIES


def test_legacy_alias_is_the_same_object():
    """There is ONE list, not two names holding two sets."""
    assert _UNIVERSE_EXTRA is UNIVERSE_BENCHMARK_PROXIES


def test_declared_proxies_are_skip_protected():
    """Skip-protection is what keeps prune_delisted_tickers from deleting a
    proxy that is (correctly) absent from constituents.json."""
    assert UNIVERSE_BENCHMARK_PROXIES <= _SKIP_TICKERS, (
        f"not skip-protected: {sorted(UNIVERSE_BENCHMARK_PROXIES - _SKIP_TICKERS)}"
    )


def test_declared_proxies_are_always_downloaded():
    """The price collector must maintain a parquet for every declared proxy.

    A proxy with no price cache has nothing for the universe write to write
    from — which is precisely IWM's state before I10704. Lockstep test rather
    than a runtime import: collectors/prices.py is imported at collector
    process start and must not pull in the feature-compute graph.
    """
    from collectors.prices import _ALWAYS_DOWNLOAD

    missing = sorted(UNIVERSE_BENCHMARK_PROXIES - set(_ALWAYS_DOWNLOAD))
    assert not missing, (
        f"declared proxies absent from collectors/prices.py::_ALWAYS_DOWNLOAD: "
        f"{missing} — no price-cache parquet would ever be written for them."
    )


# ── B. Admission through every production predicate ─────────────────────────


@pytest.mark.parametrize("sym", sorted(UNIVERSE_BENCHMARK_PROXIES))
def test_declared_proxy_admitted_by_the_one_predicate(sym):
    assert admits_universe_write(sym), (
        f"{sym} is declared but the universe-write predicate refuses it. If "
        f"it is an XL* symbol this is the I10704 defect verbatim: the "
        f"sector-ETF prefix test overriding the declaration."
    )


def test_sector_etf_test_still_rejects_undeclared_sector_etfs():
    """The declaration widens the write set; it does not disable the filter."""
    # XLRE is deliberately absent: it is 4 characters, so ``_is_sector_etf``
    # (a 3-char prefix test) has never matched it — the pre-existing "XLRE
    # leak" pinned by tests/test_daily_append_universe_chunking.py. That is
    # not this change's to fix; it is named here so the omission reads as
    # deliberate rather than as an oversight.
    for sym in ("XLI", "XLY", "XLP", "XLU", "XLB", "XLC"):
        assert _is_sector_etf(sym)
        assert not admits_universe_write(sym), (
            f"{sym} is a sector ETF and NOT declared — it must stay out of "
            f"`universe`, or the panel's cross-section silently grows."
        )


def test_macro_only_symbols_still_refused():
    for sym in ("VIX", "VIX3M", "TNX", "IRX", "GLD", "USO", "^VIX", "^TNX"):
        assert not admits_universe_write(sym)


def test_ordinary_stock_still_admitted():
    assert admits_universe_write("AAPL")


@pytest.mark.parametrize("sym", sorted(UNIVERSE_BENCHMARK_PROXIES))
def test_backfill_write_path_admits_declared_proxy_without_constituents(sym):
    """The write-path site: a proxy is never in constituents.json, so it must
    be admitted by the declaration alone."""
    price_data = {sym: object(), "AAPL": object()}
    constituents = {"AAPL"}
    admitted = [
        t for t in price_data
        if admits_universe_write(t)
        and price_data[t] is not None
        and (t in constituents or t in UNIVERSE_BENCHMARK_PROXIES)
    ]
    assert sym in admitted


@pytest.mark.parametrize("sym", sorted(UNIVERSE_BENCHMARK_PROXIES))
def test_backfill_ticker_filter_does_not_refuse_a_declared_proxy(sym):
    """``--ticker XLE`` must not be refused as a sector ETF.

    Pins the two early refusals in ``builders.backfill.backfill``'s
    ticker_filter error split. Without the carve-out, the in-region loader
    could never write four of the six proxies.
    """
    from features.compute import _SKIP_TICKERS as SKIP

    refused_as_skip = sym in SKIP and sym not in UNIVERSE_BENCHMARK_PROXIES
    refused_as_sector = _is_sector_etf(sym) and sym not in UNIVERSE_BENCHMARK_PROXIES
    assert not refused_as_skip
    assert not refused_as_sector


def test_backfill_source_carries_both_ticker_filter_carve_outs():
    src = (_REPO_ROOT / "builders" / "backfill.py").read_text()
    assert "ticker_filter not in UNIVERSE_BENCHMARK_PROXIES" in src, (
        "builders/backfill.py's ticker_filter refusals no longer exempt a "
        "declared proxy — `--ticker XLE` would refuse the write the "
        "declaration exists to authorise."
    )


def test_loader_refuses_an_undeclared_symbol():
    """The in-region loader writes members of the declaration and nothing
    else — otherwise it becomes the second hand-kept list."""
    from scripts.backfill_benchmark_proxies import _resolve_symbols

    assert _resolve_symbols(None) == sorted(UNIVERSE_BENCHMARK_PROXIES)
    assert _resolve_symbols("IWM,XLE") == ["IWM", "XLE"]
    with pytest.raises(SystemExit):
        _resolve_symbols("NVDA")


def test_data_spot_dispatcher_exposes_the_loader_workload():
    """The load is an invoke against an EXISTING in-region runner, not a
    hand-typed ssm send-command (the alpha-engine-config-I1906 class)."""
    src = (
        _REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"benchmark-proxy-backfill"' in src
    assert "python -m scripts.backfill_benchmark_proxies" in src


# ── C. Mutation: a declared proxy missing from the library fails LOUD ───────


def _stub_universe_lib(symbols: list[str], last_date: str):
    """A universe library holding exactly ``symbols``, each ending on
    ``last_date``."""
    lib = MagicMock()
    lib.list_symbols.return_value = list(symbols)

    def _tail(sym, n=1):
        res = MagicMock()
        res.data = pd.DataFrame(
            {"Close": [1.0]}, index=pd.DatetimeIndex([pd.Timestamp(last_date)]),
        )
        return res

    lib.tail.side_effect = _tail
    return lib


def _run_scan(symbols, last_date, expected_tickers=None):
    from builders import daily_append as da

    return da._scan_universe_and_emit_freshness_receipt(
        MagicMock(),
        "alpha-engine-research",
        _stub_universe_lib(symbols, last_date),
        max_stale_trading_days=99,  # staleness is NOT what this test grades
        expected_tickers=expected_tickers,
    )


def _today_iso():
    return pd.Timestamp.utcnow().normalize().date().isoformat()


@pytest.mark.parametrize("dropped", sorted(UNIVERSE_BENCHMARK_PROXIES))
def test_freshness_scan_goes_red_when_a_declared_proxy_is_absent(dropped):
    """THE mutation case, and the exact I10704 production state.

    Before this check the scan graded only the symbols the library already
    held: an absent symbol never entered ``syms``, was never scanned, and the
    receipt read ``all_fresh: True`` over the survivors. Five proxies were
    missing for months and this surface stayed green the whole time.
    """
    from builders.daily_append import UniverseFreshnessViolation

    held = sorted(UNIVERSE_BENCHMARK_PROXIES - {dropped}) + ["AAPL"]
    with pytest.raises(UniverseFreshnessViolation) as exc:
        _run_scan(held, _today_iso())
    assert dropped in str(exc.value)
    assert "DECLARED benchmark proxies" in str(exc.value)


def test_present_proxy_outside_the_scan_scope_is_not_graded_absent():
    """Pins the DELIBERATE scope of the check: absence from the LIBRARY.

    A proxy the caller's ``expected_tickers`` intersection filters out of
    ``syms`` is still present and still loadable by every consumer, so it is
    not a coverage failure. Grading scan membership instead would couple this
    producer invariant to each caller's request list — and would make the
    check fire on states the panel compile is perfectly happy with.
    """
    held = sorted(UNIVERSE_BENCHMARK_PROXIES) + ["AAPL"]
    receipt = _run_scan(
        held, _today_iso(),
        expected_tickers=["AAPL", "SPY", "IWM", "XLK", "XLV", "XLF"],  # omits XLE
    )
    assert receipt["declared_proxies_missing"] == []
    assert receipt["declared_proxies_present"] == len(UNIVERSE_BENCHMARK_PROXIES)
    # XLE was present but out of scan scope, so it is not among the scanned.
    assert receipt["declared_proxies_scanned"] == len(UNIVERSE_BENCHMARK_PROXIES) - 1


def test_freshness_scan_passes_and_reports_coverage_when_all_proxies_present():
    """Measurability: the counts are emitted on EVERY run, zeros included, so
    'nothing was checked' can never render the same as 'all covered'."""
    held = sorted(UNIVERSE_BENCHMARK_PROXIES) + ["AAPL"]
    receipt = _run_scan(held, _today_iso())
    assert receipt["all_fresh"] is True
    assert receipt["declared_proxies_total"] == len(UNIVERSE_BENCHMARK_PROXIES)
    assert receipt["declared_proxies_present"] == len(UNIVERSE_BENCHMARK_PROXIES)
    assert receipt["declared_proxies_scanned"] == len(UNIVERSE_BENCHMARK_PROXIES)
    assert receipt["declared_proxies_missing"] == []

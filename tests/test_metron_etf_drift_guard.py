"""Cross-package drift guard: producer ``RISK_FACTOR_ETFS`` vs metron's risk/sector
constants (metron-ops#85, a #43 follow-up).

The analytics reference ETFs are hand-maintained in two places that MUST stay in sync:

  - producer (here): ``collectors.metron_market_data.RISK_FACTOR_ETFS`` — the ETFs whose
    close-history the spine publishes (they're not in any held universe, so unless their
    history is published Metron has no close_history for them);
  - consumer (metron): the factor model's ``MARKET_ETF`` + ``STYLE_ETF`` (``api.services.risk``)
    and the Brinson sector map ``SECTOR_ETF`` (``portfolio_analytics.sectors``).

If metron adds/changes a factor or sector ETF and the producer list doesn't follow, the
spine silently stops emitting that ETF's close_history → it silently drops from
Risk/Attribution (the exact metron-ops#43 failure class, re-introduced quietly). This
lifts that invariant to a CI chokepoint.

Cross-package: metron + nousergon-data are co-installed on the box
(``pip install -e ../metron -e .`` per DEPLOY.md), so this can import metron's constants
directly. In the data-repo CI metron is NOT installed, so the import is guarded and the
test skips gracefully there (same discipline as the yfinance/edgar ``importorskip`` tests);
on the box (and any CI that co-installs metron) it executes the real comparison.

NOTE on the skip key: the metron distribution is named ``metron`` but exposes no top-level
``metron`` module — its importable packages are ``api`` and ``portfolio_analytics`` (see
metron pyproject ``[tool.hatch.build.targets.wheel]``). So we ``importorskip`` the actual
modules that own the constants, not the dist name.
"""

from __future__ import annotations

import pytest

from collectors import metron_market_data as mmd


def test_risk_factor_etfs_match_metron_risk_and_sector_constants():
    """``set(RISK_FACTOR_ETFS)`` must equal metron's {MARKET_ETF} ∪ STYLE_ETF ∪ SECTOR_ETF."""
    risk = pytest.importorskip(
        "api.services.risk",
        reason="metron not co-installed (data-repo CI); drift guard runs on the box / when metron is installed",
    )
    sectors = pytest.importorskip(
        "portfolio_analytics.sectors",
        reason="metron not co-installed (data-repo CI); drift guard runs on the box / when metron is installed",
    )

    metron_etfs = {risk.MARKET_ETF, *risk.STYLE_ETF.values(), *sectors.SECTOR_ETF.values()}
    producer_etfs = set(mmd.RISK_FACTOR_ETFS)

    missing_from_producer = metron_etfs - producer_etfs  # metron needs these but spine won't publish
    extra_in_producer = producer_etfs - metron_etfs       # spine publishes ETFs metron no longer uses

    assert producer_etfs == metron_etfs, (
        "RISK_FACTOR_ETFS drifted from metron's risk/sector constants — the spine and "
        "Metron's Risk/Attribution will disagree (metron-ops#43/#85). "
        f"Add to RISK_FACTOR_ETFS: {sorted(missing_from_producer)}; "
        f"remove from RISK_FACTOR_ETFS: {sorted(extra_in_producer)}."
    )


def test_risk_factor_etfs_has_no_duplicates():
    """The producer list is also a set in practice — a dup would mask a real drift in the
    set comparison (and pointlessly re-request the same close-history)."""
    assert len(mmd.RISK_FACTOR_ETFS) == len(set(mmd.RISK_FACTOR_ETFS)), (
        f"duplicate symbol(s) in RISK_FACTOR_ETFS: {mmd.RISK_FACTOR_ETFS}"
    )

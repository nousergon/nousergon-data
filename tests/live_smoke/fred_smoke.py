"""Live-FRED smoke — catches FRED REST API payload-shape regressions
that mocked unit tests miss by design.

The mocked FRED test suite stubs ``requests.get`` so tests run offline.
The cost is that no test ever exercises the real FRED API contract —
field renames, "." sentinel handling, observation envelope drift, and
API version changes are invisible to CI until production fires.

This script dispatches a real ``fetch_fred_history`` call for a short
window (~1y of DGS2, ~250 daily observations, free tier) and asserts
the response parses to a non-empty DataFrame with the expected index/
column shape. Designed to run:

  * In CI on PRs touching ``collectors/fred_history.py`` /
    ``collectors/daily_closes.py`` (which shares the FRED-fetch logic) —
    gated on the ``FRED_API_KEY`` secret. Forks without the secret get
    a clean skip, not a CI failure.
  * Locally via ``python tests/live_smoke/fred_smoke.py``.

Stays out of pytest's default collection because the file lives under
``tests/live_smoke/`` and the filename doesn't match ``test_*.py``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make repo importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Force env-only secret resolution (see polygon_smoke.py for rationale).
os.environ.setdefault("ALPHA_ENGINE_SECRETS_SOURCE", "env")

from collectors.fred_history import fetch_fred_history  # noqa: E402

SMOKE_SERIES = "DGS2"  # 2-year Treasury — daily, stable since 1976


def main() -> int:
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print(
            "fred_smoke: FRED_API_KEY not set; skipping. "
            "(Expected on fork PRs without the secret; not a failure.)",
            file=sys.stderr,
        )
        return 0

    print(
        f"fred_smoke: dispatching fetch_fred_history({SMOKE_SERIES!r}, "
        f"period_years=1) ...",
        file=sys.stderr,
    )

    try:
        df = fetch_fred_history(SMOKE_SERIES, period_years=1, api_key=api_key)
    except Exception as exc:  # noqa: BLE001 - smoke surfaces everything
        print(
            f"fred_smoke: FAILED — {type(exc).__name__}: {exc}\n"
            f"  This is exactly the regression class the smoke is meant to "
            f"catch (mocked tests would have passed). DO NOT MERGE.",
            file=sys.stderr,
        )
        return 1

    if df is None or df.empty:
        print(
            f"fred_smoke: FAILED — fetch returned empty for {SMOKE_SERIES} "
            f"over a 1-year window (FRED API likely changed shape)",
            file=sys.stderr,
        )
        return 1

    if "value" not in df.columns:
        print(
            f"fred_smoke: FAILED — DataFrame missing 'value' column; "
            f"got {list(df.columns)}",
            file=sys.stderr,
        )
        return 1

    # 1-year window should yield ~200-260 business-day observations.
    # Allow a wide lower bound to absorb minor backfill/holiday variance;
    # the assertion catches "got 1 row" (which would imply per-obs parse
    # failure) without being brittle on exact count.
    if len(df) < 50:
        print(
            f"fred_smoke: FAILED — only {len(df)} observations in 1y window; "
            f"expected ~250 — parser likely dropped most rows",
            file=sys.stderr,
        )
        return 1

    print(
        f"fred_smoke: OK — {SMOKE_SERIES} returned {len(df)} observations "
        f"from {df.index.min().date()} to {df.index.max().date()}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Live-Polygon smoke — catches Polygon REST API payload-shape regressions
that mocked unit tests miss by design.

The mocked Polygon test suite stubs ``polygon_client`` and/or
``requests.get`` so tests run offline. The cost is that no test ever
exercises the real Polygon API contract — payload shape drift (field
renames, schema deprecations, status-code semantics) is invisible to
CI until production fires.

This script dispatches a real ``get_grouped_daily`` call for the most
recently completed US trading day (~1 grouped-daily call, ~$0.01) and
asserts the response shape matches what the consumer code expects.
Designed to run:

  * In CI on PRs touching ``polygon_client.py`` / ``collectors/daily_closes.py``
    / ``collectors/nasdaq_snapshot.py`` — gated on the ``POLYGON_API_KEY``
    secret. Forks without the secret get a clean skip, not a CI failure.
  * Locally via ``python tests/live_smoke/polygon_smoke.py``.

Stays out of pytest's default collection because the file lives under
``tests/live_smoke/`` and the filename doesn't match ``test_*.py``.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Make repo importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Force env-only secret resolution — CI has no SSM access for the data
# repo's GHA OIDC role (deploy role is scoped to Lambda). The lib's
# default "auto" mode would log a noisy SSM AccessDenied; "env" path
# returns the GHA-secret-set value directly.
os.environ.setdefault("ALPHA_ENGINE_SECRETS_SOURCE", "env")

from polygon_client import polygon_client  # noqa: E402


def _most_recent_us_weekday() -> str:
    """Return YYYY-MM-DD for the most recent US weekday (Mon-Fri).

    Polygon grouped-daily T-1 settles ~09:00 UTC each day. For weekday
    smokes we want yesterday; on Mon we want Friday. Saturday/Sunday
    treated as the prior Friday. Does NOT consult the NYSE holiday
    calendar — at most we get a non-trading day with an empty result,
    which is itself useful (proves the API returned cleanly).
    """
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.isoformat()


def main() -> int:
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        print(
            "polygon_smoke: POLYGON_API_KEY not set; skipping. "
            "(Expected on fork PRs without the secret; not a failure.)",
            file=sys.stderr,
        )
        return 0

    target_date = _most_recent_us_weekday()
    print(
        f"polygon_smoke: dispatching get_grouped_daily({target_date!r}) ...",
        file=sys.stderr,
    )

    try:
        client = polygon_client(api_key=api_key)
        bars = client.get_grouped_daily(target_date)
    except Exception as exc:  # noqa: BLE001 - smoke surfaces everything
        print(
            f"polygon_smoke: FAILED — {type(exc).__name__}: {exc}\n"
            f"  This is exactly the regression class the smoke is meant to "
            f"catch (mocked tests would have passed). DO NOT MERGE.",
            file=sys.stderr,
        )
        return 1

    # Schema assertion — every value must carry the keys the consumer
    # (collectors/daily_closes._collect_window, builders/backfill) reads.
    expected_keys = {"open", "high", "low", "close", "volume", "vwap"}
    if not isinstance(bars, dict):
        print(
            f"polygon_smoke: FAILED — get_grouped_daily returned "
            f"{type(bars).__name__}, expected dict",
            file=sys.stderr,
        )
        return 1

    if not bars:
        # Empty is acceptable (non-trading day); the API call itself
        # succeeded, which is what we're validating. Don't fail.
        print(
            f"polygon_smoke: OK — {target_date} returned 0 tickers "
            f"(likely a US holiday or pre-settle window)",
            file=sys.stderr,
        )
        return 0

    sample_ticker = next(iter(bars))
    sample_bar = bars[sample_ticker]
    missing = expected_keys - set(sample_bar.keys())
    if missing:
        print(
            f"polygon_smoke: FAILED — sample bar for {sample_ticker} missing "
            f"keys {sorted(missing)}; got {sorted(sample_bar.keys())}",
            file=sys.stderr,
        )
        return 1

    print(
        f"polygon_smoke: OK — {target_date} returned {len(bars)} tickers; "
        f"sample {sample_ticker} carries {sorted(sample_bar.keys())}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

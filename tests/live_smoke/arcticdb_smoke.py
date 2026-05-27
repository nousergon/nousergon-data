"""Live-ArcticDB smoke — catches ArcticDB schema-drift regressions that
mocked unit tests miss by design.

The mocked ArcticDB test suite stubs ``arcticdb.Arctic`` and library
constructors so tests run offline. The cost is that no test ever
exercises the real ArcticDB read contract — schema column drift,
DatetimeIndex semantics, and binary protocol shape are invisible to
CI until ``builders/daily_append`` hits a ``StreamDescriptorMismatch``
in production (observed 2026-05-14 EOD, 2026-05-21 EOD).

This smoke connects to the live universe library, reads the tail of a
known-stable symbol (SPY), and asserts the schema matches the canonical
``OHLCV_COLS + [PROVENANCE_COL]`` contract that producer code emits
and consumer code reads. Read-only; no writes. Designed to run:

  * In CI on PRs touching ``store/arctic_store.py`` / ``builders/daily_append.py``
    / ``builders/backfill.py`` / ``builders/_price_cache_writeboth.py`` —
    gated on AWS OIDC role assumption. Forks without AWS credentials
    get a clean skip, not a CI failure.
  * Locally via ``python tests/live_smoke/arcticdb_smoke.py`` with
    AWS credentials sourced from the operator's environment.

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

from store.arctic_store import (  # noqa: E402
    OHLCV_COLS,
    PROVENANCE_COL,
    get_universe_lib,
)

SMOKE_SYMBOL = "SPY"  # Stable, present since universe-library inception.


def main() -> int:
    # Smoke needs AWS creds to reach the S3-backed ArcticDB. In CI these
    # come from the OIDC role; locally they come from the operator's
    # configured profile. Either way, refuse silently if the credential
    # chain is empty.
    if not any(
        os.environ.get(key)
        for key in ("AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ROLE_ARN")
    ):
        print(
            "arcticdb_smoke: no AWS credentials in env; skipping. "
            "(Expected on fork PRs without OIDC; not a failure.)",
            file=sys.stderr,
        )
        return 0

    print(
        f"arcticdb_smoke: reading tail of {SMOKE_SYMBOL!r} from universe lib ...",
        file=sys.stderr,
    )

    try:
        universe = get_universe_lib()
        # row_range slices the last row only — minimal payload, validates
        # the read path + schema without pulling the full ~2500-row series.
        result = universe.read(SMOKE_SYMBOL, row_range=(-1, None))
    except Exception as exc:  # noqa: BLE001 - smoke surfaces everything
        print(
            f"arcticdb_smoke: FAILED — {type(exc).__name__}: {exc}\n"
            f"  This is exactly the regression class the smoke is meant to "
            f"catch (mocked tests would have passed). DO NOT MERGE.",
            file=sys.stderr,
        )
        return 1

    df = result.data
    if df is None or df.empty:
        print(
            f"arcticdb_smoke: FAILED — universe.read({SMOKE_SYMBOL!r}) "
            f"returned empty",
            file=sys.stderr,
        )
        return 1

    # The canonical universe-library schema is OHLCV_COLS + PROVENANCE_COL
    # + features. The smoke asserts the OHLCV+provenance core is present;
    # features extend it and may evolve, so we don't pin their full set.
    required = set(OHLCV_COLS) | {PROVENANCE_COL}
    missing = required - set(df.columns)
    if missing:
        print(
            f"arcticdb_smoke: FAILED — {SMOKE_SYMBOL} missing required "
            f"columns {sorted(missing)}; got {sorted(df.columns)}",
            file=sys.stderr,
        )
        return 1

    last_date = df.index[-1]
    print(
        f"arcticdb_smoke: OK — {SMOKE_SYMBOL} last row dated {last_date}; "
        f"{len(df.columns)} columns include {sorted(required)}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
tests/fixtures/dummy_private_pack.py — throwaway fixture private feature pack.

Used ONLY by tests/test_private_feature_pack.py to prove the
features/private_pack.py loading mechanism + schema-contract CI
accommodation work end-to-end (alpha-engine-config#1032).

DELIBERATELY TRIVIAL AND FAKE. This is NOT an example of real alpha-bearing
compute — it exists purely to exercise the contract (module discovered by
path, ``add_private_features`` + ``PRIVATE_FEATURE_NAMES`` present, column
actually appended). A real private pack lives OUTSIDE this public repo
entirely; see features/private_pack.py's module docstring.
"""

from __future__ import annotations

import pandas as pd

# The one column this fixture pack claims to add. Deliberately named
# "test_private_dummy_feature" (not anything resembling a real signal name)
# with the `_raw` units suffix so it would also pass the naming-convention
# check if it were ever registered in the real CATALOG (it isn't — this
# fixture is never wired into features/registry.py::CATALOG).
PRIVATE_FEATURE_NAMES: list[str] = ["test_private_dummy_feature_raw"]


def add_private_features(df: pd.DataFrame) -> pd.DataFrame:
    """Append one trivially-fake column: Close rounded to the nearest int.

    Not a trading signal of any kind — just enough arithmetic to prove
    the pack's compute actually ran and produced a real column.
    """
    out = df.copy()
    if "Close" in out.columns:
        out["test_private_dummy_feature_raw"] = out["Close"].round()
    else:
        out["test_private_dummy_feature_raw"] = 0.0
    return out

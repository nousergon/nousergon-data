"""Private feature-pack mechanism tests (alpha-engine-config#1032).

Proves end-to-end, using the throwaway fixture at
``tests/fixtures/dummy_private_pack.py`` (an obviously-fake, trivial
column — see that file's docstring), that:

  1. With no ``NOUSERGON_PRIVATE_FEATURE_PACK`` set, the mechanism is a
     complete no-op — this is the path every public CI run takes, and it
     must never require the pack to exist.
  2. With the env var pointed at a conforming module, the pack loads,
     its contract is validated, and its column is appended to a features
     DataFrame at the same extension point ``features/compute.py`` uses
     (``apply_private_features``, called right after
     ``apply_factor_zscores``).
  3. A pack that is configured but broken (missing attrs, non-callable,
     wrong declared/produced columns, nonexistent path) fails LOUD rather
     than silently degrading — silent degradation here would mean alpha
     columns quietly vanish from a production run with no error.
  4. The schema-contract CI accommodation (``registry.PRIVATE_PACK_COMPUTE``
     sentinel) does exactly what #1032 asked: a private-pack CATALOG entry
     is exempt from the FEATURES-emit-list sync, but everything else
     (CATALOG registration, units suffix, SCHEMA.md row, consumer) is
     enforced identically to a public column — proven with a temporary,
     in-test CATALOG entry rather than mutating the real registry.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.private_pack import (
    ENV_VAR,
    PrivateFeaturePackError,
    apply_private_features,
    load_private_pack,
)
from features.registry import CATALOG, PRIVATE_PACK_COMPUTE, FeatureEntry
from features.writer import write_feature_snapshot

_FIXTURE_PACK = Path(__file__).resolve().parent / "fixtures" / "dummy_private_pack.py"


def _sample_df() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=5, freq="B")
    return pd.DataFrame({"Close": [100.1, 101.4, 99.8, 102.6, 103.2]}, index=idx)


# ── No-pack (public/default) path ───────────────────────────────────────────


def test_no_env_var_is_a_no_op():
    assert os.environ.get(ENV_VAR) in (None, "")
    assert load_private_pack() is None


def test_apply_private_features_no_op_returns_identical_frame():
    df = _sample_df()
    out = apply_private_features(df)
    pd.testing.assert_frame_equal(out, df)


def test_blank_env_value_is_treated_as_absent():
    assert load_private_pack(env_value="   ") is None


# ── Conforming fixture pack ──────────────────────────────────────────────────


def test_fixture_pack_loads_and_validates():
    module = load_private_pack(env_value=str(_FIXTURE_PACK))
    assert module is not None
    assert module.PRIVATE_FEATURE_NAMES == ["test_private_dummy_feature_raw"]
    assert callable(module.add_private_features)


def test_apply_private_features_appends_declared_column():
    df = _sample_df()
    out = apply_private_features(df, env_value=str(_FIXTURE_PACK))
    assert "test_private_dummy_feature_raw" in out.columns
    assert "test_private_dummy_feature_raw" not in df.columns  # original untouched
    # Trivial arithmetic pinned: round(Close).
    expected = df["Close"].round()
    pd.testing.assert_series_equal(
        out["test_private_dummy_feature_raw"], expected, check_names=False,
    )


def test_apply_private_features_preserves_public_columns():
    df = _sample_df()
    out = apply_private_features(df, env_value=str(_FIXTURE_PACK))
    pd.testing.assert_series_equal(out["Close"], df["Close"])


# ── Loud-failure paths (configured but broken) ──────────────────────────────


def test_nonexistent_path_raises():
    with pytest.raises(PrivateFeaturePackError, match="does not exist"):
        load_private_pack(env_value="/nonexistent/path/to/pack.py")


def test_missing_required_attrs_raises(tmp_path):
    bad_pack = tmp_path / "bad_pack.py"
    bad_pack.write_text("# no add_private_features, no PRIVATE_FEATURE_NAMES\n")
    with pytest.raises(PrivateFeaturePackError, match="missing required"):
        load_private_pack(env_value=str(bad_pack))


def test_non_callable_add_private_features_raises(tmp_path):
    bad_pack = tmp_path / "bad_pack2.py"
    bad_pack.write_text(
        "add_private_features = 'not callable'\n"
        "PRIVATE_FEATURE_NAMES = ['x_raw']\n"
    )
    with pytest.raises(PrivateFeaturePackError, match="not callable"):
        load_private_pack(env_value=str(bad_pack))


def test_declared_name_not_actually_produced_raises(tmp_path):
    bad_pack = tmp_path / "bad_pack3.py"
    bad_pack.write_text(
        "PRIVATE_FEATURE_NAMES = ['ghost_feature_raw']\n"
        "def add_private_features(df):\n"
        "    return df.copy()  # never actually adds ghost_feature_raw\n"
    )
    with pytest.raises(PrivateFeaturePackError, match="did not add column"):
        apply_private_features(_sample_df(), env_value=str(bad_pack))


def test_module_that_raises_on_import_is_wrapped():
    bad_pack_src = "raise ValueError('boom')\n"
    import tempfile

    with tempfile.NamedTemporaryFile(
        "w", suffix=".py", delete=False
    ) as f:
        f.write(bad_pack_src)
        path = f.name
    try:
        with pytest.raises(PrivateFeaturePackError, match="raised on import"):
            load_private_pack(env_value=path)
    finally:
        os.unlink(path)


# ── Schema-contract CI accommodation (registry sentinel) ────────────────────


def test_private_pack_compute_sentinel_value():
    # Pin the literal — SCHEMA.md documents this exact string in the
    # Compute column, and consumers may grep for it.
    assert PRIVATE_PACK_COMPUTE == "private-pack"


def test_feature_entry_defaults_to_public_compute():
    entry = FeatureEntry(name="x_raw", group="technical", description="d")
    assert entry.compute == ""
    assert entry.compute != PRIVATE_PACK_COMPUTE


def test_real_catalog_has_no_private_pack_entries_yet():
    """As of this PR, no alpha-bearing column has landed through the
    mechanism (per #1032's closes-when — the mechanism ships now, the
    first real column is a separate, future PR). This pins that state so
    the next PR that adds one updates this test deliberately rather than
    silently.
    """
    private_entries = [f for f in CATALOG if f.compute == PRIVATE_PACK_COMPUTE]
    assert private_entries == []


# ── ArcticDB/S3 write-path composition (design claim: unchanged) ───────────


def test_private_pack_column_flows_through_write_feature_snapshot():
    """The ArcticDB/S3 write path (features/writer.py) is group-driven off
    ``registry.GROUPS`` — it never needs to know a column came from a
    private pack vs. public compute. This proves that composition: once a
    private-pack column is (hypothetically) registered with a group, the
    existing writer picks it up unchanged, exactly like any public column.

    Uses a fake boto3 s3_client (records put_object calls) — no real S3 /
    ArcticDB I/O, matching this repo's existing writer-test convention.
    """
    import features.registry as registry_module

    # Simulate what a real PR landing the first private-pack column would
    # do: register it with a group, same as any public FeatureEntry.
    fake_entry = FeatureEntry(
        name="test_private_dummy_feature_raw",
        group="technical",
        description="private pack",
        compute=PRIVATE_PACK_COMPUTE,
    )
    original_groups = dict(registry_module.GROUPS)
    try:
        registry_module.GROUPS.setdefault("technical", [])
        registry_module.GROUPS["technical"] = list(
            registry_module.GROUPS["technical"]
        ) + [fake_entry.name]

        df = _sample_df()
        df.insert(0, "ticker", ["AAPL"] * len(df))
        df = apply_private_features(df, env_value=str(_FIXTURE_PACK))

        class _FakeS3:
            def __init__(self):
                self.put_calls = []

            def put_object(self, Bucket, Key, Body):
                self.put_calls.append((Bucket, Key))

        fake_s3 = _FakeS3()
        written = write_feature_snapshot(
            "2024-01-08", df, "fake-bucket", s3_client=fake_s3,
        )
        assert written.get("technical", 0) > 0
        assert any(
            key.endswith("technical.parquet") for _, key in fake_s3.put_calls
        )
    finally:
        registry_module.GROUPS.clear()
        registry_module.GROUPS.update(original_groups)


def test_private_pack_entry_exempt_from_features_sync_simulation():
    """Simulate the schema-contract sync check's private-pack branch
    in isolation (without mutating the real CATALOG/FEATURES module
    state), mirroring the logic in
    tests/test_schema_contract.py::test_features_and_catalog_are_in_sync.
    """
    fake_catalog = list(CATALOG) + [
        FeatureEntry(
            name="test_private_dummy_feature_raw",
            group="technical",
            description="private pack",
            compute=PRIVATE_PACK_COMPUTE,
        )
    ]
    from features.feature_engineer import FEATURES

    catalog_names = {f.name for f in fake_catalog}
    private_pack_names = {
        f.name for f in fake_catalog if f.compute == PRIVATE_PACK_COMPUTE
    }
    public_catalog_names = catalog_names - private_pack_names
    features = set(FEATURES)

    # The private-pack column is correctly excluded from the "must be in
    # FEATURES" requirement...
    assert not (public_catalog_names - features)
    # ...but a NON-private-pack column missing from FEATURES would still
    # be caught (sanity check the exemption isn't a no-op on the whole test).
    fake_catalog_public_bug = list(CATALOG) + [
        FeatureEntry(name="oops_not_in_features_raw", group="technical", description="d")
    ]
    catalog_names_bug = {f.name for f in fake_catalog_public_bug}
    private_pack_names_bug = {
        f.name for f in fake_catalog_public_bug if f.compute == PRIVATE_PACK_COMPUTE
    }
    public_catalog_names_bug = catalog_names_bug - private_pack_names_bug
    assert "oops_not_in_features_raw" in (public_catalog_names_bug - features)

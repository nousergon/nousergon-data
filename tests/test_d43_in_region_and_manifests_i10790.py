"""D43's manual repair builders run through the run-manifest wrapper and are
refused off the in-region box; D35 already does both (`alpha-engine-config-
I10790`).

D43's four entry points (`registry.d/units/D43-manual-repair-builders.yaml`
``code_path``) are ``builders/splice_rebase.py``, ``builders/
repair_macro_series.py``, ``builders/purge_phantom_day.py`` and
``collectors/daily_closes_fred_repair.py``. ``corporate_actions/`` carries no
CLI entry point of its own (no ``main``/``ArgumentParser``/``__main__`` in any
of its modules) — it is a library the four CLIs above (and other, non-D43
tools) import, so it needs no independent wrapping or guard.

What this file grades, for every D43 entry point:

  * off the in-region box (``NE_DATA_INSTANCE_TYPE`` unset), the tool refuses
    with :class:`run_units.NotInRegionError` BEFORE touching ArcticDB, the
    corporate-actions registry, or S3 — not merely before the write;
  * on the box, a dry-run/test-store invocation writes exactly one
    ``data_run_manifest.v1`` record;
  * an induced failure still writes a ``failed`` manifest and re-raises.

D35 (``scripts/backfill_benchmark_proxies.py``) already calls
``run_manifest.run_unit`` directly and is not in-region-refused by design (it
is dispatched in-region via the ``alpha-engine-data-spot-dispatcher`` Lambda,
so a laptop invocation is a recorded, not a forbidden, path) — this file adds
the missing manifest-record coverage for it.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

import run_units

REPO_ROOT = Path(__file__).resolve().parents[1]

#: A real-shaped sha so `resolve_code_sha` never shells out to git in a test.
FAKE_SHA = "a" * 40


class FakeSink:
    """Captures what a real ``S3ManifestSink`` would have PUT."""

    bucket = "test-bucket"

    def __init__(self) -> None:
        self.writes: list[tuple[str, dict]] = []

    def write(self, key: str, payload: bytes):
        self.writes.append((key, json.loads(payload.decode("utf-8"))))
        return None

    @property
    def only(self) -> dict:
        assert len(self.writes) == 1, (
            f"expected exactly ONE manifest per execution, got {len(self.writes)}: "
            f"{[k for k, _ in self.writes]}"
        )
        return self.writes[0][1]


@pytest.fixture
def in_region(monkeypatch) -> FakeSink:
    """A box that declared itself in-region, with the sink swapped for a fake.

    Swaps the SINK, not the wrapper, so tests stay on the real code path
    (mirrors ``tests/test_unit_manifests.py``).
    """
    fake = FakeSink()
    monkeypatch.setenv("NE_DATA_INSTANCE_TYPE", "c5.large")
    monkeypatch.setenv("NE_DATA_CODE_SHA", FAKE_SHA)
    monkeypatch.delenv(run_units.TRIGGER_ENV, raising=False)
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: fake)
    return fake


@pytest.fixture
def off_region(monkeypatch) -> None:
    """No box declaration at all — the laptop-or-CI default."""
    monkeypatch.delenv("NE_DATA_INSTANCE_TYPE", raising=False)


# ─────────────────────────── require_in_region itself ──────────────────────


def test_require_in_region_raises_when_undeclared(off_region):
    with pytest.raises(run_units.NotInRegionError, match="my_tool"):
        run_units.require_in_region("my_tool")


def test_require_in_region_passes_when_declared(monkeypatch):
    monkeypatch.setenv("NE_DATA_INSTANCE_TYPE", "c5.large")
    run_units.require_in_region("my_tool")  # must not raise


# ─────────────────────── corporate_actions/ has no CLI ──────────────────────


def test_corporate_actions_has_no_standalone_entry_point():
    """The D43 descriptor's ``corporate_actions/`` code_path segment is a
    library import, not a fifth CLI — confirmed here so this file's coverage
    claim ("every D43 entry point") does not silently go stale if one is
    added later without a matching guard."""
    import ast

    pkg_dir = REPO_ROOT / "corporate_actions"
    for path in pkg_dir.glob("*.py"):
        tree = ast.parse(path.read_text())
        top_level_funcs = {
            node.name for node in tree.body if isinstance(node, ast.FunctionDef)
        }
        assert "main" not in top_level_funcs, (
            f"{path} declares a top-level main() — corporate_actions/ was "
            "assumed to have no CLI entry point; this test and the D43 guard "
            "wiring both need updating."
        )


# ────────────────────────────── splice_rebase ───────────────────────────────


SPLICE_ARGV = [
    "splice_rebase",
    "--ticker", "MLI",
    "--splice-date", "2026-06-12",
    "--true-ex-date", "2026-07-01",
    "--split-from", "1",
    "--split-to", "2",
]
# `manual_run(write=...)` passes `sink=None` under `--dry-run` (docstring: "no
# --apply, nothing is written, including the manifest") — so the manifest-
# producing tests below run in --apply mode with the underlying repair
# function MOCKED (a test-store invocation: nothing real is touched, but the
# wrapper's write path runs for real).
SPLICE_APPLY_ARGV = SPLICE_ARGV + ["--apply"]


def test_splice_rebase_refuses_off_region_before_touching_arcticdb(off_region, monkeypatch):
    from builders import splice_rebase

    monkeypatch.setattr(sys, "argv", SPLICE_ARGV)

    def _boom(*a, **kw):  # noqa: ARG001
        raise AssertionError("splice_rebase() must not run off the in-region box")

    monkeypatch.setattr(splice_rebase, "splice_rebase", _boom)

    with pytest.raises(run_units.NotInRegionError, match="splice_rebase"):
        splice_rebase.main()


def test_splice_rebase_writes_one_manifest_on_the_box(in_region, monkeypatch):
    from builders import splice_rebase

    monkeypatch.setattr(sys, "argv", SPLICE_APPLY_ARGV)
    monkeypatch.setattr(
        splice_rebase,
        "splice_rebase",
        lambda *a, **kw: {  # noqa: ARG005
            "ticker": "MLI", "status": "applied", "n_rows_changed": 40, "rows": 3200,
        },
    )

    splice_rebase.main()

    manifest = in_region.only
    assert manifest["unit_id"] == "D43"
    assert manifest["status"] == "ok"
    assert manifest["schema_version"] == "data_run_manifest.v1"
    assert manifest["compute"]["instance_type"] == "c5.large"


def test_splice_rebase_failure_writes_failed_and_reraises(in_region, monkeypatch):
    """The canary-not-cleared path: `_SpliceRebaseFailed` is raised inside the
    wrapper, so the manifest is durable before `main()` exits non-zero."""
    from builders import splice_rebase

    monkeypatch.setattr(sys, "argv", SPLICE_APPLY_ARGV)
    monkeypatch.setattr(
        splice_rebase,
        "splice_rebase",
        lambda *a, **kw: {"ticker": "MLI", "status": "dry_run_canary_not_cleared"},  # noqa: ARG005
    )

    with pytest.raises(SystemExit):
        splice_rebase.main()

    manifest = in_region.only
    assert manifest["status"] == "failed"
    assert "dry_run_canary_not_cleared" in manifest["reason"]


# ────────────────────────────── purge_phantom_day ───────────────────────────


PURGE_ARGV = ["purge_phantom_day", "--date", "2026-06-19"]  # Juneteenth: NYSE closed
PURGE_APPLY_ARGV = PURGE_ARGV + ["--apply"]


def test_purge_phantom_day_refuses_off_region_before_touching_arcticdb(off_region, monkeypatch):
    from builders import purge_phantom_day

    monkeypatch.setattr(sys, "argv", PURGE_ARGV)
    monkeypatch.setattr(
        purge_phantom_day, "_purge",
        lambda *a, **kw: (_ for _ in ()).throw(  # noqa: ARG005
            AssertionError("_purge() must not run off the in-region box")
        ),
    )

    with pytest.raises(run_units.NotInRegionError, match="purge_phantom_day"):
        purge_phantom_day.main()


def test_purge_phantom_day_writes_one_manifest_on_the_box(in_region, monkeypatch):
    from builders import purge_phantom_day

    monkeypatch.setattr(sys, "argv", PURGE_APPLY_ARGV)
    monkeypatch.setattr(
        purge_phantom_day, "_purge",
        lambda args, ctx: (  # noqa: ARG005
            ctx.record_output("arcticdb/universe", rows_out=0, schema_version="arcticdb/universe"),
            ctx.record_output("arcticdb/macro", rows_out=0, schema_version="arcticdb/macro"),
        ),
    )

    purge_phantom_day.main()

    manifest = in_region.only
    assert manifest["unit_id"] == "D43"
    assert manifest["status"] == "ok"


def test_purge_phantom_day_failure_writes_failed_and_reraises(in_region, monkeypatch):
    from builders import purge_phantom_day

    monkeypatch.setattr(sys, "argv", PURGE_APPLY_ARGV)

    def _boom(args, ctx):  # noqa: ARG001
        raise RuntimeError("2 symbol(s) failed")

    monkeypatch.setattr(purge_phantom_day, "_purge", _boom)

    with pytest.raises(RuntimeError, match="2 symbol\\(s\\) failed"):
        purge_phantom_day.main()

    manifest = in_region.only
    assert manifest["status"] == "failed"
    assert "2 symbol(s) failed" in manifest["reason"]


# ─────────────────────────── repair_macro_series ────────────────────────────


MACRO_ARGV = ["repair_macro_series", "--symbols", "VIX3M", "--dry-run"]
MACRO_APPLY_ARGV = ["repair_macro_series", "--symbols", "VIX3M"]


def test_repair_macro_series_refuses_off_region_before_touching_arcticdb(off_region, monkeypatch):
    from builders import repair_macro_series

    monkeypatch.setattr(sys, "argv", MACRO_ARGV)
    monkeypatch.setattr(
        repair_macro_series, "repair_symbol",
        lambda *a, **kw: (_ for _ in ()).throw(  # noqa: ARG005
            AssertionError("repair_symbol() must not run off the in-region box")
        ),
    )

    with pytest.raises(run_units.NotInRegionError, match="repair_macro_series"):
        repair_macro_series.main()


def test_repair_macro_series_writes_one_manifest_on_the_box(in_region, monkeypatch):
    from builders import repair_macro_series

    monkeypatch.setattr(sys, "argv", MACRO_APPLY_ARGV)
    monkeypatch.setattr(
        repair_macro_series, "repair_symbol",
        lambda *a, **kw: {"status": "ok_dry_run", "rows_after": 2600},  # noqa: ARG005
    )

    assert repair_macro_series.main() == 0

    manifest = in_region.only
    assert manifest["unit_id"] == "D43"
    assert manifest["status"] == "ok"
    assert manifest["outputs"][0]["rows_out"] == 2600


def test_repair_macro_series_failure_writes_failed_and_reraises(in_region, monkeypatch):
    from builders import repair_macro_series

    monkeypatch.setattr(sys, "argv", MACRO_APPLY_ARGV)

    def _boom(*a, **kw):  # noqa: ARG001
        raise RuntimeError("upstream fetch exploded")

    monkeypatch.setattr(repair_macro_series, "repair_symbol", _boom)

    with pytest.raises(RuntimeError, match="upstream fetch exploded"):
        repair_macro_series.main()

    manifest = in_region.only
    assert manifest["status"] == "failed"


# ────────────────────────── daily_closes_fred_repair ────────────────────────


FRED_ARGV = [
    "daily_closes_fred_repair",
    "--start", "2026-06-01", "--end", "2026-06-05", "--dry-run",
]
FRED_APPLY_ARGV = ["daily_closes_fred_repair", "--start", "2026-06-01", "--end", "2026-06-05"]


def test_fred_repair_refuses_off_region_before_touching_s3(off_region, monkeypatch):
    from collectors import daily_closes_fred_repair

    monkeypatch.setattr(sys, "argv", FRED_ARGV)
    monkeypatch.setattr(
        daily_closes_fred_repair, "repair",
        lambda *a, **kw: (_ for _ in ()).throw(  # noqa: ARG005
            AssertionError("repair() must not run off the in-region box")
        ),
    )

    with pytest.raises(run_units.NotInRegionError, match="daily_closes_fred_repair"):
        daily_closes_fred_repair.main()


def test_fred_repair_writes_one_manifest_on_the_box(in_region, monkeypatch):
    from collectors import daily_closes_fred_repair

    monkeypatch.setattr(sys, "argv", FRED_APPLY_ARGV)
    monkeypatch.setattr(
        daily_closes_fred_repair, "repair",
        lambda *a, **kw: {"status": "ok", "rows_repaired": 12},  # noqa: ARG005
    )

    with pytest.raises(SystemExit) as exc:
        daily_closes_fred_repair.main()
    assert exc.value.code == 0

    manifest = in_region.only
    assert manifest["unit_id"] == "D43"
    assert manifest["status"] == "ok"


def test_fred_repair_failure_writes_failed_and_reraises(in_region, monkeypatch):
    from collectors import daily_closes_fred_repair

    monkeypatch.setattr(sys, "argv", FRED_APPLY_ARGV)
    monkeypatch.setattr(
        daily_closes_fred_repair, "repair",
        lambda *a, **kw: {"status": "error", "detail": "FRED refused"},  # noqa: ARG005
    )

    with pytest.raises(SystemExit) as exc:
        daily_closes_fred_repair.main()
    assert exc.value.code == 2

    manifest = in_region.only
    assert manifest["status"] == "failed"


# ────────────────────────────────── D35 ─────────────────────────────────────
#
# D35 is not in-region-refused by design (dispatched via the spot-dispatcher
# Lambda; a laptop-triggered dispatch of the in-region workload is a recorded,
# not forbidden, path — `scripts/backfill_benchmark_proxies.py`). This adds
# the manifest-record coverage the module otherwise lacked.


def _load_script(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def d35(monkeypatch):
    module = _load_script(REPO_ROOT / "scripts" / "backfill_benchmark_proxies.py", "_test_d35_backfill_benchmark_proxies")
    # No `--dry-run`: D35's `main()` only calls `run_units.manifest_sink(...)`
    # (patched to the FakeSink by `in_region`) when NOT a dry run — with
    # `load_proxy` fully mocked below, this is a test-store invocation, not a
    # real write.
    monkeypatch.setattr(sys, "argv", ["backfill_benchmark_proxies", "--symbols", "IWM"])
    return module


def test_d35_dry_run_writes_one_manifest(in_region, d35, monkeypatch):
    monkeypatch.setattr(
        d35, "load_proxy",
        lambda symbol, **kw: {"symbol": symbol, "status": "ok"},  # noqa: ARG005
    )
    # `_execute` readback-verifies against real ArcticDB when not a dry run —
    # mocked here too, so this stays a test-store invocation.
    monkeypatch.setattr(d35, "verify", lambda symbols, **kw: {"present": symbols, "missing": [], "spans": {}})  # noqa: ARG005

    assert d35.main() == 0

    manifest = in_region.only
    assert manifest["unit_id"] == "D35"
    assert manifest["schema_version"] == "data_run_manifest.v1"
    assert manifest["status"] == "ok"


def test_d35_failure_writes_failed_and_reraises(in_region, d35, monkeypatch):
    def _boom(symbol, **kw):  # noqa: ARG001
        raise RuntimeError(f"upstream fetch failed for {symbol}")

    monkeypatch.setattr(d35, "load_proxy", _boom)
    monkeypatch.setattr(d35, "verify", lambda symbols, **kw: {"present": [], "missing": symbols, "spans": {}})  # noqa: ARG005

    assert d35.main() == 1

    manifest = in_region.only
    assert manifest["status"] == "failed"
    assert "upstream fetch failed for IWM" in manifest["reason"]

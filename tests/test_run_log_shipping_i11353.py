"""A run manifest names a log that outlives the box, and a truncated `reason`
keeps the end.

`alpha-engine-config-I11353`. Diagnosing the 2026-09-21 shadow legs, the whole
recording surface for *why a leg failed* was: a manifest string cut before the
cause, and a log cut before the failure.

* `/alpha-engine/data-spot` stream `…/aws-runShellScript/stdout` held 1,025,054
  bytes / 4,848 lines and ended at 22:37:07Z — three minutes into a 73-minute
  run, inside the `morning_daily_closes` window scan. Everything after,
  including the target-date error at 22:38Z, is in no log anywhere.
* Every run manifest under `data_collection/runs/*/2026-09-21/*.json` recorded
  `log_location: "local:ip-172-31-33-124.ec2.internal:<pid>"` — a host
  terminated when the workload finished.
* The failing manifest's `reason` was cut at 1,800 of 4,428 characters, HEAD
  first — and `collectors/daily_closes.py::collect` renders the failing target
  date LAST, so the cut landed exactly on the cause.

Two halves, two contracts, both asserted here. The dispatcher's half (the key
layout, the export, the traps) is in
`infrastructure/lambdas/data-spot-dispatcher/test_handler.py`, which stubs
`nousergon_lib` and therefore cannot import `run_units`.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_units  # noqa: E402


# ── log_location ─────────────────────────────────────────────────────────────


def test_the_manifest_records_the_shipped_s3_object(monkeypatch):
    uri = (
        "s3://alpha-engine-research/data_collection/logs/"
        "shadow-sameday/2026-09-21/i-09e64cb257b556b82.log"
    )
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.setenv(run_units.RUN_LOG_S3_ENV, uri)
    assert run_units.resolve_log_location() == uri


def test_the_s3_uri_beats_the_capped_cloudwatch_group(monkeypatch):
    """A box that declares BOTH must record the object, not the stream: the
    stream is the thing that was measured losing 70 of 73 minutes."""
    uri = "s3://alpha-engine-research/data_collection/logs/w/2026-09-21/i-x.log"
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.setenv(run_units.RUN_LOG_S3_ENV, uri)
    monkeypatch.setenv("NE_DATA_INSTANCE_TYPE", "c5.large")
    assert run_units.resolve_log_location() == uri


def test_an_explicit_override_still_wins(monkeypatch):
    monkeypatch.setenv(run_units.LOG_LOCATION_ENV, "cloudwatch:/somewhere/else")
    monkeypatch.setenv(run_units.RUN_LOG_S3_ENV, "s3://bucket/key.log")
    assert run_units.resolve_log_location() == "cloudwatch:/somewhere/else"


def test_without_the_var_it_stays_local_and_does_not_invent_a_key(monkeypatch):
    """`local:` is the honest answer off a shipping box. The daily report
    counts it as a detection gap on a non-ok run — never as silence, and never
    as an S3 URI nothing wrote."""
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.delenv(run_units.RUN_LOG_S3_ENV, raising=False)
    monkeypatch.delenv("NE_DATA_INSTANCE_TYPE", raising=False)
    assert run_units.resolve_log_location().startswith("local:")


def test_the_env_var_name_matches_the_dispatcher_that_exports_it():
    """The launcher and the writer agree by NAME, not by convention: a rename
    on one side alone makes every manifest read `local:` while the log ships
    perfectly, which is the silent half of this defect."""
    source = (
        Path(__file__).resolve().parents[1]
        / "infrastructure/lambdas/data-spot-dispatcher/index.py"
    ).read_text(encoding="utf-8")
    assert f'RUN_LOG_ENV = "{run_units.RUN_LOG_S3_ENV}"' in source


# ── reason truncation keeps the tail ─────────────────────────────────────────


def _window_scan_reason(ok_dates: int = 120) -> str:
    """The 2026-09-21 shape: many `ok` dates, the failure LAST."""
    head = "morning_daily_closes: per-date window scan | "
    body = " ".join(
        f"2026-08-{d:02d}=ok(928 tickers, polygon_only)" for d in range(1, ok_dates + 1)
    )
    return f"{head}{body} 2026-09-21=ERROR polygon grouped-daily bar not final"


def test_truncation_keeps_the_failing_entry_at_the_end():
    reason = _window_scan_reason()
    assert len(reason) > run_units.REASON_MAX_LEN
    out = run_units.truncate_reason(reason)
    assert len(out) <= run_units.REASON_MAX_LEN
    assert out.endswith("2026-09-21=ERROR polygon grouped-daily bar not final")
    assert out.startswith("morning_daily_closes: per-date window scan")


def test_truncation_names_how_many_bytes_it_dropped():
    reason = _window_scan_reason()
    out = run_units.truncate_reason(reason)
    assert "reason_truncated: true" in out
    assert "reason_truncated_bytes: " in out
    dropped = int(out.split("reason_truncated_bytes: ")[1].split(",")[0])
    assert 0 < dropped < len(reason)
    # The count is the characters actually removed, not a round number: head
    # plus tail plus dropped IS the original.
    marker_len = len(out) - len(out.split("]… ")[-1]) - len(out.split(" …[")[0])
    assert len(out) - marker_len + dropped == len(reason)
    assert f"full length {len(reason)} chars" in out


def test_a_short_reason_is_returned_untouched():
    assert run_units.truncate_reason("boom") == "boom"


def test_describe_mode_failure_keeps_the_tail_too():
    """The wrapper every call site uses inherits the property, not just the
    helper — fixing one call site of a systemic defect is not a fix."""
    result = {
        "mode": "morning_enrich",
        "status": "failed",
        "collectors": {
            f"c{i}": {"status": "ok", "detail": "x" * 80} for i in range(400)
        },
    }
    result["collectors"]["zzz_last"] = {"status": "error", "error": "THE CAUSE"}
    out = run_units.describe_mode_failure("morning_enrich", result)
    assert len(out) <= run_units.REASON_MAX_LEN
    assert "reason_truncated_bytes: " in out
    # The head still names the failing sub-collector, AND the tail carries the
    # end of the rendering — the half a head-only cut used to throw away.
    assert "zzz_last" in out[:200]
    assert "THE CAUSE" in out[-400:]


def test_the_budget_stays_under_the_librarys_own_cut():
    """`nousergon_lib.run_manifest.run_unit` (>= v0.124.150) still cuts the
    final string at 2,000 chars, but no longer head-only or silently
    (`alpha-engine-config-I11358`): it keeps both ends behind an explicit
    marker, same as this module. Staying under it is no longer about
    preventing silent tail loss — it's about not paying for a second, nested
    truncation the library would otherwise perform on top of this layer's
    own, since `run_unit` wraps the reason in `f"{type(exc).__name__}: {exc}"`
    before its cut runs."""
    assert run_units.REASON_MAX_LEN + len("_CollectorError: ") < 2000

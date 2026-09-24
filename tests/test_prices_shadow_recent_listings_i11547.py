"""alpha-engine-config-I11547 — the shadow D03 refused FDXF, HONA, Q and SOLS.

Measured 2026-09-24 (read-only) on the same-day shadow runs for 2026-09-21,
-22 and -23: each shadow D03 manifest read ``status: failed``,
``rows_rejected: [{count: 4, reason: short_fetch_guard_refused}]``, while every
scheduled v1 D03 run the same days wrote all four ``ok`` (HONA 69 rows, FDXF
82, Q 227, SOLS 232). The shadow-sameday logs name the actual cause, four times
a night::

    Refresh failed for HONA: unclassified read of a key the run also writes:
    s3://alpha-engine-research/reference/price_cache/HONA.parquet was read LIVE
    as an input earlier in this shadow run and is now being published by it ...

The fetch window was never short. These are the only tickers under
``_SHORT_FETCH_ROW_THRESHOLD`` (400 rows), so they are the only ones for which
the short-fetch guard READS the existing parquet — and under the shadow
interceptor that read was recorded as a live input, so the upload of the same
key raised ``ShadowGuardViolation``. The per-ticker ``except Exception`` folded
the violation into an ordinary miss, and D03's ``rejected_keys`` labelled every
miss ``short_fetch_guard_refused``.

These tests drive the REAL ``_refresh_stale`` / ``collect`` through a real
boto3 client, with the REAL interceptor installed and an in-memory bucket under
it — the wiring a shadow run actually goes through.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import pathlib

import boto3
import botocore.client
import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import collectors.prices as prices
import run_units
import weekly_collector
from shadow import interceptor, parity
from shadow.root import ShadowGuardViolation, ShadowRoot, activate, deactivate

DAY = "2026-09-22"
ROOT = ShadowRoot(dt.date.fromisoformat(DAY))
BUCKET = "alpha-engine-research"
PREFIX = "predictor/price_cache/"  # the caller's prefix; reads/writes resolve to reference/
LIVE_KEY = "reference/price_cache/{t}.parquet"


def _listing(n: int, end: str = DAY) -> pd.DataFrame:
    """A recently-listed ticker's whole history: ``n`` sessions ending ``end``."""
    idx = pd.bdate_range(end=end, periods=n)
    return pd.DataFrame(
        {
            "Open": np.linspace(150.0, 160.0, n), "High": np.linspace(151.0, 161.0, n),
            "Low": np.linspace(149.0, 159.0, n), "Close": np.linspace(150.5, 160.5, n),
            "Volume": np.full(n, 1_000_000.0),
        },
        index=idx,
    )


def _parquet(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


class MemoryS3:
    """The bucket as botocore's ``_make_api_call`` sees it (under the interceptor)."""

    def __init__(self, objects=None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.puts: list[str] = []
        self.reads: list[str] = []

    def __call__(self, client, operation: str, params: dict):
        key = params.get("Key")
        if operation == "PutObject":
            body = params.get("Body", b"")
            if hasattr(body, "read"):
                body = body.read()
            self.objects[key] = body
            self.puts.append(key)
            return {"ETag": '"e"'}
        if operation in ("GetObject", "HeadObject"):
            self.reads.append(key)
            if key not in self.objects:
                code = "NoSuchKey" if operation == "GetObject" else "404"
                raise ClientError({"Error": {"Code": code, "Message": "missing"}}, operation)
            if operation == "HeadObject":
                return {"ContentLength": len(self.objects[key]), "ETag": '"e"'}
            return {"Body": io.BytesIO(self.objects[key]), "ContentLength": len(self.objects[key])}
        raise AssertionError(f"MemoryS3 does not model {operation}")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    monkeypatch.setattr(prices, "_sleep_seconds", lambda seconds: None)


@pytest.fixture
def shadow_s3():
    """Activate the shadow root, then put ``MemoryS3`` UNDER the interceptor."""
    real = botocore.client.BaseClient._make_api_call
    s3 = MemoryS3()
    activate(ROOT)
    interceptor._ORIGINAL = s3
    try:
        yield s3
    finally:
        interceptor._ORIGINAL = real
        deactivate()
    assert botocore.client.BaseClient._make_api_call is real


@pytest.fixture
def live_s3():
    """The same bucket with NO shadow root: the production path."""
    real = botocore.client.BaseClient._make_api_call
    s3 = MemoryS3()
    botocore.client.BaseClient._make_api_call = lambda self, op, params: s3(self, op, params)
    try:
        yield s3
    finally:
        botocore.client.BaseClient._make_api_call = real


def _vendor_answers(monkeypatch, frames: dict[str, pd.DataFrame]):
    """yfinance's shapes: one symbol -> flat columns; a batch -> ``group_by=
    "ticker"`` (symbol, field) columns over the union of the symbols' dates."""
    def _download(*_a, tickers=None, **_k):
        if isinstance(tickers, str):
            return frames[tickers].copy()
        return pd.concat({t: frames[t] for t in tickers}, axis=1)

    monkeypatch.setattr(prices.yf, "download", _download)


def _refresh(tickers, failure_reasons=None):
    return prices._refresh_stale(
        boto3.client("s3"), BUCKET, PREFIX, list(tickers), "10y", 50,
        trading_day=DAY, failure_reasons=failure_reasons,
    )


# ── the defect, in the shadow ───────────────────────────────────────────────


@pytest.mark.parametrize(
    ("cached_rows", "cache_ends"),
    [
        (69, DAY),           # the measured shape: v1 wrote D's bar ~2.5h before the shadow ran
        (68, "2026-09-21"),  # the post-cutover shape: the cache ends the prior session
    ],
)
def test_a_recent_listing_is_written_by_a_same_day_shadow_run(monkeypatch, shadow_s3, cached_rows, cache_ends):
    """HONA-shaped: 69 sessions of history, well under the 400-row threshold,
    so the short-fetch guard reads the live parquet before it decides. Before
    I11547 that read made the upload raise inside the shadow, and HONA read
    `failed` every night. It must be written to the shadow key, `ok`."""
    shadow_s3.objects[LIVE_KEY.format(t="HONA")] = _parquet(_listing(cached_rows, end=cache_ends))
    _vendor_answers(monkeypatch, {"HONA": _listing(69)})

    reasons: dict[str, str] = {}
    refreshed, failed, written = _refresh(["HONA"], failure_reasons=reasons)

    assert failed == [] and reasons == {}
    assert refreshed == 1 and written == [("HONA", 69)]
    assert shadow_s3.puts == [ROOT.key(LIVE_KEY.format(t="HONA"))], "written under the shadow root only"
    # The guard DID look — at the LIVE object, the one production compares against.
    assert LIVE_KEY.format(t="HONA") in shadow_s3.reads


def test_the_short_fetch_guard_still_refuses_in_a_shadow_run(monkeypatch, shadow_s3):
    """Not loosened: the baseline stays the LIVE cache, so a genuinely shrinking
    answer (69 cached, 10 fetched) is refused in the shadow exactly as in
    production — and recorded as what it is."""
    shadow_s3.objects[LIVE_KEY.format(t="HONA")] = _parquet(_listing(69))
    _vendor_answers(monkeypatch, {"HONA": _listing(10)})

    reasons: dict[str, str] = {}
    refreshed, failed, _written = _refresh(["HONA"], failure_reasons=reasons)

    assert refreshed == 0 and failed == ["HONA"]
    assert reasons == {"HONA": prices.FAIL_SHORT_FETCH}
    assert shadow_s3.puts == []


def test_the_behind_fetch_guard_baseline_is_also_not_an_input(monkeypatch, shadow_s3):
    """A full-length answer that ends a session early reads the cached last bar
    (I11467). Cache 69 rows ending D, fetch ending D-1 -> refused, and the
    refusal is recorded as the behind-fetch guard's, not a violation."""
    shadow_s3.objects[LIVE_KEY.format(t="AAPL")] = _parquet(_listing(2513))
    _vendor_answers(monkeypatch, {"AAPL": _listing(2512, end="2026-09-21")})

    reasons: dict[str, str] = {}
    _refreshed, failed, _written = _refresh(["AAPL"], failure_reasons=reasons)

    assert failed == ["AAPL"] and reasons == {"AAPL": prices.FAIL_BEHIND_FETCH}


def test_a_shadow_guard_violation_is_fatal_never_a_per_ticker_miss(monkeypatch, shadow_s3):
    """The violation's own contract is "always fatal". Folded into a per-ticker
    miss it read as four short-fetch refusals on three consecutive manifests.
    A GENUINE unclassified read-then-write (an input read, outside any guard
    baseline) must still stop the run."""
    key = LIVE_KEY.format(t="AAPL")
    shadow_s3.objects[key] = _parquet(_listing(2513))
    boto3.client("s3").get_object(Bucket=BUCKET, Key=key)  # read LIVE, as an input
    _vendor_answers(monkeypatch, {"AAPL": _listing(2513)})

    with pytest.raises(ShadowGuardViolation, match="unclassified read of a key the run also writes"):
        _refresh(["AAPL"])
    assert shadow_s3.puts == []


def test_collect_reads_ok_for_the_four_recent_listings_in_a_shadow_run(monkeypatch, shadow_s3):
    """The closes-when shape, end to end through ``collect()``: the four
    measured listings plus a mature ticker, same-day shadow run -> `ok`."""
    sizes = {"HONA": 69, "FDXF": 82, "Q": 227, "SOLS": 232, "NVDA": 2513}
    for ticker, n in sizes.items():
        shadow_s3.objects[LIVE_KEY.format(t=ticker)] = _parquet(_listing(n))
    _vendor_answers(monkeypatch, {t: _listing(n) for t, n in sizes.items()})
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: list(sizes))

    result = prices.collect(bucket=BUCKET, tickers=list(sizes), s3_prefix=PREFIX, reference_date=DAY)

    assert result["status"] == "ok", result.get("reason")
    assert result["failed"] == 0
    assert result["written"] == sizes
    assert "short_fetch_retries" not in result, "accepted on the batch answer, no retry needed"
    assert not [g for g in result["guards"] if g["guard"] == prices.REFUSED_KEYS_GUARD]


def test_production_path_writes_a_recent_listing(monkeypatch, live_s3):
    """No shadow root: the guard baseline scope is the identity."""
    live_s3.objects[LIVE_KEY.format(t="FDXF")] = _parquet(_listing(81, end="2026-09-21"))
    _vendor_answers(monkeypatch, {"FDXF": _listing(82)})

    refreshed, failed, written = _refresh(["FDXF"])

    assert (refreshed, failed, written) == (1, [], [("FDXF", 82)])
    assert live_s3.puts == [LIVE_KEY.format(t="FDXF")]


# ── the interceptor classification ──────────────────────────────────────────


def test_a_guard_baseline_read_is_live_and_not_an_input():
    ledger = interceptor.RunLedger()
    key = LIVE_KEY.format(t="HONA")
    with interceptor.guard_baseline_reads():
        assert interceptor.classify_read(key, bucket=BUCKET, ledger=ledger) == "guard_baseline"
        params = interceptor.rewrite_params(
            "GetObject", {"Bucket": BUCKET, "Key": key}, service="s3", root=ROOT, ledger=ledger,
        )
    assert params["Key"] == key, "the baseline is the LIVE object"
    assert ledger.baseline_reads() == frozenset({(BUCKET, key)})
    # ... so publishing the same key afterwards is not a read-then-write violation.
    interceptor.rewrite_params(
        "PutObject", {"Bucket": BUCKET, "Key": key}, service="s3", root=ROOT, ledger=ledger,
    )
    # Outside the scope the same read is an input again.
    assert interceptor.classify_read("reference/price_cache/X.parquet", bucket=BUCKET, ledger=ledger) == "input"


def test_run_state_still_wins_inside_a_guard_baseline_scope():
    ledger = interceptor.RunLedger()
    written = LIVE_KEY.format(t="NVDA")
    ledger.record_write(BUCKET, written)
    merge_base = "staging/daily_closes/" + DAY + ".parquet"
    with interceptor.guard_baseline_reads():
        assert interceptor.classify_read(written, bucket=BUCKET, ledger=ledger) == "own_write"
        assert interceptor.classify_read(merge_base, bucket=BUCKET, ledger=ledger).startswith("own_state:")


# ── attribution: the manifest names the cause it observed ───────────────────


def test_failure_counts_are_split_by_cause_and_sum_to_failed(monkeypatch, live_s3):
    live_s3.objects[LIVE_KEY.format(t="SHRNK")] = _parquet(_listing(300))
    frames = {"SHRNK": _listing(10), "OK": _listing(2513)}
    _vendor_answers(monkeypatch, frames)

    def _download(*_a, tickers=None, **_k):
        symbol = tickers if isinstance(tickers, str) else tickers[0]
        if symbol == "EMPTY":
            return pd.DataFrame()
        return frames[symbol].copy()

    monkeypatch.setattr(prices.yf, "download", _download)
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["SHRNK", "EMPTY", "OK"])
    result = prices.collect(
        bucket=BUCKET, tickers=["SHRNK", "EMPTY", "OK"], s3_prefix=PREFIX, reference_date=DAY,
        batch_size=1,  # one symbol per vendor call, so each ticker gets its own answer
    )

    assert result["status"] == "partial"
    assert result["failed_short_fetch_refused"] == 1
    assert result["failed_vendor_no_data"] == 1
    assert sum(result[k] for k in prices.FAILURE_RESULT_KEYS.values()) == result["failed"] == 2
    assert result["failure_reasons"] == {"SHRNK": prices.FAIL_SHORT_FETCH, "EMPTY": prices.FAIL_NO_DATA}
    assert "short_fetch_guard_refused=1" in result["reason"] and "vendor_no_data=1" in result["reason"]
    refused = {g["key"]: g["detail"] for g in result["guards"] if g["guard"] == prices.REFUSED_KEYS_GUARD and g["key"]}
    assert refused == {
        LIVE_KEY.format(t="SHRNK"): "SHRNK: short_fetch_guard_refused",
        LIVE_KEY.format(t="EMPTY"): "EMPTY: vendor_no_data",
    }
    summary = [g for g in result["guards"] if g["guard"] == prices.REFUSED_KEYS_GUARD and not g["key"]]
    assert [g["verdict"] for g in summary] == [prices.REFUSED_KEYS_COMPLETE]


def test_the_refusal_record_says_when_it_is_truncated(monkeypatch):
    monkeypatch.setattr(prices, "_REFUSED_KEYS_RECORD_CAP", 2)
    entries = prices.refused_keys_guard_entries({"A": "x", "B": "x", "C": "x"}, PREFIX)
    assert entries[0]["key"] is None and entries[0]["verdict"] == prices.REFUSED_KEYS_TRUNCATED
    assert entries[0]["value"] == 3.0
    assert [e["key"] for e in entries[1:]] == [LIVE_KEY.format(t="A"), LIVE_KEY.format(t="B")]


def test_d03_declares_its_rejections_by_cause_never_the_bare_total():
    expected = tuple((key, reason) for reason, key in prices.FAILURE_RESULT_KEYS.items())
    for mode in ("daily", "phase1"):
        assert run_units.PHASE_UNITS[(mode, "prices")].rejected_keys == expected
    source = (pathlib.Path(prices.__file__)).read_text(encoding="utf-8")
    for key in prices.FAILURE_RESULT_KEYS.values():
        assert f'"{key}":' in source, f"collect() must report {key} literally (rename backstop)"
    assert parity.WRITE_REFUSED_GUARD == prices.REFUSED_KEYS_GUARD
    assert parity.WRITE_REFUSED_COMPLETE == prices.REFUSED_KEYS_COMPLETE


def test_a_refresh_error_is_not_recorded_as_a_short_fetch_refusal():
    """The measured manifest said `short_fetch_guard_refused` for a failure the
    guard never made. The manifest now carries the observed cause."""
    from tests.test_partial_status_manifest_i11230 import FakeRegistry, FakeS3, _manifests

    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="daily")
    collector_result = {
        "status": "partial", "refreshed": 924, "stale": 928, "failed": 4,
        "failed_tickers": ["HONA", "Q", "FDXF", "SOLS"],
        "failed_short_fetch_refused": 0, "failed_behind_fetch_refused": 0,
        "failed_vendor_no_data": 0, "failed_batch_fetch_error": 0, "failed_refresh_error": 4,
        "reason": "4 of 932 tickers failed to refresh (refresh_error=4): HONA, Q, FDXF, SOLS",
        "written": {},
    }
    weekly_collector._phase_collect(reg, "prices", lambda: collector_result, supports_auto_skip=False)
    manifest = _manifests(s3)[0]
    assert manifest["status"] == "failed"
    assert manifest["rows_rejected"] == [{"count": 4, "reason": "refresh_error"}]


# ── attribution: the parity report cites a failure only for the keys it names ──


def _failed_d03(refused: dict[str, str], *, complete: bool = True) -> dict:
    guards = []
    if refused:
        guards.append({
            "guard": "write_refused", "mode": "enforce",
            "verdict": "complete" if complete else "truncated",
            "detail": "", "key": None, "value": float(len(refused)), "baseline": None,
        })
        guards += [
            {"guard": "write_refused", "mode": "enforce", "verdict": "refused",
             "detail": f"{t}: {why}", "key": LIVE_KEY.format(t=t), "value": None, "baseline": None}
            for t, why in refused.items()
        ]
    return {
        "unit_id": "D03", "run_id": "01M35MSPQYKAW9DGQD38PTJKPY", "status": "failed",
        "reason": "_DegradedRun: prices produced a PARTIAL artifact: 4 of 932 tickers failed to "
                  "refresh: HONA, Q, FDXF, SOLS",
        "guards": guards,
    }


def _row(key: str, manifest: dict) -> parity.KeyResult:
    return parity._absent_shadow_result(key, ["D03"], ROOT.key(key), {"D03": manifest})


FOUR = {t: "short_fetch_guard_refused" for t in ("HONA", "Q", "FDXF", "SOLS")}


def test_the_2026_09_22_shape_no_longer_blames_agnc_on_the_hona_refusal():
    """The 09-22 report: AGNC/CORT/EAT/HUBS `shadow_missing`, each citing a
    failure that named FDXF/HONA/Q/SOLS. With a complete refusal record, an
    unnamed key reads as NOT explained by that failure."""
    row = _row(LIVE_KEY.format(t="AGNC"), _failed_d03(FOUR))
    assert row.verdict == "shadow_missing"
    assert row.body["failure_attribution"] == {"D03": "not_this_key"}
    assert "NOT on this key" in row.body["detail"]
    assert "owning unit's shadow run FAILED:" not in row.body["detail"]


def test_a_key_the_failure_names_is_cited_with_its_own_reason():
    row = _row(LIVE_KEY.format(t="HONA"), _failed_d03(FOUR))
    assert row.body["failure_attribution"] == {"D03": "refused_this_key"}
    assert "refused this key (HONA: short_fetch_guard_refused)" in row.body["detail"]


def test_a_truncated_record_cannot_rule_a_key_out():
    row = _row(LIVE_KEY.format(t="AGNC"), _failed_d03(FOUR, complete=False))
    assert row.body["failure_attribution"] == {"D03": "truncated"}
    assert "truncated" in row.body["detail"]
    assert "owning unit's shadow run FAILED:" in row.body["detail"]


def test_a_manifest_without_a_refusal_record_keeps_the_unit_level_citation():
    """Every pre-I11547 manifest, and every unit that records no per-key list."""
    row = _row(LIVE_KEY.format(t="AGNC"), _failed_d03({}))
    assert row.body["failure_attribution"] == {"D03": "unrecorded"}
    assert row.body["detail"].startswith(
        "v1 wrote it; the shadow run did not — the owning unit's shadow run FAILED: D03: _DegradedRun"
    )


def test_failure_attribution_is_declared_in_the_published_schema():
    schema = json.loads(
        (pathlib.Path(__file__).resolve().parents[1] / "contracts" / "data_parity_report.schema.json")
        .read_text(encoding="utf-8")
    )
    enum = schema["$defs"]["keyResult"]["properties"]["failure_attribution"]["additionalProperties"]["enum"]
    assert set(enum) == {"refused_this_key", "not_this_key", "truncated", "unrecorded"}

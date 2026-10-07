"""D50: the morning regrade rebuilds ``features/{D-1}`` from the settled bar.

Crucible v2 ruling on alpha-engine-config-I12023 (comment 6024224623 §2). Every
guard in `features.settled_regrade` is exercised against a moto bucket with
versioning ON, because the whole design rests on S3 VersionIds: D31's recorded
inputs are served by version, D17's settled write is tied to the live object by
version, and the marker names the D31 versions it superseded.

The feature computation itself is replaced by :func:`_fake_build`, which reads
its inputs through the client it is handed, the same way
`features.compute.build_feature_frame` does: the settled bar from
``staging/daily_closes``, and the alternative partition through the
``market_data/latest_weekly.json`` pointer. Each feature column is a function of
what was read, so a wrong pin or a missed live read changes the bytes. The real
feature code is covered by its own suites; `test_default_build_is_d31s_code_*`
pins the wiring between them.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import numpy as np
import pandas as pd
import pytest

moto = pytest.importorskip("moto")
import boto3

from features import settled_regrade as sr
from features.compute import FeatureBuild
from features.feature_engineer import FEATURES
from features.input_record import InputRecorder
from features.registry import GROUPS
from features.writer import snapshot_group_frames

BUCKET = "alpha-engine-research"
D = "2026-10-02"  # a Friday: the Monday regrade is the hard case for the alt pointer
OLD_WEEK = "2026-09-26"
NEW_WEEK = "2026-10-03"
NOW = dt.datetime(2026, 10, 5, 12, 10, tzinfo=dt.UTC)  # Monday 08:10 ET
TICKERS = [f"T{i:02d}" for i in range(30)]
PROVISIONAL = "T07"
ALT_COLUMNS = set(GROUPS["alternative"])
VOLUME_COLUMNS = {"avg_volume_20d", "avg_volume_20d_raw", "rel_volume_ratio"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def s3():
    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        client.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
        yield client


def _put(client, key: str, body: bytes) -> str:
    return client.put_object(Bucket=BUCKET, Key=key, Body=body)["VersionId"]


def _closes(volume_scale: float) -> bytes:
    frame = pd.DataFrame(
        {
            "Close": [100.0 + i for i in range(len(TICKERS))],
            "Volume": [int((1_000_000 + 7919 * i) * volume_scale) for i in range(len(TICKERS))],
            "source": ["polygon"] * len(TICKERS),
        },
        index=pd.Index(TICKERS, name="ticker"),
    )
    buf = io.BytesIO()
    frame.to_parquet(buf, engine="pyarrow")
    return buf.getvalue()


def _fake_build(trading_day, bucket, *, s3, registry_client, recorder):
    """Read like D31 reads, then derive every column from what was read."""
    body = s3.get_object(Bucket=bucket, Key=f"staging/daily_closes/{trading_day}.parquet")["Body"].read()
    closes = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
    pointer = json.loads(s3.get_object(Bucket=bucket, Key="market_data/latest_weekly.json")["Body"].read())
    prefix = f"market_data/weekly/{pointer['date']}/alternative/"
    alt: dict[str, float] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            ticker = item["Key"].rsplit("/", 1)[-1].removesuffix(".json")
            alt[ticker] = json.loads(s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read())["v"]
    rows = []
    for i, ticker in enumerate(TICKERS):
        row = {"ticker": ticker}
        for j, feature in enumerate(FEATURES):
            if feature in ALT_COLUMNS:
                row[feature] = alt.get(ticker, np.nan) + j
            elif feature in VOLUME_COLUMNS:
                row[feature] = float(closes.loc[ticker, "Volume"]) / (j + 1)
            elif feature in GROUPS["macro"]:
                row[feature] = float(j)
            else:
                row[feature] = float(closes.loc[ticker, "Close"]) * (j + 1) + i * 0.5
        rows.append(row)
    return FeatureBuild(pd.DataFrame(rows), {}, n_ok=len(rows))


def _manifest_key(unit: str, run_id: str) -> str:
    return f"data_collection/runs/{unit}/{D}/{run_id}.json"


def _put_manifest(client, unit: str, run_id: str, doc: dict) -> None:
    _put(client, _manifest_key(unit, run_id), json.dumps({"unit_id": unit, "run_id": run_id, "trading_day": D, **doc}).encode())


def _settlement(key: str, verdict: str) -> dict:
    return {
        "guard": "bar_settlement",
        "mode": "observe",
        "verdict": verdict,
        "detail": f"fetch graded {verdict}",
        "key": key,
        "value": 8.2,
        "baseline": 18.25,
    }


@pytest.fixture
def world(s3):
    """Friday's D31 run, the Saturday pointer advance, Monday's D17 settle."""
    # -- Friday evening: provisional bar, pointer on the old week, D31 publishes.
    _put(s3, f"staging/daily_closes/{D}.parquet", _closes(0.8))
    _put(s3, "market_data/latest_weekly.json", json.dumps({"date": OLD_WEEK}).encode())
    for i, ticker in enumerate(TICKERS):
        _put(s3, f"market_data/weekly/{OLD_WEEK}/alternative/{ticker}.json", json.dumps({"v": float(i)}).encode())
    recorder = InputRecorder(BUCKET)
    d31_build = _fake_build(D, BUCKET, s3=recorder.wrap(s3), registry_client=None, recorder=recorder)
    outputs = []
    d31_bytes = {}
    for group, (frame, body) in snapshot_group_frames(D, d31_build.features_df).items():
        key = f"features/{D}/{group}.parquet"
        version = _put(s3, key, body)
        d31_bytes[key] = body
        if group != "factor_loading":  # D31's manifest records the five registered groups
            outputs.append({"key": key, "rows_out": len(frame), "version_id": version})
    _put_manifest(
        s3, "D31", "01K6D31AAAAAAAAAAAAAAAAAAA",
        {"status": "ok", "finished": "2026-10-02T22:54:00Z", "outputs": outputs, "inputs": recorder.freeze()},
    )
    # -- Saturday: the weekly advances the pointer to a NEW alternative partition.
    _put(s3, "market_data/latest_weekly.json", json.dumps({"date": NEW_WEEK}).encode())
    for i, ticker in enumerate(TICKERS):
        _put(s3, f"market_data/weekly/{NEW_WEEK}/alternative/{ticker}.json", json.dumps({"v": 1000.0 + i}).encode())
    # -- Monday 07:30 ET: D17 overwrites daily_closes with the settled bar.
    dc_key = f"staging/daily_closes/{D}.parquet"
    settled_version = _put(s3, dc_key, _closes(1.0))
    _put_manifest(
        s3, "D17", "01K6D17BBBBBBBBBBBBBBBBBBB",
        {
            "status": "ok",
            "finished": "2026-10-05T11:50:00Z",
            "outputs": [{"key": dc_key, "rows_out": len(TICKERS), "version_id": settled_version}],
            "guards": [_settlement(dc_key, "settled"), _settlement(f"{dc_key}#{PROVISIONAL}", "provisional")],
        },
    )
    return {"s3": s3, "outputs": outputs, "d31_bytes": d31_bytes, "dc_version": settled_version}


class OrderedClient:
    """Pass-through client that records the order of PUTs."""

    def __init__(self, inner):
        self._inner = inner
        self.put_keys: list[str] = []

    def put_object(self, **kwargs):
        self.put_keys.append(kwargs["Key"])
        return self._inner.put_object(**kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _regrade(client, **kw):
    return sr.regrade(D, bucket=BUCKET, client=client, now=NOW, build=_fake_build, code_sha="c" * 40, **kw)


def _read(client, key, version=None):
    kwargs = {"Bucket": BUCKET, "Key": key}
    if version:
        kwargs["VersionId"] = version
    return client.get_object(**kwargs)["Body"].read()


def _versions(client) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for page in client.get_paginator("list_object_versions").paginate(Bucket=BUCKET, Prefix="features/"):
        for v in page.get("Versions", []):
            out.setdefault(v["Key"], []).append(v["VersionId"])
    return out


def _frame(client, key) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(_read(client, key)), engine="pyarrow")


# ---------------------------------------------------------------------------
# The rebuild
# ---------------------------------------------------------------------------


def test_the_rebuild_takes_the_settled_bar_and_the_pinned_alternative_partition(world):
    """The Monday case: the pointer moved on Saturday, the bar settled this morning."""
    s3 = world["s3"]
    result = _regrade(s3)
    assert result["status"] == "ok", result
    technical = _frame(s3, f"features/{D}/technical.parquet")
    alternative = _frame(s3, f"features/{D}/alternative.parquet")
    # Volume moved from the provisional 0.8x to the settled bar.
    expected_volume = [float(int((1_000_000 + 7919 * i) * 1.0)) for i in range(len(TICKERS))]
    j = FEATURES.index("avg_volume_20d_raw")
    assert list(technical["avg_volume_20d_raw"]) == [v / (j + 1) for v in expected_volume]
    # The alternative columns come from Friday's partition (v = i), not the
    # week the live pointer now names (v = 1000 + i).
    k = FEATURES.index("iv_rank")
    assert list(alternative["iv_rank"]) == [float(i) + k for i in range(len(TICKERS))]
    # Every other group's bytes are D31's where nothing they read changed.
    assert _read(s3, f"features/{D}/alternative.parquet") == world["d31_bytes"][f"features/{D}/alternative.parquet"]
    assert _read(s3, f"features/{D}/technical.parquet") != world["d31_bytes"][f"features/{D}/technical.parquet"]


def test_the_marker_is_written_last_and_names_what_it_superseded(world):
    client = OrderedClient(world["s3"])
    result = _regrade(client)
    assert result["status"] == "ok", result
    assert client.put_keys[-1] == f"features/{D}/settlement.json"
    assert all(k.endswith(".parquet") for k in client.put_keys[:-1])
    marker = json.loads(_read(world["s3"], f"features/{D}/settlement.json"))
    assert marker["schema_version"] == sr.MARKER_SCHEMA
    assert marker["backfilled_at"].endswith("Z")
    assert marker["daily_closes"]["version_id"] == world["dc_version"]
    assert marker["superseded"]["d31_run_id"] == "01K6D31AAAAAAAAAAAAAAAAAAA"
    assert marker["superseded"]["outputs"] == {o["key"]: o["version_id"] for o in world["outputs"]}
    # Every superseded original is still readable by the VersionId the marker names.
    for key, version in marker["superseded"]["outputs"].items():
        assert _read(world["s3"], key, version) == world["d31_bytes"][key]
    # factor_loading is published by D31 without a manifest output; its replaced
    # version is captured too.
    assert f"features/{D}/factor_loading.parquet" in marker["superseded"]["replaced_version_ids"]
    assert marker["inputs"]["read_live"] == {f"staging/daily_closes/{D}.parquet": world["dc_version"]}
    assert "market_data/latest_weekly.json" in marker["inputs"]["pinned_to_d31"]


def test_it_writes_only_its_own_keys(world):
    s3 = world["s3"]
    before = _versions(s3)
    _regrade(s3)
    after = _versions(s3)
    new = {k for k in after if len(after[k]) > len(before.get(k, []))}
    assert new == {f"features/{D}/{g}.parquet" for g in GROUPS} | {f"features/{D}/settlement.json"}
    for key in after:
        assert not key.endswith(("registry.json", "schema_version.json"))
        assert not key.startswith("features/metron_supplemental/")


@pytest.mark.parametrize(
    "key",
    [
        "features/registry.json",
        f"features/{D}/schema_version.json",
        f"features/metron_supplemental/{D}/technical.parquet",
        "features/2026-10-01/technical.parquet",
        f"staging/daily_closes/{D}.parquet",
    ],
)
def test_the_write_allowlist_refuses_everything_else(key):
    with pytest.raises(sr.RegradeRefused):
        sr.check_write_key(key, D)


def test_a_repeat_is_not_applicable_and_byte_identical(world):
    s3 = world["s3"]
    first = _regrade(s3)
    assert first["status"] == "ok"
    snapshot = {k: _read(s3, k) for k in _versions(s3)}
    versions = _versions(s3)
    second = _regrade(s3)
    assert second["status"] == "skipped"
    assert second["skip_reason"].startswith(sr.SKIP_ALREADY_REGRADED)
    assert _versions(s3) == versions
    assert {k: _read(s3, k) for k in versions} == snapshot


def test_a_rebuild_after_a_lost_marker_reproduces_the_same_bytes(world):
    s3 = world["s3"]
    _regrade(s3)
    groups = {f"features/{D}/{g}.parquet" for g in GROUPS}
    first = {k: _read(s3, k) for k in groups}
    s3.delete_object(Bucket=BUCKET, Key=f"features/{D}/settlement.json")
    assert _regrade(s3)["status"] == "ok"
    assert {k: _read(s3, k) for k in groups} == first


def test_a_newer_settled_bar_rebuilds_again(world):
    s3 = world["s3"]
    _regrade(s3)
    dc_key = f"staging/daily_closes/{D}.parquet"
    version = _put(s3, dc_key, _closes(1.01))
    _put_manifest(
        s3, "D17", "01K6D17CCCCCCCCCCCCCCCCCCC",
        {
            "status": "ok",
            "finished": "2026-10-05T12:00:00Z",
            "outputs": [{"key": dc_key, "rows_out": len(TICKERS), "version_id": version}],
            "guards": [_settlement(dc_key, "settled")],
        },
    )
    result = _regrade(s3)
    assert result["status"] == "ok", result
    marker = json.loads(_read(s3, f"features/{D}/settlement.json"))
    assert marker["daily_closes"]["version_id"] == version
    assert marker["provisional_tickers"] == []


# ---------------------------------------------------------------------------
# Provisional rows travel per ticker
# ---------------------------------------------------------------------------


def test_a_ticker_d17_carried_provisional_stays_provisional_per_row(world):
    result = _regrade(world["s3"])
    guards = result["guards"]
    readings = {g["key"]: g["verdict"] for g in guards if g["guard"] == "bar_settlement"}
    for group in GROUPS:
        key = f"features/{D}/{group}.parquet"
        assert readings[key] == "settled"
        row = f"{key}#{PROVISIONAL}"
        if group == "macro":
            assert row not in readings
        else:
            assert readings[row] == "provisional"
    assert not [k for k in readings if k.endswith("#T08")]
    marker = json.loads(_read(world["s3"], f"features/{D}/settlement.json"))
    assert marker["provisional_tickers"] == [PROVISIONAL]


# ---------------------------------------------------------------------------
# Guards: each refusal writes nothing
# ---------------------------------------------------------------------------


def _assert_refused(world, match: str):
    s3 = world["s3"]
    before = _versions(s3)
    result = _regrade(s3)
    assert result["status"] == "error", result
    assert match in result["error"], result["error"]
    assert _versions(s3) == before
    return result


def test_refused_without_a_d17_run(world):
    world["s3"].delete_object(Bucket=BUCKET, Key=_manifest_key("D17", "01K6D17BBBBBBBBBBBBBBBBBBB"))
    _assert_refused(world, "no D17 run manifest")


def test_refused_when_d17_failed(world):
    _put_manifest(world["s3"], "D17", "01K6D17ZZZZZZZZZZZZZZZZZZZ", {"status": "failed", "finished": "2026-10-05T11:55:00Z"})
    _assert_refused(world, "status='failed'")


def test_refused_when_d17_is_not_this_mornings(world):
    """Saturday's weekly D17 also keys Friday; it is not Monday's settle."""
    s3 = world["s3"]
    key = _manifest_key("D17", "01K6D17BBBBBBBBBBBBBBBBBBB")
    doc = json.loads(_read(s3, key))
    doc["finished"] = "2026-10-03T09:40:00Z"
    _put(s3, key, json.dumps(doc).encode())
    _assert_refused(world, "is not this morning's run")


def test_refused_when_daily_closes_moved_after_d17(world):
    _put(world["s3"], f"staging/daily_closes/{D}.parquet", _closes(0.9))
    _assert_refused(world, "something wrote it after the settled enrich")


def test_refused_when_d17_graded_the_bar_provisional(world):
    s3 = world["s3"]
    key = _manifest_key("D17", "01K6D17BBBBBBBBBBBBBBBBBBB")
    doc = json.loads(_read(s3, key))
    doc["guards"][0]["verdict"] = "provisional"
    _put(s3, key, json.dumps(doc).encode())
    _assert_refused(world, "not settled")


def test_a_same_date_noop_is_graded_by_the_run_it_points_to(world):
    _put_manifest(
        world["s3"], "D17", "01K6D17ZZZZZZZZZZZZZZZZZZZ",
        {"status": "not_applicable", "reason": "no_new_data_declared", "finished": "2026-10-05T12:00:00Z"},
    )
    assert _regrade(world["s3"])["status"] == "ok"


def test_refused_without_a_d31_manifest(world):
    world["s3"].delete_object(Bucket=BUCKET, Key=_manifest_key("D31", "01K6D31AAAAAAAAAAAAAAAAAAA"))
    _assert_refused(world, "no D31 run manifest")


def test_refused_when_d31_recorded_no_inputs(world):
    s3 = world["s3"]
    key = _manifest_key("D31", "01K6D31AAAAAAAAAAAAAAAAAAA")
    doc = json.loads(_read(s3, key))
    doc["inputs"] = []
    _put(s3, key, json.dumps(doc).encode())
    _assert_refused(world, "records no inputs")


def test_refused_when_a_pinned_version_is_gone(world):
    """The pointer D31 read was deleted by version: the rebuild cannot read it as recorded."""
    s3 = world["s3"]
    doc = json.loads(_read(s3, _manifest_key("D31", "01K6D31AAAAAAAAAAAAAAAAAAA")))
    pointer = next(r for r in doc["inputs"] if r["key"].endswith("market_data/latest_weekly.json"))
    s3.delete_object(Bucket=BUCKET, Key="market_data/latest_weekly.json", VersionId=pointer["version"])
    _assert_refused(world, "could not be served as recorded")


def test_the_dead_column_postflight_is_fatal(world):
    def constant(trading_day, bucket, **kw):
        build = _fake_build(trading_day, bucket, **kw)
        build.features_df["rsi_14"] = 50.0
        return build

    s3 = world["s3"]
    before = _versions(s3)
    result = sr.regrade(D, bucket=BUCKET, client=s3, now=NOW, build=constant, code_sha="c" * 40)
    assert result["status"] == "error"
    assert "dead-column postflight" in result["error"] and "rsi_14" in result["error"]
    assert _versions(s3) == before


def test_refused_when_a_group_d31_published_is_not_rebuilt(world):
    def no_alt(trading_day, bucket, **kw):
        build = _fake_build(trading_day, bucket, **kw)
        build.features_df.drop(columns=list(ALT_COLUMNS), inplace=True)
        return build

    s3 = world["s3"]
    result = sr.regrade(D, bucket=BUCKET, client=s3, now=NOW, build=no_alt, code_sha="c" * 40)
    assert result["status"] == "error"
    assert "leave some groups provisional" in result["error"]


def test_the_feature_code_cannot_write_through_the_pinned_client(world):
    def writes(trading_day, bucket, *, s3, **kw):
        s3.put_object(Bucket=bucket, Key="features/registry.json", Body=b"{}")

    s3 = world["s3"]
    result = sr.regrade(D, bucket=BUCKET, client=s3, now=NOW, build=writes, code_sha="c" * 40)
    assert result["status"] == "error"
    assert "ReadOnlyViolation" in result["error"]


# ---------------------------------------------------------------------------
# Wiring and bounds
# ---------------------------------------------------------------------------


def test_default_build_is_d31s_code_with_arcticdb_read_before_and_to_d_minus_1(monkeypatch):
    from features import compute

    seen: dict = {}

    def fake_build_feature_frame(date_str, bucket, **kw):
        seen["build"] = (date_str, kw)
        kw["price_source_loader"]("S3", bucket)
        return "BUILD"

    def fake_load_price_source(s3, bucket, **kw):
        seen["arctic"] = kw

    monkeypatch.setattr(compute, "build_feature_frame", fake_build_feature_frame)
    monkeypatch.setattr(compute, "_load_price_source", fake_load_price_source)
    recorder = InputRecorder(BUCKET)
    assert sr._default_build(D, BUCKET, s3="S3", registry_client="RAW", recorder=recorder) == "BUILD"
    date_str, kw = seen["build"]
    assert date_str == D
    assert kw["exclude_trading_day_arctic_rows"] is True
    assert kw["registry_client"] == "RAW" and kw["recorder"] is recorder
    assert seen["arctic"]["end"] == pd.Timestamp(D) == seen["arctic"]["before"]
    assert seen["arctic"]["recorder"] is recorder


def test_the_d17_freshness_bound_covers_this_morning_and_excludes_saturday():
    """Derived from the morning schedule: a D17 that finished at the fire time
    must still count when D50 is launched at its latest, and the Saturday weekly's
    D17 (same trading-day partition, ~48 h earlier) must not."""
    from infrastructure import data_collection_stack as stack

    tpl = stack.load_template()
    morning = {s["name"]: s for s in stack.schedules(tpl)}["data-collection-morning"]["input"]["workloads"]
    position = morning.index("features-settled-regrade")
    _default, _caps, ssm = stack.dispatcher_runtime_caps()
    latest_launch = stack.worst_case_seconds(morning, through=morning[position - 1]) + ssm
    assert sr.D17_MAX_AGE_SECONDS >= latest_launch
    assert sr.D17_MAX_AGE_SECONDS < 48 * 3600 - 3 * 3600


# ---------------------------------------------------------------------------
# Through the run-manifest wrapper (weekly_collector --features-settled-regrade)
# ---------------------------------------------------------------------------


class _Args:
    date = D
    dry_run = False
    features_settled_regrade = True


def _d50_manifests(client) -> list[dict]:
    out = []
    resp = client.list_objects_v2(Bucket=BUCKET, Prefix=f"data_collection/runs/D50/{D}/")
    for item in sorted(resp.get("Contents", []), key=lambda o: o["Key"]):
        out.append(json.loads(_read(client, item["Key"])))
    return out


def test_the_mode_files_a_d50_manifest_then_a_not_applicable_repeat(world, monkeypatch):
    import functools

    import weekly_collector

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    monkeypatch.setattr(
        sr, "regrade", functools.partial(sr.regrade, now=NOW, build=_fake_build, code_sha="c" * 40)
    )
    s3 = world["s3"]
    config = {"bucket": BUCKET}

    first = weekly_collector.run_weekly(config, _Args())
    assert first["status"] == "ok", first
    [manifest] = _d50_manifests(s3)
    assert manifest["unit_id"] == "D50" and manifest["status"] == "ok"
    assert manifest["trading_day"] == D
    keys = {o["key"] for o in manifest["outputs"]}
    assert keys == {f"features/{D}/{g}.parquet" for g in GROUPS} | {f"features/{D}/settlement.json"}
    assert not any(k.startswith("arcticdb/") for k in keys)
    # The run records what it read: the D31-pinned pointer and the live bar.
    inputs = {i["key"]: i["version"] for i in manifest["inputs"]}
    assert inputs[f"s3://{BUCKET}/staging/daily_closes/{D}.parquet"] == world["dc_version"]
    assert f"s3://{BUCKET}/market_data/latest_weekly.json" in inputs
    guards = {(g["guard"], g["key"]): g["verdict"] for g in manifest["guards"]}
    assert guards[("bar_settlement", f"features/{D}/technical.parquet#{PROVISIONAL}")] == "provisional"
    assert guards[("data_empty_fresh", None)] == "ok"

    second = weekly_collector.run_weekly(config, _Args())
    assert second["status"] == "skipped"
    repeat = _d50_manifests(s3)[-1]
    assert repeat["status"] == "not_applicable"
    assert repeat["reason"] == "no_new_data_declared"


def test_a_refusal_files_a_failed_manifest_and_the_run_exits_nonzero(world, monkeypatch):
    import functools

    import weekly_collector

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setattr(
        sr, "regrade", functools.partial(sr.regrade, now=NOW, build=_fake_build, code_sha="c" * 40)
    )
    world["s3"].delete_object(Bucket=BUCKET, Key=_manifest_key("D31", "01K6D31AAAAAAAAAAAAAAAAAAA"))
    result = weekly_collector.run_weekly({"bucket": BUCKET}, _Args())
    assert result["status"] == "error"
    [manifest] = _d50_manifests(world["s3"])
    assert manifest["status"] == "failed"
    assert "no D31 run manifest" in manifest["reason"]

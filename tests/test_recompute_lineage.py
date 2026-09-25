"""Recompute lineage: `v1_cause` for a DERIVED key, proven through its inputs.

`alpha-engine-config-I11203`, Brian's "proven v1 cause" ruling. D31's
`features/{D}/*.parquet` stamps no settlement of its own, so neither existing
evidence kind can ever explain it, and it reads `unsettled` on every report.
This evidence kind is a counterfactual proof with three halves:

1. ``f(I_v) == V`` — the feature code, run over the inputs v1's D31 RECORDED,
   gives back v1's published bytes;
2. ``f(I_s) == S`` — the same for the shadow;
3. every recorded input that differs between the two runs itself grades
   `v1_cause` under its own key's comparison. The evidence chains.

Pinned here, all on synthetic data (no AWS, no ArcticDB):

A. D31 records what it read (`features.input_record`) and the manifest carries it;
B. the recompute reads exactly that and nothing else, read-only (`PinnedS3`);
C. the recompute reproduces both sides — and a CODE DIFFERENCE, a changed
   ArcticDB library, a run with no recorded inputs and a lookback difference
   are each detected and refused;
D. parity grants `recompute_lineage` only when the chain holds, and refuses —
   with a reason — when an input is unproven, an ArcticDB library differs, the
   record is about other bytes, or there is no record;
E. the `shadow-sameday` shell runs it after `parity` and never lets it mask a
   failed leg or parity's own exit.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import itertools
import json
import pathlib
import subprocess
import types

import numpy as np
import pandas as pd
import pytest

import weekly_collector
from collectors import daily_closes
from features import compute
from features.input_record import (
    AGGREGATE_MIN_MEMBERS,
    InputRecorder,
    arctic_key,
    frames_digest,
    parse_refs,
    restated_key,
)
from features.writer import snapshot_group_frames
from shadow import parity
from shadow import recompute_lineage as rl
from shadow.root import ShadowRoot
from tests.test_shadow_morning_split_i11352 import workloads  # noqa: F401 - pytest fixture

BUCKET = "alpha-engine-research"
DAY = dt.date(2026, 9, 23)
D = DAY.isoformat()
CLOSES_KEY = f"staging/daily_closes/{D}.parquet"
ROOT = ShadowRoot(DAY)
TICKERS = [f"T{i:02d}" for i in range(24)]

#: Measured write moments (UTC), 2026-09-23: v1's D19 at 16:04 ET on the
#: provisional bar, the shadow's at 18:45 ET on the settled one.
V1_D19_FETCH = dt.datetime(2026, 9, 23, 20, 4, 38, tzinfo=dt.timezone.utc)
SH_D19_FETCH = dt.datetime(2026, 9, 23, 22, 45, 22, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------------
# A versioned in-memory S3
# ---------------------------------------------------------------------------


class _ClientNoSuchKey(Exception):
    """botocore's modelled-exception constructor shape."""

    def __init__(self, error_response=None, operation_name=None):
        super().__init__(str(error_response))
        self.response = error_response or {"Error": {"Code": "NoSuchKey"}}


class VersionedS3:
    exceptions = types.SimpleNamespace(NoSuchKey=_ClientNoSuchKey)

    def __init__(self) -> None:
        self.objects: dict[str, list[tuple[str, bytes]]] = {}
        self._ids = itertools.count(1)
        self.calls: list[tuple[str, str, "str | None"]] = []

    def put(self, key: str, body: bytes) -> str:
        version = f"ver{next(self._ids):04d}"
        self.objects.setdefault(key, []).append((version, bytes(body)))
        return version

    def put_object(self, *, Bucket, Key, Body, **_):  # noqa: N803
        return {"VersionId": self.put(Key, Body)}

    def _find(self, key: str, version: "str | None") -> "tuple[str, bytes]":
        versions = self.objects.get(key)
        if not versions:
            raise _ClientNoSuchKey({"Error": {"Code": "NoSuchKey", "Message": key}}, "GetObject")
        if version is None:
            return versions[-1]
        for found in versions:
            if found[0] == version:
                return found
        raise _ClientNoSuchKey({"Error": {"Code": "NoSuchVersion", "Message": key}}, "GetObject")

    def get_object(self, *, Bucket, Key, VersionId=None, **_):  # noqa: N803
        self.calls.append(("get", Key, VersionId))
        version, body = self._find(Key, VersionId)
        return {"Body": io.BytesIO(body), "VersionId": version, "ETag": f'"{hashlib.md5(body).hexdigest()}"'}

    def head_object(self, *, Bucket, Key, VersionId=None, **_):  # noqa: N803
        version, body = self._find(Key, VersionId)
        return {"VersionId": version, "ETag": f'"{hashlib.md5(body).hexdigest()}"', "ContentLength": len(body)}

    def list_objects_v2(self, *, Bucket, Prefix="", MaxKeys=1000, **_):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix))[:MaxKeys]
        return {"Contents": [{"Key": k} for k in keys], "KeyCount": len(keys)}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        owner = self

        class _P:
            def paginate(self, **kw):
                yield owner.list_objects_v2(**kw)

        return _P()


class ShadowView:
    """What `shadow.interceptor` does to a shadow run's reads: its own writes
    are read back from under the shadow root, everything else from the live key."""

    def __init__(self, inner: VersionedS3) -> None:
        self._inner = inner
        self.exceptions = inner.exceptions

    def get_object(self, *, Bucket, Key, **kw):  # noqa: N803
        if ROOT.key(Key) in self._inner.objects:
            return self._inner.get_object(Bucket=Bucket, Key=ROOT.key(Key), **kw)
        return self._inner.get_object(Bucket=Bucket, Key=Key, **kw)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class Store:
    """`data_gate.store` shape."""

    def __init__(self, objects: "dict[str, bytes] | None" = None) -> None:
        self.objects = dict(objects or {})

    def get_bytes(self, key: str) -> bytes:
        if key not in self.objects:
            raise FileNotFoundError(key)
        return self.objects[key]

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.objects[key] = payload


# ---------------------------------------------------------------------------
# A synthetic D31 day
# ---------------------------------------------------------------------------


def _history(ticker_index: int, n: int = 400, *, end: str = "2026-09-22") -> pd.DataFrame:
    rng = np.random.default_rng(ticker_index)
    close = 100.0 * np.exp(np.cumsum(0.0003 * (ticker_index - 12) + rng.normal(0, 0.01, n)))
    idx = pd.bdate_range(end=end, periods=n)
    return pd.DataFrame(
        {
            "Open": close * 0.998,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
            "VWAP": close * 1.0005,
        },
        index=idx,
    )


def _library(*, with_trading_day_rows: bool = False, bump: float = 0.0):
    """The ArcticDB libraries as a D31 read would find them.

    ``with_trading_day_rows``: v1's D32 has appended day D, as it has by the
    time the shadow's D31 runs. ``bump`` rewrites one historical close.
    """
    universe = {t: _history(i) for i, t in enumerate(TICKERS)}
    macro = {s: _history(100 + i)[["Close"]] for i, s in enumerate(["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"])}
    if with_trading_day_rows:
        for frames in (universe, macro):
            for sym, df in frames.items():
                extra = df.iloc[[-1]].copy()
                extra.index = pd.DatetimeIndex([pd.Timestamp(D)])
                frames[sym] = pd.concat([df, extra * 1.5])
    if bump:
        universe["T03"] = universe["T03"].copy()
        universe["T03"].iloc[-10, universe["T03"].columns.get_loc("Close")] += bump
    return universe, macro


def _library_reader(universe, macro):
    def read(library, symbols, *, lookback_days, end):
        frames = universe if library == "universe" else macro
        return {s: df.copy() for s, df in frames.items() if symbols is None or s in set(symbols)}

    return read


def _closes(scale: float) -> bytes:
    rows = {t: _history(i).iloc[-1]["Close"] * scale for i, t in enumerate(TICKERS)}
    frame = pd.DataFrame(
        {
            "Open": [v * 0.999 for v in rows.values()],
            "High": [v * 1.01 for v in rows.values()],
            "Low": [v * 0.99 for v in rows.values()],
            "Close": list(rows.values()),
            "Volume": [2_000_000] * len(rows),
            "VWAP": [v * 1.0002 for v in rows.values()],
            "source": ["polygon"] * len(rows),
        },
        index=pd.Index(list(rows), name="ticker"),
    )
    buf = io.BytesIO()
    frame.to_parquet(buf)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def _no_live_arcticdb(monkeypatch):
    """`_load_price_source` lists the macro library for XL* ETFs directly."""

    class _Lib:
        def list_symbols(self):
            return []

    monkeypatch.setattr(compute, "open_macro_lib", lambda bucket: _Lib())


def _d31(client, *, library, exclude=True):
    """One D31 run: the real build, recording through the real recorder."""
    recorder = InputRecorder(BUCKET)
    reader = _library_reader(*library)

    def price_source(s3, bucket):
        return compute._load_price_source(
            s3,
            bucket,
            end=pd.Timestamp(D),
            before=pd.Timestamp(D) if exclude else None,
            recorder=recorder,
            library_reader=reader,
        )

    build = compute.build_feature_frame(
        D,
        BUCKET,
        s3=recorder.wrap(client),
        registry_client=None,
        recorder=recorder,
        exclude_trading_day_arctic_rows=exclude,
        price_source_loader=price_source,
    )
    return build, recorder.freeze()


class Day:
    """Both sides' D31 runs for DAY, published into one versioned bucket."""

    def __init__(self, *, v1_library=None, shadow_library=None):
        self.s3 = VersionedS3()
        self.s3.put("data/sector_map.json", json.dumps({t: "XLK" for t in TICKERS}).encode())
        self.s3.put("data/sub_sector_etf_map.json", b"{}")
        self.v1_closes = self.s3.put(CLOSES_KEY, _closes(1.000))  # v1's D19, 16:04 ET
        self.shadow_closes = self.s3.put(ROOT.key(CLOSES_KEY), _closes(1.004))  # shadow's D19, 18:45 ET
        self.v1_library = v1_library or _library()
        # The shadow's D31 runs after v1's D32 appended day D to ArcticDB.
        self.shadow_library = shadow_library or _library(with_trading_day_rows=True)
        v1_build, v1_inputs = _d31(self.s3, library=self.v1_library)
        sh_build, sh_inputs = _d31(ShadowView(self.s3), library=self.shadow_library)
        self.published = {"v1": {}, "shadow": {}}
        outputs = {"v1": [], "shadow": []}
        for side, build in (("v1", v1_build), ("shadow", sh_build)):
            for group, (_frame, body) in snapshot_group_frames(D, build.features_df).items():
                key = f"features/{D}/{group}.parquet"
                physical = key if side == "v1" else ROOT.key(key)
                self.published[side][key] = body
                outputs[side].append({"key": key, "version_id": self.s3.put(physical, body)})
        self.v1_manifest = self._manifest("D31", "v1-d31", "2026-09-23T20:17:09Z", v1_inputs, outputs["v1"])
        self.shadow_manifest = self._manifest("D31", "sh-d31", "2026-09-23T22:55:42Z", sh_inputs, outputs["shadow"])
        self.s3.put(f"data_collection/runs/D31/{D}/v1-d31.json", json.dumps(self.v1_manifest).encode())
        self.s3.put(ROOT.key(f"data_collection/runs/D31/{D}/sh-d31.json"), json.dumps(self.shadow_manifest).encode())
        # The two D19 writes of the differing input, each with its settlement stamp.
        for side, fetch, version, run_id in (
            ("v1", V1_D19_FETCH, self.v1_closes, "v1-d19"),
            ("shadow", SH_D19_FETCH, self.shadow_closes, "sh-d19"),
        ):
            manifest = self._manifest(
                "D19",
                run_id,
                fetch.strftime("%Y-%m-%dT%H:%M:%SZ"),
                [],
                [{"key": CLOSES_KEY, "version_id": version}],
                guards=daily_closes._settlement_guards(fetch, D, CLOSES_KEY),
            )
            where = f"data_collection/runs/D19/{D}/{run_id}.json"
            self.s3.put(where if side == "v1" else ROOT.key(where), json.dumps(manifest).encode())

    @staticmethod
    def _manifest(unit, run_id, started, inputs, outputs, guards=()):
        return {
            "schema_version": "data_run_manifest.v1",
            "unit_id": unit,
            "run_id": run_id,
            "status": "ok",
            "trading_day": D,
            "started": started,
            "finished": started,
            "code_sha": "c" * 40,
            "inputs": inputs,
            "outputs": outputs,
            "guards": list(guards),
        }

    def arctic_loader(self, library):
        reader = _library_reader(*library)

        def load(s3, bucket, *, window, recorder):
            return compute._load_price_source(
                s3,
                bucket,
                end=pd.Timestamp(window.end),
                before=pd.Timestamp(window.before) if window.before else None,
                recorder=recorder,
                library_reader=reader,
            )

        return load

    def evaluate(self, *, v1_library=None, shadow_library=None):
        """The recompute. Each side's ArcticDB replay is that side's library
        unless overridden (in production: the library AS OF the recorded read)."""
        loaders = {
            "v1": self.arctic_loader(v1_library or self.v1_library),
            "shadow": self.arctic_loader(shadow_library or self.shadow_library),
        }
        reader = parity.S3Reader(BUCKET, self.s3)
        v1 = rl.recompute_side(
            "v1", self.v1_manifest, trading_day=DAY, bucket=BUCKET, client=self.s3, arctic_loader=loaders["v1"]
        )
        shadow = rl.recompute_side(
            "shadow",
            self.shadow_manifest,
            trading_day=DAY,
            bucket=BUCKET,
            client=self.s3,
            physical_prefixes=(ROOT.prefix, ""),
            arctic_loader=loaders["shadow"],
        )

        def published(side, key, out):
            meta = reader.get_with_meta(key if side == "v1" else ROOT.key(key), version_id=(out or {}).get("version_id"))
            return None if meta is None else meta["body"]

        return rl.build_record(
            DAY,
            v1_manifest=self.v1_manifest,
            shadow_manifest=self.shadow_manifest,
            v1=v1,
            shadow=shadow,
            published=published,
            code_sha="test",
            shadow_prefix=ROOT.prefix,
        )


@pytest.fixture(scope="module")
def day():
    with pytest.MonkeyPatch.context() as mp:

        class _Lib:
            def list_symbols(self):
                return []

        mp.setattr(compute, "open_macro_lib", lambda bucket: _Lib())
        yield Day()


@pytest.fixture(scope="module")
def record(day):
    with pytest.MonkeyPatch.context() as mp:

        class _Lib:
            def list_symbols(self):
                return []

        mp.setattr(compute, "open_macro_lib", lambda bucket: _Lib())
        return day.evaluate()


# ---------------------------------------------------------------------------
# A. D31 records what it read
# ---------------------------------------------------------------------------


def test_the_recorder_round_trips_every_input_kind():
    rec = InputRecorder(BUCKET)
    rec.object_read(BUCKET, "data/sector_map.json", "vA", '"e1"')
    rec.object_read(BUCKET, "data/sector_map.json", "vB", '"e2"')  # first read wins
    for i in range(AGGREGATE_MIN_MEMBERS + 1):
        rec.object_read(BUCKET, f"market_data/weekly/2026-09-19/alternative/T{i:03d}.json", f"v{i}", None)
    frames = {"AAPL": _history(1)}
    rec.arctic_loaded("universe", frames, end=D, lookback_days=1018, before=D, as_of="2026-09-23T20:17:31.000001Z")
    rec.restatement(start="2024-12-09", end=D, restated_tickers=[])
    refs = rec.freeze()
    rec.object_read(BUCKET, "late.json", "v9", None)  # frozen: ignored
    assert all(set(r) == {"key", "etag", "version", "schema_version"} for r in refs)

    pins = parse_refs(refs)
    assert pins.objects["data/sector_map.json"].version_id == "vA"
    assert pins.objects["data/sector_map.json"].etag == "e1"
    assert "late.json" not in pins.objects
    (prefix, members), = pins.sets.items()
    assert prefix == "market_data/weekly/2026-09-19/alternative/"
    assert members.count == AGGREGATE_MIN_MEMBERS + 1
    universe = pins.arctic["universe"]
    assert (universe.end, universe.lookback_days, universe.before) == (D, 1018, D)
    assert universe.as_of == "2026-09-23T20:17:31.000001Z"
    assert universe.digest == frames_digest(frames)
    assert pins.restated == ()
    assert pins.unreadable == ()


def test_parse_refs_reads_both_version_spellings_and_names_what_it_cannot():
    pins = parse_refs(
        [
            {"key": f"s3://{BUCKET}/a.json", "version_id": "v1"},
            {"key": f"s3://{BUCKET}/b.json", "version": "v2"},
            {"key": arctic_key(BUCKET, "universe", end=D, lookback_days=1018, before=None), "version": "nope"},
            {"key": restated_key(BUCKET, start="x", end=D), "version": "restated:AAA,BBB"},
            {"key": "gopher://what", "version": "?"},
        ]
    )
    assert pins.objects["a.json"].version_id == "v1"
    assert pins.objects["b.json"].version_id == "v2"
    assert pins.restated == ("AAA", "BBB")
    assert len(pins.unreadable) == 2


def test_the_recording_client_records_the_version_actually_served():
    s3 = VersionedS3()
    version = s3.put("data/sector_map.json", b"{}")
    rec = InputRecorder(BUCKET)
    rec.wrap(s3).get_object(Bucket=BUCKET, Key="data/sector_map.json")
    (ref,) = rec.refs()
    assert ref["key"] == f"s3://{BUCKET}/data/sector_map.json"
    assert ref["version"] == version
    assert ref["etag"] == hashlib.md5(b"{}").hexdigest()


def test_d31_drops_the_trading_days_arctic_rows_and_records_the_read_moment():
    """The shadow fidelity leak: by the time the shadow's D31 runs, v1's D32 has
    appended day D to the SHARED ArcticDB library, and the shadow read it as an
    input. D31 now takes day D from its own daily_closes delta only."""
    universe, macro = _library(with_trading_day_rows=True)
    seen = {}

    class _Rec:
        def arctic_loaded(self, library, frames, **kw):
            seen[library] = (frames, kw)

    compute._load_price_source(
        None, BUCKET, end=pd.Timestamp(D), before=pd.Timestamp(D), recorder=_Rec(),
        library_reader=_library_reader(universe, macro),
    )
    for library in ("universe", "macro"):
        frames, kw = seen[library]
        assert all(df.index.max() < pd.Timestamp(D) for df in frames.values())
        assert kw["before"] == D and kw["end"] == D and kw["lookback_days"] == compute._ARCTICDB_LOOKBACK_DAYS
        assert pd.Timestamp(kw["as_of"]).tzinfo is not None


def test_d31_hands_its_input_refs_to_the_run_manifest(monkeypatch):
    """`weekly_collector` folds the refs `compute_and_write` returns onto the
    D31 manifest, in the lib's closed `InputRef` shape."""
    from tests.test_phase_collect_run_manifest import FakeRegistry, FakeS3, _manifests

    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    key = "features/2026-09-14/technical.parquet"
    s3 = FakeS3({key: 4096})
    refs = [
        {"key": f"s3://{BUCKET}/data/sector_map.json", "etag": "e", "version": "v7", "schema_version": None},
        {"key": arctic_key(BUCKET, "universe", end="2026-09-14", lookback_days=1018, before="2026-09-14"),
         "etag": None, "version": "sha256:abc", "schema_version": None},
    ]
    weekly_collector._phase_collect(
        FakeRegistry(s3),
        "features",
        lambda: {"status": "ok", "tickers_computed": 900, "input_refs": refs},
        artifact_key=key,
    )
    (manifest,) = _manifests(s3)
    assert manifest["unit_id"] == "D31"
    assert manifest["inputs"] == refs


# ---------------------------------------------------------------------------
# B. The recompute reads exactly what was recorded, and only reads
# ---------------------------------------------------------------------------


def _pins(**objects):
    return parse_refs([{"key": f"s3://{BUCKET}/{k}", "version": v} for k, v in objects.items()])


def test_a_current_recorded_version_is_served_with_a_plain_get():
    s3 = VersionedS3()
    version = s3.put("data/sector_map.json", b"{}")
    pinned = rl.PinnedS3(s3, _pins(**{"data/sector_map.json": version}))
    assert pinned.get_object(Bucket=BUCKET, Key="data/sector_map.json")["Body"].read() == b"{}"
    assert ("get", "data/sector_map.json", None) in s3.calls
    assert pinned.served["data/sector_map.json"]["version_id"] == version


def test_an_overwritten_recorded_version_is_served_by_version():
    s3 = VersionedS3()
    old = s3.put("data/sector_map.json", b'{"A": "XLK"}')
    s3.put("data/sector_map.json", b'{"A": "XLF"}')
    pinned = rl.PinnedS3(s3, _pins(**{"data/sector_map.json": old}))
    assert pinned.get_object(Bucket=BUCKET, Key="data/sector_map.json")["Body"].read() == b'{"A": "XLK"}'
    assert ("get", "data/sector_map.json", old) in s3.calls


def test_the_shadow_root_is_tried_first_for_a_shadow_runs_own_write():
    s3 = VersionedS3()
    s3.put(CLOSES_KEY, b"v1")
    own = s3.put(ROOT.key(CLOSES_KEY), b"shadow")
    pinned = rl.PinnedS3(s3, _pins(**{CLOSES_KEY: own}), physical_prefixes=(ROOT.prefix, ""))
    assert pinned.get_object(Bucket=BUCKET, Key=CLOSES_KEY)["Body"].read() == b"shadow"
    assert pinned.served[CLOSES_KEY]["physical_key"] == ROOT.key(CLOSES_KEY)


def test_an_unrecorded_key_reads_as_missing_and_is_named():
    s3 = VersionedS3()
    s3.put("market_data/latest_weekly.json", b"{}")
    pinned = rl.PinnedS3(s3, _pins())
    with pytest.raises(pinned.exceptions.NoSuchKey):
        pinned.get_object(Bucket=BUCKET, Key="market_data/latest_weekly.json")
    assert pinned.unrecorded == ["market_data/latest_weekly.json"]


def test_the_recompute_cannot_write_or_pick_its_own_version():
    pinned = rl.PinnedS3(VersionedS3(), _pins())
    with pytest.raises(rl.ReadOnlyViolation):
        pinned.put_object(Bucket=BUCKET, Key="features/x.parquet", Body=b"")
    with pytest.raises(rl.ReadOnlyViolation):
        pinned.delete_object(Bucket=BUCKET, Key="features/x.parquet")
    with pytest.raises(rl.ReadOnlyViolation):
        pinned.get_object(Bucket=BUCKET, Key="a", VersionId="v")


def test_a_recorded_set_that_changed_since_is_a_problem():
    s3 = VersionedS3()
    prefix = "market_data/weekly/2026-09-19/alternative/"
    rec = InputRecorder(BUCKET)
    for i in range(AGGREGATE_MIN_MEMBERS + 1):
        key = f"{prefix}T{i:03d}.json"
        rec.object_read(BUCKET, key, s3.put(key, b"{}"), None)
    pins = parse_refs(rec.refs())
    s3.put(f"{prefix}T000.json", b'{"changed": 1}')  # a member rewritten since
    pinned = rl.PinnedS3(s3, pins)
    for i in range(AGGREGATE_MIN_MEMBERS + 1):
        pinned.get_object(Bucket=BUCKET, Key=f"{prefix}T{i:03d}.json")
    assert pinned.set_problems()


# ---------------------------------------------------------------------------
# C. The recompute
# ---------------------------------------------------------------------------


def test_both_sides_reproduce_and_only_the_differing_input_is_listed(day, record):
    assert record["schema_version"] == rl.SCHEMA_VERSION
    assert record["status"] == "complete", record["sides"]
    assert set(record["keys"]) == {f"features/{D}/{g}.parquet" for g in ("technical", "interaction", "macro", "alternative", "fundamental", "factor_loading")}
    for key, entry in record["keys"].items():
        assert entry["v1"]["reproduced"] and entry["shadow"]["reproduced"], key
    technical = record["keys"][f"features/{D}/technical.parquet"]
    assert technical["v1"]["published_sha256"] != technical["shadow"]["published_sha256"]
    (delta,) = record["differing_inputs"]
    assert delta["key"] == CLOSES_KEY
    assert delta["v1"]["version_id"] == day.v1_closes
    assert delta["shadow"]["version_id"] == day.shadow_closes
    assert delta["shadow"]["physical_key"] == ROOT.key(CLOSES_KEY)
    assert record["recompute_host"]["numpy"] == np.__version__


def test_a_code_difference_between_the_runs_and_the_recompute_is_refused(day, monkeypatch):
    """The recompute proves the code maps inputs to outputs. A feature code
    that differs from the one that published — here by one ulp-scale term —
    reproduces neither side, and parity refuses."""
    real = compute.compute_features

    def drifted(*args, **kwargs):
        out = real(*args, **kwargs)
        if "rsi_14" in out:
            out["rsi_14"] = out["rsi_14"] * (1 + 1e-12)
        return out

    monkeypatch.setattr(compute, "compute_features", drifted)
    record = day.evaluate()
    technical = record["keys"][f"features/{D}/technical.parquet"]
    assert not technical["v1"]["reproduced"] and not technical["shadow"]["reproduced"]
    proof, why = parity._recompute_lineage_evidence(
        f"features/{D}/technical.parquet",
        parity.RecomputeLineage("lineage/D31/x.json", record, chain=({"key": CLOSES_KEY},)),
        technical["v1"]["published_sha256"],
        technical["shadow"]["published_sha256"],
        numeric=12,
    )
    assert proof is None
    assert "did not reproduce the v1 file" in why


def test_an_arctic_library_that_changed_since_the_read_refuses_the_side(day):
    record = day.evaluate(v1_library=_library(bump=0.5))
    assert record["status"] == "refused"
    assert "arcticdb/universe" in record["sides"]["v1"]["refusal"]
    assert record["sides"]["shadow"]["refusal"] is None
    assert record["differing_inputs"] == []


def test_a_run_that_recorded_no_inputs_is_refused_by_name():
    pins = parse_refs([])
    why = rl._pin_problems(pins, DAY, compute._ARCTICDB_LOOKBACK_DAYS)
    assert "records no inputs" in why


def test_a_run_that_read_a_different_window_is_a_code_difference():
    refs = [
        {"key": arctic_key(BUCKET, lib, end=D, lookback_days=730, before=D), "version": "sha256:x"}
        for lib in ("universe", "macro")
    ] + [{"key": restated_key(BUCKET, start="a", end=D), "version": "restated:"}]
    why = rl._pin_problems(parse_refs(refs), DAY, compute._ARCTICDB_LOOKBACK_DAYS)
    assert "code difference" in why
    refs = [
        {"key": arctic_key(BUCKET, lib, end=D, lookback_days=compute._ARCTICDB_LOOKBACK_DAYS, before=None), "version": "sha256:x"}
        for lib in ("universe", "macro")
    ] + [{"key": restated_key(BUCKET, start="a", end=D), "version": "restated:"}]
    why = rl._pin_problems(parse_refs(refs), DAY, compute._ARCTICDB_LOOKBACK_DAYS)
    assert "strictly before the trading day" in why


def test_a_run_that_restated_a_split_is_not_replayed():
    refs = [
        {"key": arctic_key(BUCKET, lib, end=D, lookback_days=compute._ARCTICDB_LOOKBACK_DAYS, before=D), "version": "sha256:x"}
        for lib in ("universe", "macro")
    ] + [{"key": restated_key(BUCKET, start="a", end=D), "version": "restated:NVDA"}]
    assert "restated" in rl._pin_problems(parse_refs(refs), DAY, compute._ARCTICDB_LOOKBACK_DAYS)


def test_the_as_of_reader_asks_arcticdb_for_the_recorded_moment(monkeypatch):
    """Production path: each library is read AS OF the moment the run recorded,
    so a history rewrite after the run does not change what the recompute reads."""
    import sys

    from nousergon_lib import arcticdb as lib_arctic

    requests = []

    class _Req:
        def __init__(self, **kw):
            requests.append(kw)
            self.kw = kw

    class _Handle:
        def read_batch(self, reqs):
            return [types.SimpleNamespace(data=_history(1)) for _ in reqs]

    monkeypatch.setitem(sys.modules, "arcticdb", types.SimpleNamespace(ReadRequest=_Req))
    monkeypatch.setattr(lib_arctic, "open_universe_lib", lambda bucket: _Handle())
    monkeypatch.setattr(lib_arctic, "open_macro_lib", lambda bucket: _Handle())
    monkeypatch.setattr(lib_arctic, "get_universe_symbols", lambda bucket: ["AAPL", "MSFT"])
    moment = "2026-09-23T20:17:31.000001Z"
    pins = parse_refs(
        [
            {"key": arctic_key(BUCKET, lib, end=D, lookback_days=1018, before=D, as_of=moment), "version": "sha256:x"}
            for lib in ("universe", "macro")
        ]
    )
    read = rl.as_of_library_reader(BUCKET, pins)
    out = read("universe", None, lookback_days=1018, end=pd.Timestamp(D))
    assert sorted(out) == ["AAPL", "MSFT"]
    assert {r["symbol"] for r in requests} == {"AAPL", "MSFT"}
    assert all(pd.Timestamp(r["as_of"]) == pd.Timestamp(moment) for r in requests)
    assert requests[0]["date_range"] == (pd.Timestamp(D) - pd.Timedelta(days=1018), pd.Timestamp(D))


# ---------------------------------------------------------------------------
# D. Parity grades the chain
# ---------------------------------------------------------------------------


def _grade(day, record, *, live=None, shadow=None, store_objects=None):
    key = f"features/{D}/technical.parquet"
    store = Store(store_objects if store_objects is not None else {rl.lineage_record_key(DAY): json.dumps(record).encode()})
    manifests = parity._V1CauseManifests(
        parity.S3Reader(BUCKET, day.s3),
        {u.unit_id: u for u in parity.load_units()},
        DAY,
        ROOT,
        store=store,
    )
    context = manifests.context(key, ["D31"])
    body = parity.compare_bytes(
        key,
        live if live is not None else day.published["v1"][key],
        shadow if shadow is not None else day.published["shadow"][key],
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(key),
        trading_day=DAY + dt.timedelta(days=1),
        v1_cause=context,
    )
    return body, context


def test_the_chain_grants_v1_cause_through_a_provisional_input(day, record):
    body, context = _grade(day, record)
    assert context.recompute is not None and context.recompute.chain_refusal is None
    assert body["verdict"] == "v1_cause", body.get("v1_cause_refused")
    (evidence,) = body["v1_cause"]["evidence"]
    assert evidence["kind"] == "recompute_lineage"
    (link,) = evidence["differing_inputs"]
    assert link["key"] == CLOSES_KEY
    assert link["evidence_kinds"] == ["v1_bar_provisional"]
    assert link["v1"]["writer"]["run_id"] == "v1-d19"
    assert link["shadow"]["writer"]["run_id"] == "sh-d19"


def test_the_evidence_validates_against_the_report_contract(day, record):
    import jsonschema

    schema = json.loads(
        (pathlib.Path(__file__).resolve().parents[1] / "contracts" / "data_parity_report.schema.json").read_text()
    )
    item = _evidence_item_schema(schema)
    assert "recompute_lineage" in item["properties"]["kind"]["enum"]
    body, _ = _grade(day, record)
    for evidence in body["v1_cause"]["evidence"]:
        jsonschema.validate(evidence, item)


def _evidence_item_schema(node):
    """The schema of one `v1_cause.evidence[]` entry: the one whose `kind` enumerates the kinds."""
    if isinstance(node, dict):
        kind = (node.get("properties") or {}).get("kind") or {}
        if "v1_bar_provisional" in (kind.get("enum") or []):
            return node
        for value in node.values():
            found = _evidence_item_schema(value)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _evidence_item_schema(value)
            if found is not None:
                return found
    return None


def test_an_input_that_is_not_proven_v1_caused_refuses_the_chain(day, record):
    """Both D19 writes stamped `settled` (the shadow's re-stamped as if v1 had
    also fetched late): the closes differ for a reason nothing proves, so the
    features they fed stay strict."""
    manifest_key = f"data_collection/runs/D19/{D}/v1-d19.json"
    original = day.s3.objects[manifest_key][-1][1]
    settled = json.loads(original)
    settled["guards"] = daily_closes._settlement_guards(SH_D19_FETCH, D, CLOSES_KEY)
    day.s3.put(manifest_key, json.dumps(settled).encode())
    try:
        body, context = _grade(day, record)
    finally:
        day.s3.put(manifest_key, original)
    assert body["verdict"] == "mismatch"
    assert context.recompute.chain_refusal and CLOSES_KEY in context.recompute.chain_refusal
    assert any("not v1_cause" in why for why in body["v1_cause_refused"])


def test_a_differing_arctic_library_refuses_the_chain(day, record):
    widened = json.loads(json.dumps(record))
    widened["differing_inputs"].append({"kind": "arcticdb", "key": "arcticdb/universe"})
    body, _ = _grade(day, widened)
    assert body["verdict"] == "mismatch"
    assert any("arcticdb/universe" in why and "no settlement stamp" in why for why in body["v1_cause_refused"])


def test_a_record_about_other_bytes_proves_nothing_about_these(day, record):
    key = f"features/{D}/technical.parquet"
    frame = pd.read_parquet(io.BytesIO(day.published["shadow"][key]))
    frame.loc[frame.index[0], "rsi_14"] += 1.0
    buf = io.BytesIO()
    frame.to_parquet(buf, index=False)
    body, _ = _grade(day, record, shadow=buf.getvalue())
    assert body["verdict"] == "mismatch"
    assert any("not the object this report graded" in why for why in body["v1_cause_refused"])


def test_no_record_says_why_on_the_row(day, record):
    body, context = _grade(day, record, store_objects={})
    assert context.recompute.record is None
    assert body["verdict"] == "mismatch"
    assert any("no recompute lineage" in why and "has not run" in why for why in body["v1_cause_refused"])


def test_a_refused_record_grants_nothing(day, record):
    refused = json.loads(json.dumps(record))
    refused["status"] = "refused"
    refused["sides"]["v1"]["refusal"] = "arcticdb/universe: the replayed read does not digest"
    body, _ = _grade(day, refused)
    assert body["verdict"] == "mismatch"
    assert any("not complete" in why and "arcticdb/universe" in why for why in body["v1_cause_refused"])


def test_only_feature_keys_carry_lineage(day, record):
    store = Store({rl.lineage_record_key(DAY): json.dumps(record).encode()})
    manifests = parity._V1CauseManifests(
        parity.S3Reader(BUCKET, day.s3), {u.unit_id: u for u in parity.load_units()}, DAY, ROOT, store=store
    )
    assert manifests.context(CLOSES_KEY, ["D19"]).recompute is None
    assert manifests.context(f"features/{D}/macro.parquet", ["D31"]).recompute is not None


# ---------------------------------------------------------------------------
# E. The dispatch
# ---------------------------------------------------------------------------


def _run_sameday(cmd: str, tmp_path: pathlib.Path, *, legs: int, parity_rc: int, lineage: int):
    calls = tmp_path / "calls.log"
    stub = f"""
python() {{
  if [ "$1" = "-c" ]; then
    TZ=America/New_York date +%F
    return 0
  fi
  echo "$*" >> {calls}
  case "$3" in
    run) return {legs} ;;
    parity) return {parity_rc} ;;
    recompute-lineage) return {lineage} ;;
    prune) return 0 ;;
  esac
  return 99
}}
"""
    proc = subprocess.run(["bash", "-c", stub + cmd], capture_output=True, text=True, timeout=60)
    lines = calls.read_text().splitlines() if calls.exists() else []
    return proc.returncode, [line.split()[2] for line in lines]


@pytest.mark.parametrize(
    "legs,parity_rc,lineage,expected",
    [
        (0, 0, 0, 0),
        (0, 1, 0, 1),  # NOT MET stays NOT MET
        (0, 1, 2, 2),  # a crashed recompute is a failed run
        (0, 2, 2, 2),  # parity published nothing
        (1, 1, 2, 1),  # a failed leg is the exit, and both steps still ran
    ],
)
def test_sameday_runs_the_recompute_after_parity(workloads, tmp_path, legs, parity_rc, lineage, expected):  # noqa: F811
    rc, calls = _run_sameday(workloads["shadow-sameday"], tmp_path, legs=legs, parity_rc=parity_rc, lineage=lineage)
    prune = ["prune"] if parity_rc <= 1 else []  # alpha-engine-config-I11447: only on a graded day
    assert calls == ["run", "run", "parity", "recompute-lineage", *prune]
    assert rc == expected


def test_the_recompute_publishes_its_record_to_the_parity_store(workloads):  # noqa: F811
    segment = workloads["shadow-sameday"].split("python -m shadow recompute-lineage ", 1)[1].split(";", 1)[0]
    assert "--trading-day $TD" in segment
    assert "--store s3://alpha-engine-research/data_collection" in segment
    assert "--dispatch-gate" not in segment

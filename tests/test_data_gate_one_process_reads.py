"""`alpha-engine-config-I11792` — the six gate readings share ONE process.

`data-gate.yml` used to run `python -m data_gate read` six times, once per
gate. Every invocation evaluates the whole board (`read.run`), and `S3Store`'s
read memo lives for one process, so a run re-LISTed the same prefixes and
re-downloaded the same objects six times over — the 33.5 MB
`parity_adjudication/2026-09-28/0001.json` 70 times on 2026-10-14 alone.
`python -m data_gate read --gate A --gate B ...` now takes the same readings in
one process over one store.

These tests replay both shapes against the same fake bucket and pin:

* the readings are IDENTICAL — every gate's dated `gate.json`, the ladder and
  the board, byte for byte once the wall-clock stamps are removed, and the
  exit code;
* the S3 calls are COUNTED: the adjudication record is fetched once instead of
  six times, and LISTs and GETs fall to the one-process count;
* a reading still sees what an earlier reading in the same process published
  (the ladder, the dated keys), as the next process used to.

No AWS: the bucket is a dict, and the IAM/Scheduler/Step Functions clients
refuse every call, so those columns read UNMEASURABLE in both shapes alike.
"""

from __future__ import annotations

import collections
import datetime as dt
import json
import re

import pytest

import data_gate.__main__ as main_module
from data_gate import unit_readers
from data_gate.read import BOARD_KEY, GATES
from data_gate.store import S3Store
from tests import test_parity_adjudication_i12023 as adjudication_fixture

BUCKET = "alpha-engine-research"
PREFIX = "data_collection"
TRADING_DAY = adjudication_fixture.AFTER
RECORD_KEY = f"{PREFIX}/{adjudication_fixture.RECORD_0}"

#: The order `data-gate.yml` reads them in (and used to run its steps in).
WORKFLOW_ORDER = [
    "data-phase0",
    "data-cutover-ready",
    "data-phase1",
    "data-phase2",
    "data-phase3",
    "data-collection-reliability",
]


class _ClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    """A bucket in a dict, counting every request and every byte served."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)
        self.calls: collections.Counter[str] = collections.Counter()
        self.gets_by_key: collections.Counter[str] = collections.Counter()
        self.bytes_by_key: collections.Counter[str] = collections.Counter()

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        fake = self

        class _Paginator:
            def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803 - boto3's shape
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                pages = [keys[i : i + 1000] for i in range(0, len(keys), 1000)] or [[]]
                for page in pages:
                    fake.calls["ListObjectsV2"] += 1
                    yield {"Contents": [{"Key": k} for k in page]}

        return _Paginator()

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803
        self.calls["GetObject"] += 1
        self.gets_by_key[Key] += 1
        if Key not in self.objects:
            raise _ClientError("NoSuchKey")
        self.bytes_by_key[Key] += len(self.objects[Key])
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str):  # noqa: N803
        self.calls["PutObject"] += 1
        self.objects[Key] = Body
        return {}


class _Refused:
    """IAM / Scheduler / Step Functions: every call is AccessDenied."""

    def __getattr__(self, name: str):
        def _call(*_args, **_kwargs):
            raise PermissionError(f"AccessDenied: {name}")

        return _call


def _bucket() -> _FakeS3:
    store = adjudication_fixture._store()
    return _FakeS3({f"{PREFIX}/{key}": body for key, body in store.raw.items()})


def _no_pacing(monkeypatch) -> None:
    """The identity column paces IAM simulate calls (`unit_readers._pace`,
    `SIMULATE_MIN_INTERVAL_S` between calls) through the `_sleep` seam, which
    is there so tests never sleep. Every IAM call here is refused at once, so
    the floor protects nothing and only cost ~4 s of wall clock per board
    evaluation. The pacing itself is pinned in `test_simulate_plan.py`."""
    monkeypatch.setattr(unit_readers, "_sleep", lambda _seconds: None)


@pytest.fixture(autouse=True)
def _unpaced(monkeypatch):
    _no_pacing(monkeypatch)


def _cli(monkeypatch, s3: _FakeS3, gates: list[str], *extra: str) -> int:
    """`python -m data_gate read --gate ...` against ``s3``, a fresh store per process."""

    def _open_store(uri, **kwargs):
        store = S3Store(
            BUCKET,
            PREFIX,
            client=s3,
            iam_client=_Refused(),
            scheduler_client=_Refused(),
            sfn_client=_Refused(),
            dry_run=kwargs.get("dry_run", False),
        )
        return store

    monkeypatch.setattr(main_module, "open_store", _open_store)
    monkeypatch.delenv("DATA_GATE_GITHUB_TOKEN", raising=False)
    argv = ["read"]
    for gate in gates:
        argv += ["--gate", gate]
    argv += ["--store", f"s3://{BUCKET}/{PREFIX}", "--trading-day", TRADING_DAY.isoformat(), "--trigger", "schedule"]
    return main_module.main(argv + list(extra))


#: When this module started. A timestamp at or after it is the WALL CLOCK of a
#: replay (`generated_utc`, and `data.gate.ladder_fresh` quoting the ladder's
#: own `generated_utc`), the one thing two replays a minute apart differ in.
#: Every fixture timestamp predates it and is compared as data.
_STARTED = dt.datetime.now(dt.timezone.utc).replace(microsecond=0) - dt.timedelta(minutes=1)
_ISO = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)")


def _clock(match: re.Match) -> str:
    stamp = dt.datetime.fromisoformat(match.group(0).replace("Z", "+00:00"))
    return "<wall-clock>" if stamp >= _STARTED else match.group(0)


def _scrub(value):
    if isinstance(value, dict):
        return {k: _scrub(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub(v) for v in value]
    if isinstance(value, str):
        return _ISO.sub(_clock, value)
    return value


def _published(s3: _FakeS3) -> dict[str, object]:
    keys = [f"{PREFIX}/gates/{gate}/{TRADING_DAY.isoformat()}/gate.json" for gate in WORKFLOW_ORDER]
    keys += [f"{PREFIX}/gates/ladder.json", f"{PREFIX}/{BOARD_KEY}"]
    return {key: _scrub(json.loads(s3.objects[key])) for key in keys}


@pytest.fixture(scope="module")
def replays():
    """The same run, both shapes: six processes, and one. Module-scoped: each
    replay evaluates the whole board seven times over."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        _no_pacing(monkeypatch)
        before = _bucket()
        codes = [_cli(monkeypatch, before, [gate]) for gate in WORKFLOW_ORDER]
        after = _bucket()
        code = _cli(monkeypatch, after, WORKFLOW_ORDER)
    return before, codes, after, code


def test_the_workflow_reads_every_registered_gate():
    assert sorted(WORKFLOW_ORDER) == sorted(GATES)


def test_one_process_publishes_the_same_readings_as_six(replays):
    before, codes, after, code = replays
    assert codes == [0] * len(WORKFLOW_ORDER)
    assert code == 0
    published_before, published_after = _published(before), _published(after)
    assert published_before.keys() == published_after.keys()
    for key in published_before:
        assert published_after[key] == published_before[key], key
    # The parity clause really went through the adjudication record in both.
    cutover = published_after[f"{PREFIX}/gates/data-cutover-ready/{TRADING_DAY.isoformat()}/gate.json"]
    parity = next(c for c in cutover["clauses"] if c["name"] == "data.cutover_ready.parity")
    assert "ADJUDICATED" in parity["detail"]


def test_the_adjudication_record_is_downloaded_once_per_run_not_once_per_gate(replays):
    before, _codes, after, _code = replays
    assert before.gets_by_key[RECORD_KEY] == len(WORKFLOW_ORDER)
    assert after.gets_by_key[RECORD_KEY] == 1


def test_s3_calls_fall_to_one_process_worth(replays):
    """Counted, not asserted in prose. Writes are unchanged: each reading still
    publishes its dated key, the ladder and the board."""
    before, _codes, after, _code = replays

    def _input_bytes(s3: _FakeS3) -> int:
        # What the run READS as evidence, not what it re-reads of its own
        # publications (`gates/`), which both shapes re-read alike.
        return sum(n for k, n in s3.bytes_by_key.items() if not k.startswith(f"{PREFIX}/gates/"))

    print(f"\nbefore: {dict(before.calls)} {_input_bytes(before)} evidence bytes")
    print(f"after:  {dict(after.calls)} {_input_bytes(after)} evidence bytes")
    assert after.calls["PutObject"] == before.calls["PutObject"] == 3 * len(WORKFLOW_ORDER)
    # Every listing and object is fetched once, except what a reading's own
    # writes invalidated, so the one-process count is a little over a sixth.
    assert after.calls["ListObjectsV2"] * 4 < before.calls["ListObjectsV2"]
    assert after.calls["GetObject"] * 3 < before.calls["GetObject"]
    assert _input_bytes(after) * len(WORKFLOW_ORDER) == _input_bytes(before)
    assert after.bytes_by_key[RECORD_KEY] * len(WORKFLOW_ORDER) == before.bytes_by_key[RECORD_KEY]
    # A publication is still re-read after each write that changed it (the
    # memo drops a written key), never served stale and never more often.
    assert after.gets_by_key[f"{PREFIX}/gates/ladder.json"] == len(WORKFLOW_ORDER)
    # No object is fetched more often in one process than it was across six.
    for key, count in after.gets_by_key.items():
        assert count <= before.gets_by_key[key], key


def test_a_later_reading_sees_what_an_earlier_one_published(monkeypatch):
    """The ladder the first reading writes is the one the second grades
    `data.gate.ladder_fresh` on — the memo must not serve the pre-write
    absence. Read alone, the fresh bucket has no ladder and that clause is
    UNMET; read second in the same process, it is MET, exactly as the second
    process used to see it."""
    s3 = _bucket()
    assert _cli(monkeypatch, s3, ["data-phase0", "data-cutover-ready"]) == 0
    s3_alone = _bucket()
    assert _cli(monkeypatch, s3_alone, ["data-phase0"]) == 0

    def _fresh(bucket, gate):
        reading = json.loads(bucket.objects[f"{PREFIX}/gates/{gate}/{TRADING_DAY.isoformat()}/gate.json"])
        return next(c for c in reading["clauses"] if c["name"] == "data.gate.ladder_fresh")

    assert _fresh(s3_alone, "data-phase0")["met"] is False
    assert _fresh(s3, "data-phase0")["met"] is False
    board = json.loads(s3.objects[f"{PREFIX}/{BOARD_KEY}"])
    row = next(r for r in board["rows"] if r["clause"] == "data.gate.ladder_fresh")
    assert row["state"] == "MET", row["detail"]


def test_fail_on_unmet_stops_where_the_failed_step_stopped_the_job(monkeypatch):
    """A step exiting 1 skipped every later step, so one process stops at the
    same gate: the readings before it are published, none after it is."""
    s3 = _bucket()
    code = _cli(monkeypatch, s3, WORKFLOW_ORDER, "--fail-on-unmet")
    assert code == main_module.EXIT_NOT_MET
    written = [g for g in WORKFLOW_ORDER if f"{PREFIX}/gates/{g}/{TRADING_DAY.isoformat()}/gate.json" in s3.objects]
    assert written == ["data-phase0"]


def test_a_failed_measurement_exits_two_and_stops(monkeypatch):
    calls = []

    def _run(store, *, gate, **_kwargs):
        calls.append(gate)
        raise RuntimeError("the descriptors would not load")

    monkeypatch.setattr("data_gate.read.run", _run)
    assert _cli(monkeypatch, _bucket(), WORKFLOW_ORDER) == main_module.EXIT_UNMEASURED
    assert calls == ["data-phase0"]


def test_a_gate_named_twice_is_refused(monkeypatch):
    with pytest.raises(SystemExit):
        _cli(monkeypatch, _bucket(), ["data-phase0", "data-phase0"])

"""A graded parity breach's evidence is preserved automatically, idempotently and loudly.

`data_gate.parity_evidence` (alpha-engine-config-I12023 follow-up): every object
VERSION a frozen parity report's exceptions compared, and every version an
adjudication record cites, is copied to
``parity_adjudication/{report_day}/evidence/{key}@{version}`` and recorded in a
manifest — so no hand copy is ever needed before the bucket's noncurrent-version
purge. A version that cannot be copied fails the step, naming it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib

import pytest
import yaml

from data_gate import __main__ as cli
from data_gate import evidence
from data_gate import parity_adjudication as adj
from data_gate import parity_evidence as pe
from data_gate.cutover import cutover_trading_day

CUTOVER_DAY = cutover_trading_day()
AFTER = dt.date(2026, 10, 2)
REPORT_KEY = evidence.parity_store_key(CUTOVER_DAY)
RECORD_KEY = f"parity_adjudication/{CUTOVER_DAY.isoformat()}/0001.json"
GENERATED = "2026-09-28T23:43:46Z"
NOW = dt.datetime(2026, 10, 6, 1, 30, tzinfo=dt.timezone.utc)
REPO = pathlib.Path(__file__).resolve().parents[1]


def _t(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))


class FakeVault:
    """A versioned bucket in memory: source versions by key, retained objects by store key."""

    def __init__(self, *, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.versions: dict[str, list[dict]] = {}
        self.retained: dict[str, dict] = {}
        self.copies: list[tuple[str, str, str]] = []
        self.puts: list[str] = []
        self.list_versions_calls = 0
        self.fail_copy: set[tuple[str, str]] = set()

    def add_version(self, key: str, vid: str, written: str, body: bytes = b"x") -> None:
        self.versions.setdefault(key, []).append(
            {"VersionId": vid, "LastModified": _t(written), "ETag": hashlib.md5(body).hexdigest(), "Size": len(body)}
        )

    # -- EvidenceVault --
    def list_versions(self, key):
        self.list_versions_calls += 1
        return [dict(v) for v in self.versions.get(key, [])]

    def head_version(self, key, version_id):
        for v in self.versions.get(key, []):
            if v["VersionId"] == version_id:
                return {"ETag": v["ETag"], "Size": v["Size"], "LastModified": v["LastModified"]}
        raise FileNotFoundError(f"NoSuchVersion {key}@{version_id}")

    def list_retained(self, prefix):
        return {k for k in self.retained if k.startswith(prefix)}

    def head_retained(self, key):
        obj = self.retained[key]
        return {"ETag": obj["ETag"], "Metadata": dict(obj.get("Metadata") or {})}

    def copy_version(self, source_key, version_id, dest_key, metadata):
        if (source_key, version_id) in self.fail_copy:
            raise PermissionError("AccessDenied")
        source = self.head_version(source_key, version_id)
        self.copies.append((source_key, version_id, dest_key))
        self.retained[dest_key] = {"ETag": source["ETag"], "Metadata": dict(metadata)}
        return {"ETag": source["ETag"]}

    def get_retained(self, key):
        obj = self.retained.get(key)
        return None if obj is None else obj["Body"]

    def put_retained(self, key, payload):
        self.puts.append(key)
        self.retained[key] = {"ETag": hashlib.md5(payload).hexdigest(), "Body": payload, "Metadata": {}}


class _Store:
    uri = "file://test"

    def __init__(self, documents: dict) -> None:
        self.raw = {k: json.dumps(v, sort_keys=True).encode("utf-8") for k, v in documents.items()}

    def list_keys(self, prefix: str = ""):
        return [k for k in sorted(self.raw) if k.startswith(prefix)]

    def get_bytes(self, key: str) -> bytes:
        if key not in self.raw:
            raise FileNotFoundError(key)
        return self.raw[key]


def _report() -> dict:
    """The frozen 09-28 report's shape: two JSON mismatches (one with a recorded
    manifest version), an ArcticDB in-region row, a passing row, and an unsettled
    D-1 pair on a features row."""
    return {
        "schema_version": "data_parity_report.v3",
        "bucket": "alpha-engine-research",
        "trading_day": CUTOVER_DAY.isoformat(),
        "generated_at": GENERATED,
        "met": False,
        "summary": {"total": 5, "match": 2, "mismatch": 2, "in_region_only": 1, "settling_bar_keys": 1, "v1_cause": 0},
        "keys": [
            {"key": "market_data/macro/latest.json", "verdict": "mismatch", "values": {"breaches": 4},
             "live_version": {"basis": "current_matches_manifest", "manifest_version_id": "_vDEseto"},
             "shadow_key": "staging/shadow/2026-09-28/market_data/macro/latest.json"},
            {"key": "market_data/technicals/rating_performance.json", "verdict": "mismatch", "values": {"breaches": 17},
             "shadow_key": "staging/shadow/2026-09-28/market_data/technicals/rating_performance.json"},
            {"key": "arcticdb/universe", "comparator": "arcticdb", "verdict": "in_region_only"},
            {"key": "a.json", "verdict": "match", "shadow_key": "staging/shadow/2026-09-28/a.json",
             "live_version": {"manifest_version_id": "never-copied"}},
            {"key": "features/2026-09-28/technical.parquet", "verdict": "match",
             "prior_day_settled": {"available": True, "settled": False, "breaches": 2,
                                   "key": "features/2026-09-25/technical.parquet",
                                   "shadow_key": "staging/shadow/2026-09-25/features/2026-09-25/technical.parquet"}},
            {"key": "features/2026-09-28/alternative.parquet", "verdict": "match",
             "prior_day_settled": {"available": True, "settled": True, "breaches": 0,
                                   "key": "features/2026-09-25/alternative.parquet",
                                   "shadow_key": "staging/shadow/2026-09-25/features/2026-09-25/alternative.parquet"}},
        ],
        "prior_day_settled": {"unsettled": 1, "unsettled_examples": [
            {"key": "features/2026-09-25/technical.parquet", "breaches": 2}]},
    }


def _record() -> dict:
    return {
        "schema_version": adj.ADJUDICATION_SCHEMA_VERSION,
        "supersedes": None,
        "report": {"key": REPORT_KEY, "sha256": "irrelevant-to-preservation"},
        "exceptions": [{
            "key": "features/2026-09-25/technical.parquet",
            "kind": "prior_day_unsettled",
            "inputs": [{
                "id": "AA", "kind": "bar",
                "source_key": "reference/price_cache/AA.parquet", "source_version_id": "pc-0925",
                "settled": {"source_kind": adj.SETTLED_SOURCE_V1_LATER,
                            "source_key": "reference/price_cache/AA.parquet", "source_version_id": "pc-0928"},
            }, {
                "id": "SPY", "kind": "bar",
                "source_key": "reference/price_cache/SPY.parquet", "source_version_id": "spy-0925",
                "settled": {"source_kind": adj.SETTLED_SOURCE_VENDOR, "vendor": "polygon-grouped-daily"},
            }],
        }],
    }


def _vault() -> FakeVault:
    v = FakeVault()
    v.add_version("market_data/macro/latest.json", "_vDEseto", "2026-09-28T20:13:55Z")
    v.add_version("market_data/technicals/rating_performance.json", "rp-0925", "2026-09-25T20:15:00Z")
    v.add_version("market_data/technicals/rating_performance.json", "rp-0928", "2026-09-28T20:15:44Z")
    v.add_version("market_data/technicals/rating_performance.json", "rp-collector", "2026-09-29T22:00:00Z")
    v.add_version("staging/shadow/2026-09-28/market_data/macro/latest.json", "sh-macro", "2026-09-28T22:42:00Z")
    v.add_version("staging/shadow/2026-09-28/market_data/technicals/rating_performance.json", "sh-rp", "2026-09-28T22:43:52Z")
    v.add_version("features/2026-09-25/technical.parquet", "ft-0925", "2026-09-25T20:38:00Z")
    v.add_version("staging/shadow/2026-09-25/features/2026-09-25/technical.parquet", "sh-ft", "2026-09-25T23:00:00Z")
    v.add_version("reference/price_cache/AA.parquet", "pc-0925", "2026-09-25T20:07:56Z")
    v.add_version("reference/price_cache/AA.parquet", "pc-0928", "2026-09-28T20:08:48Z")
    v.add_version("reference/price_cache/SPY.parquet", "spy-0925", "2026-09-25T20:07:56Z")
    return v


EXPECTED = {
    ("market_data/macro/latest.json", "_vDEseto"),
    ("market_data/technicals/rating_performance.json", "rp-0928"),
    ("staging/shadow/2026-09-28/market_data/macro/latest.json", "sh-macro"),
    ("staging/shadow/2026-09-28/market_data/technicals/rating_performance.json", "sh-rp"),
    ("features/2026-09-25/technical.parquet", "ft-0925"),
    ("staging/shadow/2026-09-25/features/2026-09-25/technical.parquet", "sh-ft"),
    ("reference/price_cache/AA.parquet", "pc-0925"),
    ("reference/price_cache/AA.parquet", "pc-0928"),
    ("reference/price_cache/SPY.parquet", "spy-0925"),
}


def _store(with_record: bool = True) -> _Store:
    docs = {REPORT_KEY: _report()}
    if with_record:
        docs[RECORD_KEY] = _record()
    return _Store(docs)


def _run(vault, store=None):
    return pe.preserve_graded_parity(store or _store(), trading_day=AFTER, vault=vault, now=NOW)


def _manifest(vault) -> dict:
    return json.loads(vault.retained[pe.manifest_key(CUTOVER_DAY)]["Body"])


def test_every_referenced_version_is_copied_to_the_retained_prefix_with_its_provenance():
    vault = _vault()
    result = _run(vault)
    copied = {(src, vid) for src, vid, _dest in vault.copies}
    assert copied == EXPECTED
    for src, vid, dest in vault.copies:
        assert dest == f"parity_adjudication/{CUTOVER_DAY.isoformat()}/evidence/{src}@{vid}"
        meta = vault.retained[dest]["Metadata"]
        assert meta["source-key"] == src and meta["source-version-id"] == vid
    assert len(result.copied) == len(EXPECTED) and result.manifest_written
    manifest = _manifest(vault)
    assert manifest["report_key"] == REPORT_KEY
    assert {(o["source_key"], o["source_version_id"]) for o in manifest["objects"]} == EXPECTED
    assert all(o["how"] == "copied" and o["referenced_by"] for o in manifest["objects"])


def test_the_shadow_object_resolves_to_the_version_current_when_the_report_was_generated():
    """rating_performance's live row recorded no manifest version: the version the
    report compared is the one current at `generated_at`, never a later collector write."""
    vault = _vault()
    _run(vault)
    rp = {vid for src, vid, _ in vault.copies if src == "market_data/technicals/rating_performance.json"}
    assert rp == {"rp-0928"}
    resolutions = {(r["source_key"], r["as_of_utc"]): r["version_id"] for r in _manifest(vault)["resolutions"]}
    assert resolutions[("market_data/technicals/rating_performance.json", GENERATED)] == "rp-0928"


def test_passing_rows_settled_prior_days_and_arcticdb_rows_are_not_evidence():
    vault = _vault()
    _run(vault)
    sources = {src for src, _vid, _ in vault.copies}
    assert "a.json" not in sources and "staging/shadow/2026-09-28/a.json" not in sources
    assert not any("alternative" in s or s.startswith("arcticdb/") for s in sources)


def test_a_second_run_copies_nothing_and_writes_nothing():
    vault = _vault()
    _run(vault)
    copies, puts, lists = len(vault.copies), len(vault.puts), vault.list_versions_calls
    result = _run(vault)
    assert len(vault.copies) == copies and len(vault.puts) == puts
    assert vault.list_versions_calls == lists, "as-of resolutions are reused from the manifest"
    assert result.copied == [] and len(result.already) == len(EXPECTED) and not result.manifest_written


def test_the_hand_copy_is_adopted_by_its_metadata_not_duplicated():
    vault = _vault()
    dest = pe.retained_key(CUTOVER_DAY, "reference/price_cache/AA.parquet", "pc-0925")
    vault.retained[dest] = {"ETag": "whatever", "Metadata": {
        "source-key": "reference/price_cache/AA.parquet", "source-version-id": "pc-0925",
        "source-last-modified": "2026-09-25T20:07:56+00:00"}}
    result = _run(vault)
    assert ("reference/price_cache/AA.parquet", "pc-0925") not in {(s, v) for s, v, _ in vault.copies}
    assert dest in result.adopted
    entry = next(o for o in _manifest(vault)["objects"] if o["retained_key"] == dest)
    assert entry["how"] == "adopted (metadata)"


def test_a_retained_object_that_does_not_verify_fails_loudly():
    vault = _vault()
    dest = pe.retained_key(CUTOVER_DAY, "reference/price_cache/AA.parquet", "pc-0925")
    vault.retained[dest] = {"ETag": "x", "Metadata": {"source-version-id": "something-else"}}
    with pytest.raises(pe.EvidencePreservationError) as raised:
        _run(vault)
    assert any(dest in f and "something-else" in f for f in raised.value.failures)


def test_a_version_that_cannot_be_copied_fails_loudly_after_recording_the_rest():
    vault = _vault()
    vault.fail_copy.add(("reference/price_cache/SPY.parquet", "spy-0925"))
    with pytest.raises(pe.EvidencePreservationError) as raised:
        _run(vault)
    assert any("reference/price_cache/SPY.parquet@spy-0925" in f and "AccessDenied" in f for f in raised.value.failures)
    recorded = {(o["source_key"], o["source_version_id"]) for o in _manifest(vault)["objects"]}
    assert recorded == EXPECTED - {("reference/price_cache/SPY.parquet", "spy-0925")}


def test_a_purged_version_fails_loudly_and_is_named():
    vault = _vault()
    vault.versions["reference/price_cache/AA.parquet"] = [
        v for v in vault.versions["reference/price_cache/AA.parquet"] if v["VersionId"] != "pc-0928"
    ]
    with pytest.raises(pe.EvidencePreservationError) as raised:
        _run(vault)
    assert any("reference/price_cache/AA.parquet@pc-0928" in f and "cannot be read" in f for f in raised.value.failures)


def test_a_shadow_object_with_no_version_at_report_time_fails_loudly():
    vault = _vault()
    vault.versions["staging/shadow/2026-09-28/market_data/macro/latest.json"] = []
    with pytest.raises(pe.EvidencePreservationError) as raised:
        _run(vault)
    assert any("already gone" in f for f in raised.value.failures)


def test_a_retained_copy_that_disappeared_is_recopied_from_its_source():
    vault = _vault()
    _run(vault)
    dest = pe.retained_key(CUTOVER_DAY, "reference/price_cache/AA.parquet", "pc-0925")
    del vault.retained[dest]
    result = _run(vault)
    assert result.copied == [dest]


def test_without_an_adjudication_record_the_reports_own_evidence_is_still_preserved():
    vault = _vault()
    _run(vault, _store(with_record=False))
    copied = {(s, v) for s, v, _ in vault.copies}
    assert copied == {ref for ref in EXPECTED if not ref[0].startswith("reference/price_cache/")}


def test_dry_run_copies_and_writes_nothing():
    vault = _vault()
    vault.dry_run = True
    result = _run(vault)
    assert vault.copies == [] and vault.puts == []
    assert len(result.copied) == len(EXPECTED) and result.dry_run


def test_retained_keys_can_never_be_read_as_an_adjudication_record():
    """Preserving evidence must not be able to add, change or supersede an adjudication."""
    vault = _vault()
    _run(vault)
    keys = list(vault.retained)
    assert adj.adjudication_record_keys(keys, CUTOVER_DAY) == []
    assert all(k.startswith(pe.retained_prefix(CUTOVER_DAY)) for k in keys)


def test_a_clean_report_with_no_record_preserves_nothing():
    report = _report()
    report.update(met=True, summary={"total": 1, "match": 1}, keys=[{"key": "a.json", "verdict": "match"}],
                  prior_day_settled={})
    assert _run(_vault(), _Store({REPORT_KEY: report})) is None


def test_the_cli_exits_2_and_names_the_version_when_preservation_fails(monkeypatch, capsys):
    def boom(store, *, trading_day):
        raise pe.EvidencePreservationError(["reference/price_cache/SPY.parquet@spy-0925: copy failed"])

    monkeypatch.setattr(pe, "preserve_graded_parity", boom)
    code = cli.main(["preserve-parity-evidence", "--store", "/nonexistent", "--trading-day", AFTER.isoformat()])
    assert code == cli.EXIT_UNMEASURED
    assert "spy-0925" in capsys.readouterr().err


def test_the_cli_exits_0_when_everything_is_preserved(monkeypatch, capsys):
    vault = _vault()
    real = pe.preserve_graded_parity
    monkeypatch.setattr(pe, "preserve_graded_parity",
                        lambda store, *, trading_day: real(_store(), trading_day=AFTER, vault=vault, now=NOW))
    assert cli.main(["preserve-parity-evidence", "--store", "/nonexistent", "--trading-day", AFTER.isoformat()]) == 0
    assert f"copied {len(EXPECTED)}" in capsys.readouterr().out


def test_a_local_store_is_refused_rather_than_silently_skipped(tmp_path):
    (tmp_path / "parity").mkdir()
    (tmp_path / REPORT_KEY).write_text(json.dumps(_report()))
    from data_gate.store import LocalStore

    with pytest.raises(pe.EvidencePreservationError, match="not an S3 store"):
        pe.preserve_graded_parity(LocalStore(tmp_path), trading_day=AFTER)


def test_the_s3_vault_copies_the_exact_version_with_its_provenance():
    calls = {}

    class Client:
        def copy_object(self, **kwargs):
            calls.update(kwargs)
            return {"CopyObjectResult": {"ETag": '"abc"'}}

    vault = pe.S3EvidenceVault(Client(), "alpha-engine-research", "data_collection")
    out = vault.copy_version("reference/price_cache/AA.parquet", "pc-0925",
                             pe.retained_key(CUTOVER_DAY, "reference/price_cache/AA.parquet", "pc-0925"), {"m": "1"})
    assert out == {"ETag": "abc"}
    assert calls["CopySource"] == {"Bucket": "alpha-engine-research", "Key": "reference/price_cache/AA.parquet",
                                   "VersionId": "pc-0925"}
    assert calls["Key"] == ("data_collection/parity_adjudication/" + CUTOVER_DAY.isoformat()
                            + "/evidence/reference/price_cache/AA.parquet@pc-0925")
    assert calls["MetadataDirective"] == "REPLACE"


def test_the_gate_workflow_preserves_after_every_reading_and_never_masks_a_failure():
    workflow = yaml.safe_load((REPO / ".github/workflows/data-gate.yml").read_text())
    job = next(j for j in workflow["jobs"].values() if any("data_gate read" in str(s.get("run", "")) for s in j["steps"]))
    steps = job["steps"]
    preserve_at = [i for i, s in enumerate(steps) if "preserve-parity-evidence" in str(s.get("run", ""))]
    read_at = [i for i, s in enumerate(steps) if "data_gate read" in str(s.get("run", ""))]
    assert len(preserve_at) == 1 and preserve_at[0] > max(read_at)
    step = steps[preserve_at[0]]
    assert not step.get("continue-on-error")
    assert "--dry-run" not in step["run"]

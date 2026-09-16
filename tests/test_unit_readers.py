"""Withholding tests for the four base-column readers, and the RETIRED lifecycle path.

`alpha-engine-config-I10823`. Every reader here is proved the same way: take the
evidence away and the clause is never MET — UNMET when we looked and it was
absent, UNMEASURABLE when we could not look, each naming what is missing. No
AWS and no network: every source is a fake injected onto an in-memory store.
"""

from __future__ import annotations

import copy
import json
import textwrap

import pytest
import yaml

from data_gate import clauses as clause_module
from data_gate import unit_readers
from data_gate.descriptors import DescriptorError, Unit, load_units
from data_gate.read import CONSOLE_STATE, GATES, _board_document, evaluate, load_phases
from data_gate.sources import ArtifactRegistrySource, GitHubContents
from data_gate.store import open_store

from tests.data_gate_support import TRADING_DAY, EmptyStore

READER_COLUMNS = ("observability_row", "artifact_registry", "consumers", "identity")


@pytest.fixture(scope="module")
def units():
    return load_units()


def _unit(units, unit_id: str, **overrides) -> Unit:
    base = next(u for u in units if u.unit_id == unit_id)
    raw = copy.deepcopy(base.raw)
    raw.update(overrides)
    return Unit(unit_id=base.unit_id, path=base.path, raw=raw)


class _AwsError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _registry_source(document: dict) -> ArtifactRegistrySource:
    return ArtifactRegistrySource("memory://ARTIFACT_REGISTRY.yaml", lambda: yaml.safe_dump(document).encode())


# ---------------------------------------------------------------------------
# observability_row
# ---------------------------------------------------------------------------


def _write_row(directory, name: str, **fields) -> None:
    (directory / name).write_text(yaml.safe_dump(fields), encoding="utf-8")


def test_withholding_the_observability_row_is_unmet_naming_the_file(units, tmp_path):
    unit = _unit(units, "D19")
    _write_row(tmp_path, "some-umbrella.yaml", component_id="ne-postclose-trading-pipeline", lifecycle="in-service")
    reading = unit_readers.read_observability_row(unit, rows_dir=tmp_path)
    assert not reading.met and not reading.unmeasurable
    assert f"registry.d/{unit.component_id}.yaml" in reading.detail


def test_an_own_row_matching_the_descriptor_reads_met(units, tmp_path):
    unit = _unit(units, "D19")
    _write_row(
        tmp_path,
        f"{unit.component_id}.yaml",
        component_id=unit.component_id,
        owning_repo="nousergon-data",
        lifecycle="in-service",
    )
    reading = unit_readers.read_observability_row(unit, rows_dir=tmp_path)
    assert reading.met, reading.detail


def test_a_stale_row_whose_lifecycle_disagrees_is_unmet(units, tmp_path):
    unit = _unit(units, "D19")
    _write_row(tmp_path, "x.yaml", component_id=unit.component_id, owning_repo="nousergon-data", lifecycle="disabled")
    reading = unit_readers.read_observability_row(unit, rows_dir=tmp_path)
    assert not reading.met and "lifecycle" in reading.detail


def test_an_unparseable_row_file_makes_absence_unmeasurable(units, tmp_path):
    unit = _unit(units, "D19")
    (tmp_path / "broken.yaml").write_text("component_id: [unclosed\n", encoding="utf-8")
    reading = unit_readers.read_observability_row(unit, rows_dir=tmp_path)
    assert reading.unmeasurable and not reading.met


# ---------------------------------------------------------------------------
# artifact_registry
# ---------------------------------------------------------------------------

_REGISTRY = {
    "artifacts": [
        {"artifact_id": "daily_closes_parquet", "s3_key_template": "staging/daily_closes/{trading_day}.parquet"},
    ],
    "grandfathered_paths": [{"path_prefix": "market_data/technicals/rating_history/", "reason": "per-date"}],
}


def test_no_registry_source_is_unmeasurable_never_met(units):
    reading = unit_readers.read_artifact_registry(EmptyStore(), _unit(units, "D19"))
    assert reading.unmeasurable and not reading.met


def test_a_registered_key_and_resolving_row_read_met(units):
    store = EmptyStore()
    store.artifact_registry_source = _registry_source(_REGISTRY)
    reading = unit_readers.read_artifact_registry(store, _unit(units, "D19"))
    assert reading.met, reading.detail
    assert "daily_closes_parquet" in reading.detail


def test_withholding_the_registry_row_is_unmet_naming_the_key(units):
    store = EmptyStore()
    store.artifact_registry_source = _registry_source({"artifacts": [], "grandfathered_paths": []})
    reading = unit_readers.read_artifact_registry(store, _unit(units, "D19"))
    assert not reading.met and not reading.unmeasurable
    assert "staging/daily_closes/{date}.parquet" in reading.detail
    assert "daily_closes_parquet" in reading.detail  # the unresolved registry_rows id


def test_a_grandfathered_prefix_covers_a_wildcard_key(units):
    store = EmptyStore()
    store.artifact_registry_source = _registry_source(_REGISTRY)
    unit = _unit(units, "D26", writes=["market_data/technicals/rating_history/*"], registry_rows=[])
    reading = unit_readers.read_artifact_registry(store, unit)
    assert reading.met, reading.detail


def test_a_declared_off_row_on_an_in_service_unit_is_parked_not_met(units):
    document = copy.deepcopy(_REGISTRY)
    document["artifacts"][0]["declared_off"] = {"since": "2026-09-01", "reason": "paused"}
    store = EmptyStore()
    store.artifact_registry_source = _registry_source(document)
    reading = unit_readers.read_artifact_registry(store, _unit(units, "D19"))
    assert not reading.met and "parked" in reading.detail


def test_a_denied_registry_read_is_unmeasurable_naming_the_grant(units):
    class _Client:
        def get_object(self, **_kwargs):
            raise _AwsError("AccessDenied")

    store = EmptyStore()
    store.artifact_registry_source = ArtifactRegistrySource.from_s3(
        "s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml", lambda: _Client()
    )
    reading = unit_readers.read_artifact_registry(store, _unit(units, "D19"))
    assert reading.unmeasurable and not reading.met
    assert "DataGateReadPublishedArtifactRegistry" in reading.detail


def test_open_store_attaches_the_published_registry_to_an_s3_store():
    store = open_store("s3://alpha-engine-research/data_collection", dry_run=True)
    assert store.artifact_registry_source.uri.endswith("_freshness_monitor/ARTIFACT_REGISTRY.yaml")
    assert store.github_contents is None
    local = open_store("/nonexistent-local-store", dry_run=True)
    assert local.artifact_registry_source is None


# ---------------------------------------------------------------------------
# consumers
# ---------------------------------------------------------------------------


def _opener(visible_repos: set[str], files: set[tuple[str, str]]):
    calls: list[str] = []

    def opener(url: str, headers: dict[str, str]):
        calls.append(url)
        assert headers["Authorization"].startswith("Bearer ")
        tail = url.split("/repos/nousergon/", 1)[1]
        repo, _, rest = tail.partition("/")
        if repo not in visible_repos:
            return 404, b'{"message": "Not Found"}'
        if not rest:
            return 200, b"{}"
        path = rest[len("contents/") :]
        if (repo, path) in files:
            return 200, json.dumps({"type": "file"}).encode()
        return 404, b'{"message": "Not Found"}'

    return opener, calls


def test_no_github_reader_is_unmeasurable_never_met(units):
    reading = unit_readers.read_consumers(EmptyStore(), _unit(units, "D19"))
    assert reading.unmeasurable and not reading.met
    assert "DATA_GATE_GITHUB_TOKEN" in reading.detail


def test_every_declared_reader_present_reads_met(units):
    opener, _ = _opener(
        {"crucible-executor", "crucible-dashboard"},
        {("crucible-executor", "executor/upstream_artifact_gate.py"), ("crucible-dashboard", "loaders/s3_loader.py")},
    )
    store = EmptyStore()
    store.github_contents = GitHubContents("t", opener=opener)
    reading = unit_readers.read_consumers(store, _unit(units, "D19"))
    assert reading.met, reading.detail


def test_withholding_a_reader_file_is_unmet_naming_it(units):
    opener, _ = _opener(
        {"crucible-executor", "crucible-dashboard"}, {("crucible-executor", "executor/upstream_artifact_gate.py")}
    )
    store = EmptyStore()
    store.github_contents = GitHubContents("t", opener=opener)
    reading = unit_readers.read_consumers(store, _unit(units, "D19"))
    assert not reading.met and not reading.unmeasurable
    assert "crucible-dashboard:loaders/s3_loader.py" in reading.detail


def test_a_repository_the_token_cannot_see_is_unmeasurable_not_absent(units):
    opener, _ = _opener({"crucible-executor"}, {("crucible-executor", "executor/upstream_artifact_gate.py")})
    store = EmptyStore()
    store.github_contents = GitHubContents("t", opener=opener)
    reading = unit_readers.read_consumers(store, _unit(units, "D19"))
    assert reading.unmeasurable and not reading.met
    assert "not visible" in reading.detail


def test_paths_are_resolved_once_per_run(units):
    opener, calls = _opener(
        {"crucible-executor", "crucible-dashboard"},
        {("crucible-executor", "executor/upstream_artifact_gate.py"), ("crucible-dashboard", "loaders/s3_loader.py")},
    )
    github = GitHubContents("t", opener=opener)
    store = EmptyStore()
    store.github_contents = github
    unit_readers.read_consumers(store, _unit(units, "D19"))
    first = len(calls)
    unit_readers.read_consumers(store, _unit(units, "D17"))  # same two consumers
    assert len(calls) == first


def test_declared_empty_consumers_is_a_finding_not_met(units):
    reading = unit_readers.read_consumers(EmptyStore(), _unit(units, "D02"))
    assert not reading.met and "consumers: []" in reading.detail


def test_a_self_repo_consumer_is_checked_in_the_tree(units):
    unit = _unit(units, "D35", consumers=["nousergon-data:features/does_not_exist.py"])
    reading = unit_readers.read_consumers(EmptyStore(), unit)
    assert not reading.met and not reading.unmeasurable
    assert "features/does_not_exist.py" in reading.detail


# ---------------------------------------------------------------------------
# identity
# ---------------------------------------------------------------------------


class _Simulator:
    """Allows PutObject/DeleteObject on resources under ``allowed_prefixes``."""

    def __init__(self, allowed_prefixes: tuple[str, ...], error: str | None = None) -> None:
        self.allowed_prefixes = allowed_prefixes
        self.error = error
        self.calls: list[dict] = []

    def simulate_principal_policy(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise _AwsError(self.error)
        results = []
        for arn in kwargs["ResourceArns"]:
            key = arn.split(":::alpha-engine-research/", 1)[1]
            allowed = any(key.startswith(p) for p in self.allowed_prefixes)
            results.append({"EvalResourceName": arn, "EvalDecision": "allowed" if allowed else "implicitDeny"})
        return {"EvaluationResults": results, "IsTruncated": False}


def test_no_iam_client_is_unmeasurable_never_met(units):
    reading = unit_readers.read_identity(EmptyStore(), _unit(units, "D19"))
    assert reading.unmeasurable and not reading.met


def test_a_role_scoped_to_the_declared_prefixes_reads_met(units):
    store = EmptyStore()
    store.iam_client = _Simulator(("staging/",))
    reading = unit_readers.read_identity(store, _unit(units, "D19"))
    assert reading.met, reading.detail
    assert all(c["PolicySourceArn"].endswith("role/nousergon-data-collection-box-role") for c in store.iam_client.calls)


def test_withholding_the_grant_on_a_declared_key_is_unmet_naming_it(units):
    store = EmptyStore()
    store.iam_client = _Simulator(("market_data/",))
    reading = unit_readers.read_identity(store, _unit(units, "D19"))
    assert not reading.met and "staging/daily_closes/gate-probe.parquet" in reading.detail


def test_a_whole_bucket_wildcard_is_unmet(units):
    store = EmptyStore()
    store.iam_client = _Simulator(("",))
    reading = unit_readers.read_identity(store, _unit(units, "D19"))
    assert not reading.met
    assert "whole-bucket wildcard" in reading.detail and "bucket-wide Delete" in reading.detail


def test_a_denied_simulation_is_unmeasurable_naming_the_grant(units):
    store = EmptyStore()
    store.iam_client = _Simulator((), error="AccessDenied")
    reading = unit_readers.read_identity(store, _unit(units, "D19"))
    assert reading.unmeasurable and not reading.met
    assert "iam:SimulatePrincipalPolicy" in reading.detail and "DataGateSimulateWriterIdentities" in reading.detail


def test_an_undeclared_writer_identity_is_unmet_naming_the_config(units):
    store = EmptyStore()
    store.iam_client = _Simulator(("",))
    reading = unit_readers.read_identity(store, _unit(units, "D39"))  # runs_on: github-hosted
    assert not reading.met and not reading.unmeasurable
    assert "writer_identities.yaml" in reading.detail


# ---------------------------------------------------------------------------
# The board: no stubs left for these columns.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def board(units):
    return clause_module.generate(EmptyStore(), units, load_phases(), trading_day=TRADING_DAY)


def test_no_reader_column_renders_the_phase0_stub(board):
    for clause in board:
        if clause.name.rsplit(".", 1)[-1] in READER_COLUMNS:
            assert "no reader is built" not in clause.detail, clause.name


# ---------------------------------------------------------------------------
# RETIRED — deliverable 6.
# ---------------------------------------------------------------------------


def _write_descriptor(tmp_path, units, **overrides):
    raw = copy.deepcopy(next(u for u in units if u.unit_id == "D15L").raw)
    raw.update(overrides)
    for key in [k for k, v in overrides.items() if v is None]:
        raw.pop(key)
    (tmp_path / "D15L-collector-lambda.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")


def test_a_retirement_without_a_reason_is_refused(tmp_path, units):
    _write_descriptor(tmp_path, units, retirement=None)
    with pytest.raises(DescriptorError, match="retirement"):
        load_units(tmp_path)
    _write_descriptor(tmp_path, units, retirement={"ruling": "plan §8.1", "reason": "  "})
    with pytest.raises(DescriptorError, match="reason"):
        load_units(tmp_path)


def test_a_retired_unit_renders_every_clause_retired_with_its_reason(board):
    d15l = [c for c in board if c.name.startswith("data.D15L.")]
    assert len(d15l) >= 9
    assert all(clause_module.is_retired(c) for c in d15l)
    assert all(not c.met and not c.unmeasurable for c in d15l)
    assert all("RETIRED: plan §8.1 EXCLUSION: RETIRE" in c.detail for c in d15l)


def test_retired_clauses_are_in_no_gate_and_no_denominator(board):
    retired = {c.name for c in board if clause_module.is_retired(c)}
    assert retired, "D15L/D40/D41 are declared retired"
    for gate in GATES:
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
        assert not retired & {c.name for c in result.clauses}, gate


def test_the_board_renders_retired_as_its_own_console_state(board):
    document = _board_document(board, trading_day=TRADING_DAY, generated_utc="2026-09-15T00:00:00Z", store_uri=None)
    retired_rows = [r for r in document["rows"] if r["state"] == "RETIRED"]
    assert retired_rows and all(r["console_state"] == "RETIRED" for r in retired_rows)
    assert document["clauses_retired"] == len(retired_rows)
    assert document["clauses_total"] == document["clauses_met"] + document["clauses_unmet"] + document["transparency_gap"]
    assert len(document["rows"]) == (
        document["clauses_total"] + document["clauses_retired"] + document["clauses_unconnected"]
    )
    assert CONSOLE_STATE["RETIRED"] not in {"HEALTHY", "DEGRADED", "UNREPORTED"}


def test_a_non_retired_lifecycle_is_still_graded(board):
    # D33 is `disabled` and D39 `pending`: declared, but not retired, so graded.
    for unit_id in ("D33", "D39"):
        assert not any(clause_module.is_retired(c) for c in board if c.name.startswith(f"data.{unit_id}."))


def test_the_retired_detail_is_a_single_line(units):
    unit = next(u for u in units if u.unit_id == "D15L")
    assert "\n" not in unit.retirement_summary
    assert textwrap.dedent(unit.retirement_summary) == unit.retirement_summary

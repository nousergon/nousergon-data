"""The shadow run's two properties: it cannot write live, and its diff is honest.

`alpha-engine-config-I10778`, plan `data_collection_plan_260914.md` §6.2 step 4.

The load-bearing test in this file is
``test_no_s3_write_operation_can_escape_the_shadow_root``. It does not check the
operations we happened to think of — it enumerates **every operation in
botocore's own S3 service model** and asserts that each one either rewrites its
key into the shadow root or raises. That is the no-double-write invariant stated
over the whole API surface rather than over a list somebody maintains, which is
the difference between "we handled the writes we know about" and "a write
outside the shadow root is not reachable".
"""

from __future__ import annotations

import datetime as dt
import io
import json
import pathlib

import pytest

from data_gate.descriptors import load_units
from shadow import interceptor, parity
from shadow.root import ShadowGuardViolation, ShadowRoot, activate, active_root, deactivate

TRADING_DAY = dt.date(2026, 9, 12)
ROOT = ShadowRoot(TRADING_DAY)
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture
def shadow_active():
    activate(ROOT)
    try:
        yield ROOT
    finally:
        deactivate()


# ---------------------------------------------------------------------------
# The output-root override
# ---------------------------------------------------------------------------


def test_prefix_is_exactly_the_plan_s_output_root():
    assert ROOT.prefix == "staging/shadow/2026-09-12/"


def test_key_rewrite_is_idempotent_and_reversible():
    shadowed = ROOT.key("market_data/eod_closes/2026-09-12.json")
    assert shadowed == "staging/shadow/2026-09-12/market_data/eod_closes/2026-09-12.json"
    assert ROOT.key(shadowed) == shadowed
    assert ROOT.live_key(shadowed) == "market_data/eod_closes/2026-09-12.json"


def test_an_empty_key_raises_rather_than_producing_the_prefix_itself():
    with pytest.raises(ShadowGuardViolation):
        ROOT.key("")


def test_no_root_active_means_the_production_path_is_untouched():
    assert active_root() is None
    assert not interceptor.installed()


def _s3_operations() -> list[str]:
    import botocore.session

    model = botocore.session.get_session().get_service_model("s3")
    return list(model.operation_names)


def _operation_takes_a_key(operation: str) -> bool:
    import botocore.session

    model = botocore.session.get_session().get_service_model("s3")
    shape = model.operation_model(operation).input_shape
    return bool(shape is not None and "Key" in getattr(shape, "members", {}))


def test_no_s3_write_operation_can_escape_the_shadow_root(shadow_active):
    """Every keyed S3 operation is rewritten into the shadow root, or refused.

    The invariant, over botocore's whole S3 model rather than over a list.
    """
    escaped: list[str] = []
    for operation in _s3_operations():
        if not _operation_takes_a_key(operation):
            continue
        params = {"Bucket": "alpha-engine-research", "Key": "market_data/technicals/latest.json"}
        try:
            # A fresh ledger per operation: this grades each operation's
            # classification alone, not read-your-writes across the loop (I10891).
            result = interceptor.rewrite_params(
                operation, params, service="s3", root=ROOT, ledger=interceptor.RunLedger()
            )
        except ShadowGuardViolation:
            continue  # refused: the safe outcome
        if operation in interceptor.READ_OPERATIONS:
            assert result["Key"] == params["Key"], f"{operation} is a read; it must not be rewritten"
            continue
        if not str(result.get("Key", "")).startswith(ROOT.prefix):
            escaped.append(operation)
    assert not escaped, (
        "these S3 operations carry a Key and were neither refused nor rewritten into the "
        f"shadow root: {escaped}"
    )


def test_an_unclassified_s3_operation_is_refused_not_passed_through(shadow_active):
    with pytest.raises(ShadowGuardViolation, match="not classified"):
        interceptor.rewrite_params(
            "PutObjectLegalHold",
            {"Bucket": "b", "Key": "market_data/x.json"},
            service="s3",
            root=ROOT,
        )


def test_bulk_delete_rewrites_every_key(shadow_active):
    params = {"Bucket": "b", "Delete": {"Objects": [{"Key": "a.json"}, {"Key": "b/c.json"}]}}
    result = interceptor.rewrite_params("DeleteObjects", params, service="s3", root=ROOT)
    assert [o["Key"] for o in result["Delete"]["Objects"]] == [
        "staging/shadow/2026-09-12/a.json",
        "staging/shadow/2026-09-12/b/c.json",
    ]


def test_reads_pass_through_untouched(shadow_active):
    params = {"Bucket": "b", "Key": "market_data/eod_closes/latest.json"}
    assert interceptor.rewrite_params("GetObject", params, service="s3", root=ROOT) == params


def test_outbound_notification_is_refused(shadow_active):
    with pytest.raises(ShadowGuardViolation, match="outbound"):
        interceptor.rewrite_params(
            "Publish", {"TopicArn": "arn:…", "Message": "x"}, service="sns", root=ROOT
        )


def test_a_client_created_before_activation_is_still_redirected():
    """Totality does not depend on import order.

    The patch is on ``BaseClient._make_api_call``, so a client that already
    existed when the root was activated is redirected too — which is what
    makes "activate first, then run anything" a guarantee rather than a
    convention.
    """
    import boto3

    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")
    seen: list[dict] = []
    activate(ROOT)
    try:
        interceptor._ORIGINAL = lambda self, op, params: seen.append({"op": op, **params}) or {}
        client.put_object(Bucket="alpha-engine-research", Key="market_data/technicals/latest.json", Body=b"{}")
    finally:
        deactivate()
    assert seen and seen[0]["Key"] == "staging/shadow/2026-09-12/market_data/technicals/latest.json"


def test_a_run_manifest_still_writes_under_the_shadow_root(shadow_active):
    """A shadow run keeps writing its run manifest — into the shadow (plan §2 row 7)."""
    manifest_key = "data_collection/runs/D17/2026-09-12/01J.json"
    result = interceptor.rewrite_params(
        "PutObject", {"Bucket": "alpha-engine-research", "Key": manifest_key}, service="s3", root=ROOT
    )
    assert result["Key"] == f"staging/shadow/2026-09-12/{manifest_key}"


# ---------------------------------------------------------------------------
# ArcticDB: the surface the interceptor cannot see
# ---------------------------------------------------------------------------


def test_arctic_libraries_are_redirected_and_never_a_live_name():
    for live in ("universe", "macro", "delisted_history", "universe_schema_meta"):
        shadowed = ROOT.arctic_library(live)
        assert shadowed == f"shadow_20260912_{live}"
        assert shadowed not in ("universe", "macro", "delisted_history", "universe_schema_meta")
        assert ROOT.arctic_library(shadowed) == shadowed


def test_arctic_store_routes_every_library_open_through_the_shadow_helper():
    """The ArcticDB half of the invariant, asserted against the module itself.

    ``store/arctic_store.py`` is the single ArcticDB writer
    (`registry.d/writer_inventory.yaml`), so every ``get_library`` call in it
    must pass through ``shadow_arctic_library`` — otherwise a shadow run
    appends to the live universe.
    """
    import ast

    source = (REPO_ROOT / "store" / "arctic_store.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    opens = {"get_library", "open_universe_lib", "open_macro_lib", "open_preliminary_lib"}
    offenders: list[str] = []
    for function in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        for node in ast.walk(function):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name in opens and function.name != "_open_library":
                offenders.append(f"{function.name}:{node.lineno} ({name})")
    assert not offenders, (
        "every ArcticDB library open in store/arctic_store.py must go through "
        "`_open_library`, which applies shadow_arctic_library — otherwise a shadow run "
        f"appends to the LIVE library: {offenders}"
    )
    assert "shadow_arctic_library" in source, (
        "_open_library must apply shadow_arctic_library; without it the chokepoint is a "
        "chokepoint over nothing"
    )


# ---------------------------------------------------------------------------
# The parity diff
# ---------------------------------------------------------------------------


def test_every_declared_write_of_a_live_unit_produces_a_row_or_a_named_exclusion():
    """Nothing is silently dropped — the report's denominator is auditable."""
    units = load_units()
    targets = parity.expand_writes(units, TRADING_DAY)
    # Compared on (unit, kind, value) rather than on the descriptor's raw text:
    # two units can declare the SAME target in different words (D13
    # "arcticdb/universe (library)" and D35 "arcticdb/universe (benchmark proxy
    # series)"), which is one comparison attributed to both — not a gap.
    covered = {
        (unit_id, target.kind, target.value or target.declared)
        for target in targets
        for unit_id in target.unit_id.split(",")
    }
    missing = []
    for unit in units:
        if str(unit.raw.get("lifecycle")) not in parity.LIVE_LIFECYCLES:
            continue
        if unit.unit_id in parity.EXCLUDED_UNITS:
            continue
        for write in unit.raw.get("writes") or []:
            target = parity.classify_write(unit.unit_id, str(write), TRADING_DAY)
            if (unit.unit_id, target.kind, target.value or target.declared) not in covered:
                missing.append((unit.unit_id, str(write)))
    assert not missing, f"declared writes with no parity row: {sorted(missing)}"


def test_arcticdb_writes_are_classified_in_region_only_not_skipped():
    units = load_units()
    targets = parity.expand_writes(units, TRADING_DAY)
    arctic = [t for t in targets if t.kind == "arcticdb"]
    assert arctic, "the descriptors declare ArcticDB libraries; they must appear as rows"
    assert {t.value for t in arctic} >= {"universe", "macro"}


# ---------------------------------------------------------------------------
# D04/D08/D14: prose write targets corrected to resolvable keys (I10820)
# ---------------------------------------------------------------------------
#
# nousergon-data-PR1727 replaced these three descriptors' `writes` prose with
# concrete key/prefix/library patterns. The tests below prove the parity tool
# itself resolves them — not just that the YAML looks like a key — since a
# corrected descriptor the resolver still can't parse would grade
# `unmeasurable` exactly as before, silently.


def test_d04_fred_macro_history_resolves_to_a_listable_prefix():
    """D04's `{ticker}` is one of many FRED series keys — not derivable ahead
    of time — so the corrected descriptor is a listable prefix, not a single
    key. It must not fall back to `undiffable`."""
    targets = parity.expand_writes(_one_unit("D04"), TRADING_DAY)
    assert len(targets) == 1
    target = targets[0]
    assert target.kind == "prefix"
    assert target.value == "reference/price_cache/"
    assert target.reason == ""


def test_d08_universe_returns_resolves_to_the_research_db_carrier_keys():
    """D08's `universe_returns` is a TABLE inside `research.db`, not an S3
    object — the resolvable target is the two files that carry it: the live
    pointer key and the per-day dated backup, both now concrete keys."""
    targets = {t.value: t for t in parity.expand_writes(_one_unit("D08"), TRADING_DAY)}
    assert set(targets) == {"research.db", f"backups/research_{TRADING_DAY.isoformat()}.db"}
    for target in targets.values():
        assert target.kind == "key"
        assert target.reason == ""


def test_d14_prune_delisted_tickers_resolves_both_write_targets():
    """D14 declares two outputs: an ArcticDB library/symbol reference
    (`delisted_history::{ticker}`) and a listable audit-record prefix
    (`builders/prune_audit/{trading_day}-*.json`). Neither may resolve as
    `undiffable` after PR1727's correction."""
    targets = parity.expand_writes(_one_unit("D14"), TRADING_DAY)
    assert {t.kind for t in targets} == {"arcticdb", "prefix"}
    arctic = next(t for t in targets if t.kind == "arcticdb")
    assert arctic.value == "delisted_history"
    prefix = next(t for t in targets if t.kind == "prefix")
    assert prefix.value == f"builders/prune_audit/{TRADING_DAY.isoformat()}-"


def test_delisted_history_library_symbol_reference_is_arcticdb_not_prose():
    """Class-level fix: `classify_write` recognised only the `arcticdb/<lib>`
    prefix spelling. D14's `<library>::{symbol}` spelling — a genuine
    ArcticDB library/symbol reference per `store/arctic_store.py::
    DELISTED_HISTORY_LIB` — fell through to the generic `::` prose branch and
    graded `unmeasurable` even after the descriptor was corrected. It must
    now classify as `arcticdb`, gated on the same `LIVE_ARCTIC_LIBRARIES`
    registry `shadow.root` uses to redirect shadow writes."""
    target = parity.classify_write("D14", "delisted_history::{ticker}", TRADING_DAY)
    assert target.kind == "arcticdb"
    assert target.value == "delisted_history"
    assert target.reason == ""


def test_d14_prune_audit_key_matches_once_the_writer_stamps_trading_day():
    """alpha-engine-config-I10820 second defect: `_write_audit` used to stamp
    the audit key with the SF's own wall-clock `today`, never the trading day
    being audited, so this family's manifest-scoped keys (rendered with
    `{trading_day}` per `test_d14_prune_delisted_tickers_resolves_both_write_
    targets` above) could never match a real object. Fixed at the writer
    (`builders/prune_delisted_tickers.py::prune_delisted_tickers`'s new
    `trading_day` parameter). This proves the FULL comparison — descriptor
    template + a manifest whose output key now uses that same stamp — reaches
    `match`, not `shadow_missing`."""
    member = f"builders/prune_audit/{TRADING_DAY.isoformat()}-191704Z-apply.json"
    payload = json.dumps({"trading_day": TRADING_DAY.isoformat()}).encode("utf-8")
    reader = _FakeReader(
        {
            _shadow_manifest_key("D14"): _manifest_bytes("D14", "2026-09-12T20:00:00Z", [member]),
            _v1_manifest_key("D14"): _manifest_bytes("D14", "2026-09-12T12:18:50Z", [member]),
            member: payload,
            ROOT.key(member): payload,
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D14")
    )
    matched = {row.key: row.verdict for row in report.rows if row.comparator != "arcticdb"}
    assert matched == {member: "match"}


# ---------------------------------------------------------------------------
# D03/D46: writer-template reachability fix (alpha-engine-config-I10895)
# ---------------------------------------------------------------------------
#
# D03 declared the retired `predictor/price_cache/*.parquet` tree (nothing
# writes there since the Wave 3 PR4 cutover) and D46 declared a
# `{date}.parquet` shape no writer ever produced. Both are corrected here to
# the key their writer code actually publishes; these tests prove
# `expand_writes` resolves the corrected templates rather than falling back
# to `undiffable` — a corrected descriptor the resolver still can't parse
# would silently grade the same as before.


def test_d03_prices_resolves_to_the_reference_price_cache_prefix():
    """D03's `{ticker}` is one of ~900 tickers, not derivable ahead of time —
    a listable prefix, same as D04/D34, which already declare this key and
    now dedup with it in one comparison instead of three unmeasurable rows."""
    targets = parity.expand_writes(_one_unit("D03"), TRADING_DAY)
    assert len(targets) == 1
    target = targets[0]
    assert target.kind == "prefix"
    assert target.value == "reference/price_cache/"
    assert target.reason == ""


def test_d46_insider_transactions_resolves_both_write_targets():
    """D46 declares two outputs: the run-stamped artifact (`{run_stamp}` is
    not derivable ahead of time — a listable prefix) and the concrete
    `latest.json` sidecar key. Neither may resolve as `undiffable`."""
    targets = {t.kind: t for t in parity.expand_writes(_one_unit("D46"), TRADING_DAY)}
    assert set(targets) == {"prefix", "key"}
    assert targets["prefix"].value == "data/insider_transactions/"
    assert targets["prefix"].reason == ""
    assert targets["key"].value == "data/insider_transactions/latest.json"
    assert targets["key"].reason == ""


def test_a_prose_target_still_grades_unmeasurable():
    """Withholding case: a descriptor that still declares a write in prose —
    D09's `research.db::score_performance`, a SQLite table pointer, not an
    ArcticDB library — must NOT be swept up by the D14 library/symbol fix.
    `research.db` is not in `LIVE_ARCTIC_LIBRARIES`, so this stays
    `undiffable`, distinguishing a real fix from a resolver that got looser."""
    target = parity.classify_write("D09", "research.db::score_performance", TRADING_DAY)
    assert target.kind == "undiffable"
    assert "prose" in target.reason


def _frame(symbols, close):
    import pandas as pd

    return pd.DataFrame({"symbol": list(symbols), "close_raw": list(close)})


def _parquet(frame) -> bytes:
    buf = io.BytesIO()
    frame.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


def test_parquet_comparator_matches_identical_tables():
    payload = _parquet(_frame(["AAPL", "MSFT"], [1.0, 2.0]))
    body = parity.compare_bytes("staging/x.parquet", payload, payload, rel=1e-6, absolute=1e-9)
    assert body["verdict"] == "match"
    assert body["values"]["compared_cells"] == 2


def test_parquet_comparator_catches_row_count_symbol_set_and_value_drift():
    live = _parquet(_frame(["AAPL", "MSFT"], [1.0, 2.0]))
    fewer = _parquet(_frame(["AAPL"], [1.0]))
    body = parity.compare_bytes("x.parquet", live, fewer, rel=1e-6, absolute=1e-9)
    assert body["verdict"] == "mismatch"
    assert body["row_count"] == {"live": 2, "shadow": 1}
    assert body["symbol_set"]["only_live"] == ["MSFT"]

    drifted = _parquet(_frame(["AAPL", "MSFT"], [1.0, 2.01]))
    body = parity.compare_bytes("x.parquet", live, drifted, rel=1e-6, absolute=1e-9)
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1
    assert body["values"]["examples"][0]["row"] == "MSFT"


def test_value_tolerance_is_honoured():
    live = _parquet(_frame(["AAPL"], [100.0]))
    near = _parquet(_frame(["AAPL"], [100.000001]))
    assert parity.compare_bytes("x.parquet", live, near, rel=1e-4, absolute=0.0)["verdict"] == "match"
    assert parity.compare_bytes("x.parquet", live, near, rel=1e-12, absolute=0.0)["verdict"] == "mismatch"


def test_schema_drift_is_a_mismatch_even_when_values_agree():
    import pandas as pd

    live = _parquet(pd.DataFrame({"symbol": ["AAPL"], "close_raw": [1.0]}))
    renamed = _parquet(pd.DataFrame({"symbol": ["AAPL"], "close": [1.0]}))
    body = parity.compare_bytes("x.parquet", live, renamed, rel=1e-6, absolute=1e-9)
    assert body["verdict"] == "mismatch"
    assert body["schema"]["only_live"] == ["close_raw"]


def test_json_comparator_reports_the_path_of_a_difference():
    live = json.dumps({"as_of": "2026-09-12", "rows": [{"symbol": "AAPL", "rating": 3}]}).encode()
    shadow = json.dumps({"as_of": "2026-09-12", "rows": [{"symbol": "AAPL", "rating": 4}]}).encode()
    body = parity.compare_bytes("x.json", live, shadow, rel=1e-6, absolute=1e-9)
    assert body["verdict"] == "mismatch"
    assert "rating" in body["values"]["examples"][0]


class _FakeReader:
    """An S3 the test controls, keyed exactly as the real bucket would be."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.bucket = "alpha-engine-research"

    def get(self, key: str):
        return self.objects.get(key)

    def list(self, prefix: str, limit: int):
        return sorted(k for k in self.objects if k.startswith(prefix))[:limit]


def _one_unit(unit_id: str):
    return [u for u in load_units() if u.unit_id == unit_id]


def test_report_is_met_only_when_every_row_matched():
    payload = _parquet(_frame(["AAPL"], [1.0]))
    live_key = "staging/daily_closes/2026-09-12.parquet"
    reader = _FakeReader({live_key: payload, ROOT.key(live_key): payload})
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=reader,
        units=_one_unit("D17"),
        now=dt.datetime(2026, 9, 12, 23, 0, tzinfo=dt.timezone.utc),
    )
    assert report.met is True
    assert report.summary["match"] == 1

    reader = _FakeReader({live_key: payload})
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=reader,
        units=_one_unit("D17"),
    )
    assert report.met is False
    assert report.summary["shadow_missing"] == 1


def test_an_unmeasurable_row_can_never_be_met():
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=_FakeReader({}),
        units=_one_unit("D18"),  # arcticdb/universe — in-region only
    )
    assert report.met is False
    assert report.summary["in_region_only"] == 1


def test_an_empty_report_is_never_met():
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="b", reader=_FakeReader({}), units=[]
    )
    assert report.rows == []
    assert report.met is False


def test_a_report_carrying_breach_examples_still_serialises():
    """numpy scalars in the breach examples must not blow up at publish time."""
    live_key = "staging/daily_closes/2026-09-12.parquet"
    reader = _FakeReader(
        {
            live_key: _parquet(_frame(["AAPL"], [1.0])),
            ROOT.key(live_key): _parquet(_frame(["AAPL"], [1.5])),
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=reader,
        units=_one_unit("D17"),
    )
    assert report.summary["mismatch"] == 1
    json.dumps(report.as_dict())  # the publish path; must not raise


def test_report_conforms_to_its_published_schema():
    import jsonschema

    payload = _parquet(_frame(["AAPL"], [1.0]))
    live_key = "staging/daily_closes/2026-09-12.parquet"
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=_FakeReader({live_key: payload, ROOT.key(live_key): payload}),
        units=_one_unit("D17") + _one_unit("D18"),
    )
    schema = json.loads(
        (REPO_ROOT / "contracts" / "data_parity_report.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(report.as_dict(), schema)


def test_the_gate_reads_the_key_this_tool_publishes():
    """Producer/consumer contract: one spelling of the key, imported not retyped."""
    from data_gate import evidence

    assert evidence.parity_store_key(TRADING_DAY) == parity.parity_key(TRADING_DAY)
    assert parity.parity_key(TRADING_DAY) == "parity/2026-09-12.json"


# ---------------------------------------------------------------------------
# I10890: parity scoped to the units the shadow run actually executed
# ---------------------------------------------------------------------------


def _manifest_bytes(unit_id: str, finished: str, output_keys: Iterable[str]) -> bytes:
    return json.dumps(
        {
            "schema_version": "data_run_manifest.v1",
            "unit_id": unit_id,
            "finished": finished,
            "outputs": [{"key": k, "rows_out": 1} for k in output_keys],
        }
    ).encode("utf-8")


def _shadow_manifest_key(unit_id: str, trading_day: dt.date = TRADING_DAY, run_id: str = "run1") -> str:
    return ROOT.key(f"data_collection/runs/{unit_id}/{trading_day.isoformat()}/{run_id}.json")


def _v1_manifest_key(unit_id: str, trading_day: dt.date = TRADING_DAY, run_id: str = "run1") -> str:
    return f"data_collection/runs/{unit_id}/{trading_day.isoformat()}/{run_id}.json"


def test_a_unit_absent_from_the_run_manifests_lands_in_out_of_run_scope_not_shadow_missing():
    """A weekday shadow run that only ran D17 must not grade D18 at all — not
    as `in_region_only`, not as anything a real shadow run could not fix."""
    payload = _parquet(_frame(["AAPL"], [1.0]))
    live_key = "staging/daily_closes/2026-09-12.parquet"
    reader = _FakeReader(
        {
            live_key: payload,
            ROOT.key(live_key): payload,
            _shadow_manifest_key("D17"): _manifest_bytes("D17", "2026-09-12T20:00:00Z", [live_key]),
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=reader,
        units=_one_unit("D17") + _one_unit("D18"),
    )
    assert [row.key for row in report.rows] == [live_key]
    assert report.summary["in_region_only"] == 0
    excluded = {e["unit_id"]: e for e in report.excluded}
    assert excluded["D18"]["class"] == "out_of_run_scope"
    assert "D18" not in {uid for row in report.rows for uid in row.unit_ids}


def test_a_unit_present_in_the_manifests_is_still_graded():
    payload = _parquet(_frame(["AAPL"], [1.0]))
    live_key = "staging/daily_closes/2026-09-12.parquet"
    reader = _FakeReader(
        {
            live_key: payload,
            ROOT.key(live_key): payload,
            _shadow_manifest_key("D17"): _manifest_bytes("D17", "2026-09-12T20:00:00Z", [live_key]),
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D17")
    )
    assert report.summary["match"] == 1
    assert not any(e["unit_id"] == "D17" for e in report.excluded)


def test_a_prefix_family_compares_only_the_trading_days_manifest_keys():
    """D04 (a `prefix` target) must never be diffed by listing the live
    prefix — only the keys the run manifests declare for THIS trading day."""
    member = "reference/price_cache/AAPL.parquet"
    stray = "reference/price_cache/ZZZZ_stale_from_another_day.parquet"
    payload = _parquet(_frame(["AAPL"], [1.0]))
    reader = _FakeReader(
        {
            member: payload,
            ROOT.key(member): payload,
            stray: b"stale bytes nobody's manifest declares",
            _shadow_manifest_key("D04"): _manifest_bytes("D04", "2026-09-12T20:00:00Z", [member]),
            _v1_manifest_key("D04"): _manifest_bytes("D04", "2026-09-12T19:00:00Z", [member]),
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D04")
    )
    assert [row.key for row in report.rows] == [member]
    assert report.summary["match"] == 1
    assert not any("max-keys-per-prefix" in (row.body.get("unmeasurable_reason") or "") for row in report.rows)


def test_a_51_object_historical_prefix_does_not_produce_unmeasurable():
    member = "reference/price_cache/AAPL.parquet"
    payload = _parquet(_frame(["AAPL"], [1.0]))
    objects = {member: payload, ROOT.key(member): payload}
    for i in range(51):
        objects[f"reference/price_cache/historical_{i:03d}.parquet"] = b"not for this trading day"
    objects[_shadow_manifest_key("D04")] = _manifest_bytes("D04", "2026-09-12T20:00:00Z", [member])
    objects[_v1_manifest_key("D04")] = _manifest_bytes("D04", "2026-09-12T19:00:00Z", [member])
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=_FakeReader(objects),
        units=_one_unit("D04"),
        max_keys_per_prefix=50,
    )
    assert report.summary["unmeasurable"] == 0
    assert [row.key for row in report.rows] == [member]


def test_a_prefix_family_with_no_v1_manifest_for_the_trading_day_is_unmeasurable():
    member = "reference/price_cache/AAPL.parquet"
    payload = _parquet(_frame(["AAPL"], [1.0]))
    reader = _FakeReader(
        {
            member: payload,
            ROOT.key(member): payload,
            _shadow_manifest_key("D04"): _manifest_bytes("D04", "2026-09-12T20:00:00Z", [member]),
            # no v1 manifest under data_collection/runs/D04/2026-09-12/ at all
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D04")
    )
    assert report.summary["unmeasurable"] == 1
    assert "no v1 manifest for trading day" in report.rows[0].body["unmeasurable_reason"]


def test_no_manifest_evidence_anywhere_falls_back_to_the_old_full_listing():
    """An empty manifest listing (a unit list handed in without ever running
    `shadow run`) must not silently exclude everything — it grades as
    before I10890, the more conservative of the two behaviours."""
    payload = _parquet(_frame(["AAPL"], [1.0]))
    live_key = "staging/daily_closes/2026-09-12.parquet"
    reader = _FakeReader({live_key: payload, ROOT.key(live_key): payload})
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D17")
    )
    assert report.summary["match"] == 1
    assert not any(e["class"] == "out_of_run_scope" for e in report.excluded)


def test_the_shadow_prefix_accumulating_across_attempts_keeps_only_the_latest_manifest():
    """I10890 gotcha: four shadow attempts left four D17 manifests on one day.
    The stale attempt's own declared output must not appear as a candidate."""
    current = "staging/daily_closes/2026-09-12.parquet"
    stale = "staging/daily_closes/2026-09-12-attempt1-only.parquet"
    payload = _parquet(_frame(["AAPL"], [1.0]))
    reader = _FakeReader(
        {
            current: payload,
            ROOT.key(current): payload,
            _shadow_manifest_key("D17", run_id="attempt1"): _manifest_bytes(
                "D17", "2026-09-12T14:50:00Z", [stale]
            ),
            _shadow_manifest_key("D17", run_id="attempt4"): _manifest_bytes(
                "D17", "2026-09-12T19:17:00Z", [current]
            ),
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D17")
    )
    assert [row.key for row in report.rows] == [current]


# ---------------------------------------------------------------------------
# I10894: provenance fields (revision, fetched_at) are declared, not counted
# as data breaches
# ---------------------------------------------------------------------------


def test_resolve_contract_reads_the_declared_key_pattern_and_provenance_fields():
    contract = parity.resolve_contract("staging/daily_closes/2026-09-12.parquet")
    assert contract is not None
    assert contract.provenance_fields == frozenset({"revision"})

    contract = parity.resolve_contract("market_data/weekly/2026-09-12/constituents.json")
    assert contract is not None
    assert contract.provenance_fields == frozenset({"fetched_at"})

    assert parity.resolve_contract("no/such/key.json") is None


def test_a_parquet_pair_differing_only_in_revision_is_match_under_provenance_diffs():
    import pandas as pd

    live = pd.DataFrame({"ticker": ["AAPL"], "Close": [100.0], "revision": [2]})
    shadow = pd.DataFrame({"ticker": ["AAPL"], "Close": [100.0], "revision": [3]})
    key = "staging/daily_closes/2026-09-12.parquet"
    contract = parity.resolve_contract(key)
    body = parity.compare_bytes(
        key, _parquet(live), _parquet(shadow), rel=1e-6, absolute=1e-9, contract=contract
    )
    assert body["verdict"] == "match"
    assert body["values"]["breaches"] == 0
    assert body["provenance_diffs"]["count"] == 1
    assert body["provenance_diffs"]["examples"][0]["column"] == "revision"


def test_a_parquet_pair_differing_in_close_still_mismatches_with_a_contract_present():
    import pandas as pd

    live = pd.DataFrame({"ticker": ["AAPL"], "Close": [100.0], "revision": [2]})
    shadow = pd.DataFrame({"ticker": ["AAPL"], "Close": [101.0], "revision": [2]})
    key = "staging/daily_closes/2026-09-12.parquet"
    contract = parity.resolve_contract(key)
    body = parity.compare_bytes(
        key, _parquet(live), _parquet(shadow), rel=1e-6, absolute=1e-9, contract=contract
    )
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1
    assert body["values"]["examples"][0]["column"] == "Close"
    assert body["provenance_diffs"]["count"] == 0


def test_a_schemaless_key_still_red_defaults_every_field_as_data():
    """No contract resolved -> `revision` is not exempted from anything."""
    import pandas as pd

    live = pd.DataFrame({"ticker": ["AAPL"], "revision": [2]})
    shadow = pd.DataFrame({"ticker": ["AAPL"], "revision": [3]})
    body = parity.compare_bytes(
        "no/contract/for/this.parquet", _parquet(live), _parquet(shadow), rel=1e-6, absolute=1e-9
    )
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1
    assert body["provenance_diffs"]["count"] == 0


def test_a_json_pair_differing_only_in_fetched_at_is_match_under_provenance_diffs():
    live = json.dumps({"date": "2026-09-12", "fetched_at": "12:18:49Z", "sp500_count": 503}).encode()
    shadow = json.dumps({"date": "2026-09-12", "fetched_at": "19:17:03Z", "sp500_count": 503}).encode()
    key = "market_data/weekly/2026-09-12/constituents.json"
    contract = parity.resolve_contract(key)
    body = parity.compare_bytes(key, live, shadow, rel=1e-6, absolute=1e-9, contract=contract)
    assert body["verdict"] == "match"
    assert body["values"]["breaches"] == 0
    assert body["provenance_diffs"]["count"] == 1
    assert "fetched_at" in body["provenance_diffs"]["examples"][0]


def test_a_json_pair_differing_in_a_data_field_still_mismatches_with_a_contract_present():
    live = json.dumps({"date": "2026-09-12", "fetched_at": "12:18:49Z", "sp500_count": 503}).encode()
    shadow = json.dumps({"date": "2026-09-12", "fetched_at": "12:18:49Z", "sp500_count": 502}).encode()
    key = "market_data/weekly/2026-09-12/constituents.json"
    contract = parity.resolve_contract(key)
    body = parity.compare_bytes(key, live, shadow, rel=1e-6, absolute=1e-9, contract=contract)
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1
    assert body["provenance_diffs"]["count"] == 0


# ---------------------------------------------------------------------------
# I10892: grade the live object v1's manifest recorded for the trading day
# ---------------------------------------------------------------------------


class _VersionedReader(_FakeReader):
    """A versioned bucket: `objects` is the CURRENT object per key, `versions`
    every retained (key, version_id) -> (etag, bytes)."""

    def __init__(self, objects, etags, versions=None):
        super().__init__(objects)
        self.etags = etags
        self.versions = versions or {}
        self.version_gets: list[tuple[str, str]] = []

    def get_with_meta(self, key, version_id=None):
        if version_id is not None:
            self.version_gets.append((key, version_id))
            hit = self.versions.get((key, version_id))
            return None if hit is None else {"body": hit[1], "etag": hit[0], "version_id": version_id}
        if key not in self.objects:
            return None
        return {"body": self.objects[key], "etag": self.etags.get(key), "version_id": "current"}


_CLOSE_KEY = "staging/daily_closes/2026-09-12.parquet"


def _v1_manifest_with_version(etag, version_id, key=_CLOSE_KEY, capture="head_object"):
    record = {"key": key, "rows_out": 1, "etag": etag, "version_id": version_id}
    if capture is not None:
        record["version_capture"] = capture
    return json.dumps(
        {"schema_version": "data_run_manifest.v1", "unit_id": "D17", "finished": "2026-09-12T20:10:00Z", "outputs": [record]}
    ).encode("utf-8")


def _run_d17(reader):
    return parity.run_parity(
        trading_day=TRADING_DAY, bucket="alpha-engine-research", reader=reader, units=_one_unit("D17")
    )


def test_current_etag_matching_the_manifest_grades_the_current_object():
    payload = _parquet(_frame(["AAPL"], [1.0]))
    reader = _VersionedReader(
        {_CLOSE_KEY: payload, ROOT.key(_CLOSE_KEY): payload, _v1_manifest_key("D17"): _v1_manifest_with_version("e-day", "v-day")},
        {_CLOSE_KEY: "e-day"},
    )
    report = _run_d17(reader)
    row = report.rows[0]
    assert row.verdict == "match"
    assert row.body["live_version"]["basis"] == "current_matches_manifest"
    assert reader.version_gets == []


def test_a_superseded_live_key_with_a_recorded_version_id_is_graded_against_that_version():
    """The 09-15 shape: v1's D+1 run rewrote the key; the day's version is retained."""
    day_payload = _parquet(_frame(["AAPL"], [146.80]))
    next_day_payload = _parquet(_frame(["AAPL"], [150.18]))
    reader = _VersionedReader(
        {
            _CLOSE_KEY: next_day_payload,
            ROOT.key(_CLOSE_KEY): day_payload,
            _v1_manifest_key("D17"): _v1_manifest_with_version("e-day", "v-day"),
        },
        {_CLOSE_KEY: "e-next-day"},
        versions={(_CLOSE_KEY, "v-day"): ("e-day", day_payload)},
    )
    row = _run_d17(reader).rows[0]
    assert row.verdict == "match"
    assert row.body["live_version"] == {
        "basis": "manifest_version_id",
        "manifest_etag": "e-day",
        "manifest_version_id": "v-day",
        "current_etag": "e-next-day",
    }
    assert reader.version_gets == [(_CLOSE_KEY, "v-day")]


def test_a_recorded_version_still_catches_a_real_difference():
    reader = _VersionedReader(
        {
            _CLOSE_KEY: _parquet(_frame(["AAPL"], [150.18])),
            ROOT.key(_CLOSE_KEY): _parquet(_frame(["AAPL"], [146.78])),
            _v1_manifest_key("D17"): _v1_manifest_with_version("e-day", "v-day"),
        },
        {_CLOSE_KEY: "e-next-day"},
        versions={(_CLOSE_KEY, "v-day"): ("e-day", _parquet(_frame(["AAPL"], [146.80])))},
    )
    row = _run_d17(reader).rows[0]
    assert row.verdict == "mismatch"
    assert row.body["live_version"]["basis"] == "manifest_version_id"


def test_etag_differs_and_no_version_id_recorded_is_live_superseded_never_mismatch():
    payload = _parquet(_frame(["AAPL"], [1.0]))
    reader = _VersionedReader(
        {
            _CLOSE_KEY: _parquet(_frame(["AAPL"], [2.0])),
            ROOT.key(_CLOSE_KEY): payload,
            _v1_manifest_key("D17"): _v1_manifest_with_version("e-day", None),
        },
        {_CLOSE_KEY: "e-next-day"},
    )
    report = _run_d17(reader)
    row = report.rows[0]
    assert row.verdict == "live_superseded"
    assert row.body["live_version"]["basis"] == "superseded_no_version_id"
    assert report.summary["live_superseded"] == 1
    assert report.summary["mismatch"] == 0
    assert report.met is False


def test_a_recorded_version_past_retention_is_live_superseded():
    reader = _VersionedReader(
        {
            _CLOSE_KEY: _parquet(_frame(["AAPL"], [2.0])),
            ROOT.key(_CLOSE_KEY): _parquet(_frame(["AAPL"], [1.0])),
            _v1_manifest_key("D17"): _v1_manifest_with_version("e-day", "v-expired"),
        },
        {_CLOSE_KEY: "e-next-day"},
    )
    row = _run_d17(reader).rows[0]
    assert row.verdict == "live_superseded"
    assert row.body["live_version"]["basis"] == "recorded_version_not_retained"


def test_a_manifest_with_no_output_record_for_the_key_keeps_todays_behaviour():
    """No record -> the current object is graded, exactly as before I10892."""
    reader = _VersionedReader(
        {
            _CLOSE_KEY: _parquet(_frame(["AAPL"], [2.0])),
            ROOT.key(_CLOSE_KEY): _parquet(_frame(["AAPL"], [1.0])),
            _v1_manifest_key("D17"): _v1_manifest_with_version("e-other", "v-other", key="some/other/key.json"),
        },
        {_CLOSE_KEY: "e-next-day"},
    )
    row = _run_d17(reader).rows[0]
    assert row.verdict == "mismatch"
    assert row.body["live_version"] == {"basis": "unrecorded"}


@pytest.mark.parametrize(
    "etag, version_id, capture",
    [
        (None, None, None),  # a pre-I10892 manifest: etag null, no version fields
        ("e", "v", "unavailable"),  # a lookup that failed is not a measurement
        (None, None, "object_absent"),
    ],
)
def test_an_unmeasured_record_keeps_todays_behaviour(etag, version_id, capture):
    reader = _VersionedReader(
        {
            _CLOSE_KEY: _parquet(_frame(["AAPL"], [2.0])),
            ROOT.key(_CLOSE_KEY): _parquet(_frame(["AAPL"], [1.0])),
            _v1_manifest_key("D17"): _v1_manifest_with_version(etag, version_id, capture=capture),
        },
        {_CLOSE_KEY: "e-next-day"},
    )
    row = _run_d17(reader).rows[0]
    assert row.verdict == "mismatch"
    assert row.body["live_version"]["basis"] == "unrecorded"
    assert reader.version_gets == []


def test_a_live_superseded_report_conforms_to_the_schema():
    import jsonschema

    reader = _VersionedReader(
        {
            _CLOSE_KEY: _parquet(_frame(["AAPL"], [2.0])),
            ROOT.key(_CLOSE_KEY): _parquet(_frame(["AAPL"], [1.0])),
            _v1_manifest_key("D17"): _v1_manifest_with_version("e-day", None),
        },
        {_CLOSE_KEY: "e-next-day"},
    )
    report = _run_d17(reader)
    schema = json.loads((REPO_ROOT / "contracts" / "data_parity_report.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(report.as_dict(), schema)
    assert set(report.summary) - {"total"} <= set(
        schema["$defs"]["keyResult"]["properties"]["verdict"]["enum"]
    )


def test_s3_reader_get_with_meta_fetches_a_version_and_treats_no_such_version_as_absent():
    class _Err(Exception):
        def __init__(self, code):
            self.response = {"Error": {"Code": code}}

    class _Client:
        def __init__(self):
            self.calls = []

        def get_object(self, **kw):
            self.calls.append(kw)
            if kw.get("VersionId") == "gone":
                raise _Err("NoSuchVersion")
            if kw.get("VersionId") == "denied":
                raise _Err("AccessDenied")
            return {"Body": io.BytesIO(b"x"), "ETag": '"abc"', "VersionId": kw.get("VersionId", "null")}

    client = _Client()
    reader = parity.S3Reader("alpha-engine-research", client=client)
    assert reader.get_with_meta("k", version_id="v1") == {"body": b"x", "etag": "abc", "version_id": "v1"}
    assert reader.get_with_meta("k") == {"body": b"x", "etag": "abc", "version_id": None}
    assert reader.get_with_meta("k", version_id="gone") is None
    with pytest.raises(_Err):
        reader.get_with_meta("k", version_id="denied")


def test_the_report_schema_accepts_excluded_class_and_provenance_diffs():
    """The producer contract test for the two schema additions."""
    import jsonschema

    payload = _parquet(_frame(["AAPL"], [1.0]))
    live_key = "staging/daily_closes/2026-09-12.parquet"
    reader = _FakeReader(
        {
            live_key: payload,
            ROOT.key(live_key): payload,
            _shadow_manifest_key("D17"): _manifest_bytes("D17", "2026-09-12T20:00:00Z", [live_key]),
        }
    )
    report = parity.run_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        reader=reader,
        units=_one_unit("D17") + _one_unit("D18"),
    )
    schema = json.loads(
        (REPO_ROOT / "contracts" / "data_parity_report.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(report.as_dict(), schema)
    assert any(e["class"] == "out_of_run_scope" for e in report.excluded)

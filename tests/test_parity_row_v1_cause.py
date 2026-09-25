"""Row-scoped `v1_cause` evidence for rows a run carried over (alpha-engine-config-I11563).

The measured 2026-09-22 shape. v1's D17 fetched polygon's grouped-daily the
next morning (`settled`), but polygon does not serve CPRI or SAM, so D17's
file CARRIED those two rows over from v1 D19's 16:06 ET write
(`provisional`). The shadow's D17 carried the same two rows from the shadow's
D19, which fetched at 18:45 ET (`settled`). CPRI and SAM `Close`,
`Adj_Close` and `Volume` were the only breaches on the D-1 re-check.

Under I11559 the key's two readings disagree, so the whole-key stamp is
ambiguous and the row stays strict. This change:

1. D17 (and D19) stamp EACH carried row, keyed ``<key>#<ticker>``, graded on
   the write time of the object it came from.
2. Parity grants `v1_cause` only when EVERY row holding a numeric breach reads
   `provisional` on v1's side and `settled` on the shadow's. A carried
   `settled` reading is never taken on trust: it is followed back to the run
   that fetched the row.
3. A `mismatch` row graded for v1_cause always says why it was refused, even
   when no evidence applied at all.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pandas as pd
import pytest

import run_units
import weekly_collector
from collectors import daily_closes
from shadow import parity
from shadow.root import ShadowRoot

REPORT_DAY = dt.date(2026, 9, 23)
PRIOR_DAY = "2026-09-22"
KEY = f"staging/daily_closes/{PRIOR_DAY}.parquet"

#: Measured write and fetch moments (UTC), 2026-09-22/23.
V1_D19_FETCH = dt.datetime(2026, 9, 22, 20, 4, 38, tzinfo=dt.timezone.utc)  # 16:04 ET
V1_D19_WROTE = dt.datetime(2026, 9, 22, 20, 6, 48, tzinfo=dt.timezone.utc)  # 16:06 ET
V1_D17_FETCH = dt.datetime(2026, 9, 23, 12, 19, 40, tzinfo=dt.timezone.utc)
SH_D19_FETCH = dt.datetime(2026, 9, 22, 22, 45, 22, tzinfo=dt.timezone.utc)  # 18:45 ET
SH_D19_WROTE = dt.datetime(2026, 9, 22, 22, 46, 46, tzinfo=dt.timezone.utc)
SH_D17_FETCH = dt.datetime(2026, 9, 23, 11, 47, 48, tzinfo=dt.timezone.utc)

CARRIED = ("CPRI", "SAM")


def _iso(moment: dt.datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _manifest(unit: str, run_id: str, started: dt.datetime, finished: dt.datetime, guards: list[dict]) -> dict:
    return {
        "unit_id": unit,
        "run_id": run_id,
        "started": _iso(started),
        "finished": _iso(finished),
        "status": "ok",
        "trading_day": PRIOR_DAY,
        "outputs": [{"key": KEY}],
        "guards": guards,
    }


def _d19(run_id: str, fetch: dt.datetime, wrote: dt.datetime, carried=(), carried_from=None) -> dict:
    guards = daily_closes._settlement_guards(
        fetch, PRIOR_DAY, KEY, carried_rows=carried, carried_from=carried_from
    )
    return _manifest("D19", run_id, fetch, wrote, guards)


def _d17(run_id: str, fetch: dt.datetime, carried_from: dt.datetime, carried=CARRIED) -> dict:
    guards = daily_closes._settlement_guards(
        fetch, PRIOR_DAY, KEY, carried_rows=carried, carried_from=carried_from
    )
    return _manifest("D17", run_id, fetch, fetch + dt.timedelta(minutes=3), guards)


def _measured_context(**overrides) -> parity.V1CauseContext:
    """The four measured writes; an override of ``None`` removes that write."""
    defaults = {
        "v1_d19": lambda: _d19("v1-d19", V1_D19_FETCH, V1_D19_WROTE),
        "v1_d17": lambda: _d17("v1-d17", V1_D17_FETCH, V1_D19_WROTE),
        "sh_d19": lambda: _d19("sh-d19", SH_D19_FETCH, SH_D19_WROTE),
        "sh_d17": lambda: _d17("sh-d17", SH_D17_FETCH, SH_D19_WROTE),
    }
    v1_d19, v1_d17, sh_d19, sh_d17 = (
        overrides[name] if name in overrides else build() for name, build in defaults.items()
    )
    v1 = [m for m in (v1_d19, v1_d17) if m is not None]
    shadow = [m for m in (sh_d19, sh_d17) if m is not None]
    return parity.V1CauseContext(
        v1=parity.manifest_recording(v1, KEY),
        shadow=parity.manifest_recording(shadow, KEY),
        v1_history=parity.manifests_recording(v1, KEY),
        shadow_history=parity.manifests_recording(shadow, KEY),
    )


def _frame(rows: dict[str, tuple[float, float, int]], source: str = "polygon") -> bytes:
    frame = pd.DataFrame(
        {
            "Close": [r[0] for r in rows.values()],
            "Adj_Close": [r[1] for r in rows.values()],
            "Volume": [r[2] for r in rows.values()],
            "source": [source] * len(rows),
        },
        index=pd.Index(list(rows), name="ticker"),
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


_LIVE = {"AAPL": (254.43, 254.43, 41_000_000), "CPRI": (15.195, 15.195, 2_100_000), "SAM": (168.08, 168.08, 101_000)}
_SHADOW = {"AAPL": (254.43, 254.43, 41_000_000), "CPRI": (15.190, 15.190, 2_600_000), "SAM": (168.25, 168.25, 131_000)}


def _compare(live=_LIVE, shadow=_SHADOW, context=None, *, shadow_source: str = "polygon") -> dict:
    return parity.compare_bytes(
        KEY,
        _frame(live),
        _frame(shadow, source=shadow_source),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(KEY),
        trading_day=REPORT_DAY,
        v1_cause=context if context is not None else _measured_context(),
    )


# ---------------------------------------------------------------------------
# 1. The collector stamps each carried row
# ---------------------------------------------------------------------------


def test_the_coalesce_names_every_row_it_carried_from_the_existing_object():
    existing = [
        {"ticker": "TNX", "Close": 4.5, "source": "fred"},
        {"ticker": "AAPL", "Close": 100.0, "source": "polygon"},
        {"ticker": "MSFT", "Close": 400.0, "source": "yfinance"},
    ]
    new = [
        {"ticker": "AAPL", "Close": 101.0, "source": "yfinance"},  # downgrade blocked
        {"ticker": "MSFT", "Close": 401.0, "source": "polygon"},  # overwritten
    ]
    carried: list[str] = []
    _, stats = daily_closes._coalesce_by_source_priority(new, existing, PRIOR_DAY, carried=carried)
    assert sorted(carried) == ["AAPL", "TNX"]
    assert stats["retained"] + stats["downgrade_blocked"] == len(carried)


def test_each_carried_row_gets_its_own_reading_graded_on_the_source_objects_write():
    guards = daily_closes._settlement_guards(
        V1_D17_FETCH, PRIOR_DAY, KEY, carried_rows=["SAM", "CPRI"], carried_from=V1_D19_WROTE
    )
    assert [(g["key"], g["verdict"]) for g in guards] == [
        (KEY, "settled"),
        (f"{KEY}#CPRI", "provisional"),
        (f"{KEY}#SAM", "provisional"),
    ]
    assert all(set(g) == {"guard", "mode", "verdict", "detail", "key", "value", "baseline"} for g in guards)
    assert "2026-09-22T20:06:48Z" in guards[1]["detail"]


def test_a_run_that_carried_nothing_records_only_its_fetch():
    guards = daily_closes._settlement_guards(V1_D17_FETCH, PRIOR_DAY, KEY, carried_rows=[], carried_from=V1_D19_WROTE)
    assert [g["key"] for g in guards] == [KEY]


def test_every_carried_row_is_named_however_many_there_are():
    rows = [f"T{i:03d}" for i in range(900)]
    guards = daily_closes._settlement_guards(V1_D17_FETCH, PRIOR_DAY, KEY, carried_rows=rows, carried_from=V1_D19_WROTE)
    assert len(guards) == 901
    assert max(len(g["detail"]) for g in guards) < 2000


def test_the_row_readings_survive_the_fold_onto_d17s_manifest(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    written: list[dict] = []

    class _Sink:
        def write(self, key: str, payload: bytes) -> None:
            if key.startswith("data_collection/runs/"):
                written.append(json.loads(payload.decode("utf-8")))

    class _Args:
        date = PRIOR_DAY
        dry_run = False

    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: _Sink())
    result = {
        "status": "ok",
        "date": PRIOR_DAY,
        "collectors": {
            "daily_closes": {
                "status": "ok",
                "guards": daily_closes._settlement_guards(
                    V1_D17_FETCH, PRIOR_DAY, KEY, carried_rows=CARRIED, carried_from=V1_D19_WROTE
                ),
            }
        },
    }
    weekly_collector._run_whole_mode_unit(
        "morning_enrich", lambda config, args: result, {"bucket": "alpha-engine-research"}, _Args()
    )
    (manifest,) = written
    stamps = {g["key"]: g["verdict"] for g in manifest["guards"] if g["guard"] == "bar_settlement"}
    assert stamps == {KEY: "settled", f"{KEY}#CPRI": "provisional", f"{KEY}#SAM": "provisional"}


# ---------------------------------------------------------------------------
# 2. Parity grants v1_cause row by row, and only on proof
# ---------------------------------------------------------------------------


def test_the_measured_d1_closes_row_grades_v1_cause_on_the_two_carried_rows():
    body = _compare()
    assert body["values"]["breaches"] == 6
    assert body["verdict"] == "v1_cause"
    (proof,) = body["v1_cause"]["evidence"]
    assert proof["kind"] == "v1_bar_provisional"
    assert proof["scope"] == "rows"
    assert proof["rows_explained"] == 2
    assert sorted(proof["rows"]) == ["CPRI", "SAM"]
    cpri = proof["rows"]["CPRI"]
    assert cpri["breaches"] == 3
    assert (cpri["v1"]["verdict"], cpri["v1"]["basis"]) == ("provisional", "carried")
    assert [m["run_id"] for m in cpri["v1"]["via"]] == ["v1-d17"]
    # The shadow's carried `settled` was followed back to the run that fetched it.
    assert (cpri["shadow"]["verdict"], cpri["shadow"]["basis"]) == ("settled", "fetched")
    assert [m["run_id"] for m in cpri["shadow"]["via"]] == ["sh-d17", "sh-d19"]
    assert "_numeric_breach_rows" not in body


def test_the_whole_key_stamp_stays_ambiguous():
    context = _measured_context()
    assert parity._bar_settlement_stamp(context.v1, KEY) is None
    assert parity._bar_settlement_stamp(context.shadow, KEY)["verdict"] == "settled"


def test_a_breach_on_a_row_v1_fetched_settled_keeps_the_whole_key_strict():
    live = {**_LIVE, "AAPL": (254.43, 254.43, 41_000_000)}
    shadow = {**_SHADOW, "AAPL": (254.90, 254.90, 41_000_000)}
    body = _compare(live, shadow)
    assert body["verdict"] == "mismatch"
    assert "v1_cause" not in body
    assert any("row AAPL" in reason and "settled, not provisional" in reason for reason in body["v1_cause_refused"])


def test_an_identity_breach_on_a_carried_row_is_never_explained():
    body = _compare(shadow_source="yfinance")
    assert body["verdict"] == "mismatch"
    assert any("identity" in reason for reason in body["v1_cause_refused"])


def test_a_shadow_row_carried_settled_with_no_earlier_write_on_record_is_unproven():
    context = _measured_context(sh_d19=None)
    body = _compare(context=context)
    assert body["verdict"] == "mismatch"
    assert any("not proven" in reason for reason in body["v1_cause_refused"])


def test_a_carried_settled_reading_that_hides_a_provisional_fetch_is_caught():
    """The shadow's D19 ran twice. The 16:05 ET attempt fetched CPRI and SAM
    (`provisional`); the 18:40 ET re-run carried them, and D17 then carried
    them from THAT object, written after the settlement hour. Graded on write
    time alone, D17's reading says `settled`. Followed back, it is not."""
    early = _d19("sh-d19-early", dt.datetime(2026, 9, 22, 20, 5, tzinfo=dt.timezone.utc),
                 dt.datetime(2026, 9, 22, 20, 6, tzinfo=dt.timezone.utc))
    rerun = _d19(
        "sh-d19-rerun",
        dt.datetime(2026, 9, 22, 22, 40, tzinfo=dt.timezone.utc),
        SH_D19_WROTE,
        carried=CARRIED,
        carried_from=dt.datetime(2026, 9, 22, 20, 6, tzinfo=dt.timezone.utc),
    )
    d17 = _d17("sh-d17", SH_D17_FETCH, SH_D19_WROTE)
    assert {g["verdict"] for g in d17["guards"] if g["key"] == f"{KEY}#CPRI"} == {"settled"}
    shadow = [early, rerun, d17]
    context = parity.V1CauseContext(
        v1=_measured_context().v1,
        shadow=parity.manifest_recording(shadow, KEY),
        v1_history=_measured_context().v1_history,
        shadow_history=parity.manifests_recording(shadow, KEY),
    )
    reading, _ = parity._row_bar_settlement(context.shadow, KEY, "CPRI", context.shadow_history)
    assert (reading.verdict, reading.basis) == ("provisional", "carried")
    assert [m["run_id"] for m in reading.trail] == ["sh-d17", "sh-d19-rerun"]
    body = _compare(context=context)
    assert body["verdict"] == "mismatch"
    assert any("shadow's manifests stamp it bar_settlement: provisional" in r for r in body["v1_cause_refused"])


def test_a_pre_row_stamp_manifest_proves_no_row():
    """The I11559 shape: one whole-key carried reading, no rows named. Which
    rows were fetched is not recorded, so no row is proven."""
    v1_d17 = _d17("v1-d17", V1_D17_FETCH, V1_D19_WROTE, carried=())
    v1_d17["guards"].append({**v1_d17["guards"][0], "verdict": "provisional", "detail": "2 row(s) carried"})
    body = _compare(context=_measured_context(v1_d17=v1_d17))
    assert body["verdict"] == "mismatch"
    assert any("whole-key readings disagree" in reason for reason in body["v1_cause_refused"])


def test_a_d17_with_no_stamps_at_all_proves_nothing():
    """What every v1 D17 manifest measured on 2026-09-22/23 looked like."""
    v1_d17 = _d17("v1-d17", V1_D17_FETCH, V1_D19_WROTE)
    v1_d17["guards"] = []
    body = _compare(context=_measured_context(v1_d17=v1_d17))
    assert body["verdict"] == "mismatch"
    assert any("recorded no bar_settlement reading" in reason for reason in body["v1_cause_refused"])


def test_row_scope_never_reaches_a_history_artifact():
    """`row_date` keys (price_cache) carry earlier rows no stamp describes."""
    key = "reference/price_cache/CPRI.parquet"
    frame = pd.DataFrame({"Close": [15.195]}, index=pd.Index([pd.Timestamp(PRIOR_DAY)], name="Date"))
    other = pd.DataFrame({"Close": [15.0]}, index=pd.Index([pd.Timestamp(PRIOR_DAY)], name="Date"))
    live, shadow = io.BytesIO(), io.BytesIO()
    frame.to_parquet(live)
    other.to_parquet(shadow)
    body = parity.compare_bytes(
        key, live.getvalue(), shadow.getvalue(), rel=0.0, absolute=0.0,
        contract=parity.resolve_contract(key), trading_day=REPORT_DAY, v1_cause=_measured_context(),
    )
    assert body["verdict"] == "mismatch"
    assert any("key_date" in reason for reason in body["v1_cause_refused"])


# ---------------------------------------------------------------------------
# 3. A refused row always says why
# ---------------------------------------------------------------------------


def test_a_mismatch_with_no_evidence_at_all_still_records_why_it_was_refused():
    empty = parity.V1CauseContext(v1=None, shadow=None)
    body = _compare(context=empty)
    assert body["verdict"] == "mismatch"
    assert body["v1_cause_refused"]
    assert "v1_cause NOT proven" in parity._verdict_detail(body)


def test_the_refusal_reaches_the_prior_day_settled_field():
    body = _compare(context=parity.V1CauseContext(v1=None, shadow=None))
    result = parity.KeyResult(KEY, ["D17", "D19"], body["verdict"], "parquet", body)
    field = parity._prior_day_settled_field(result, prior_day=dt.date(2026, 9, 22), prior_live_key=KEY,
                                            prior_shadow_key=f"staging/shadow/{PRIOR_DAY}/{KEY}")
    assert field["verdict"] == "mismatch"
    assert field["v1_cause_refused"] == body["v1_cause_refused"]


# ---------------------------------------------------------------------------
# 4. The report reads each side's whole history of writes to the key
# ---------------------------------------------------------------------------


class _Reader:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def get(self, key: str):
        return self.objects.get(key)

    def list(self, prefix: str, limit: int):
        return sorted(k for k in self.objects if k.startswith(prefix))[:limit]


def test_the_manifest_reader_hands_parity_every_write_of_the_key():
    context = _measured_context()
    manifests = {
        f"data_collection/runs/D19/{PRIOR_DAY}/v1-d19.json": context.v1_history[0],
        f"data_collection/runs/D17/{PRIOR_DAY}/v1-d17.json": context.v1_history[1],
        f"staging/shadow/{PRIOR_DAY}/data_collection/runs/D19/{PRIOR_DAY}/sh-d19.json": context.shadow_history[0],
        f"staging/shadow/{PRIOR_DAY}/data_collection/runs/D17/{PRIOR_DAY}/sh-d17.json": context.shadow_history[1],
    }
    reader = _Reader({k: json.dumps(v).encode() for k, v in manifests.items()})
    from data_gate.descriptors import load_units

    units = {u.unit_id: u for u in load_units()}
    day = dt.date.fromisoformat(PRIOR_DAY)
    built = parity._V1CauseManifests(reader, units, day, ShadowRoot(day)).context(KEY, ["D17", "D19"])
    assert built.v1["run_id"] == "v1-d17"
    assert [m["run_id"] for m in built.v1_history] == ["v1-d19", "v1-d17"]
    assert [m["run_id"] for m in built.shadow_history] == ["sh-d19", "sh-d17"]
    assert _compare(context=built)["verdict"] == "v1_cause"


@pytest.mark.parametrize("row", CARRIED)
def test_each_carried_row_reads_provisional_on_v1_and_settled_on_the_shadow(row):
    context = _measured_context()
    v1, _ = parity._row_bar_settlement(context.v1, KEY, row, context.v1_history)
    shadow, _ = parity._row_bar_settlement(context.shadow, KEY, row, context.shadow_history)
    assert (v1.verdict, shadow.verdict) == ("provisional", "settled")
    fetched, _ = parity._row_bar_settlement(context.v1, KEY, "AAPL", context.v1_history)
    assert (fetched.verdict, fetched.basis) == ("settled", "fetched")

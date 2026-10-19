"""Guard commissioning, class ``units``: one induced-fault record per unit.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate: "one induced fault per guard class
(withheld vendor, truncated universe, **unit-scaled column**, empty frame,
stale ``as_of``)") and the phase-2 tracker `alpha-engine-config-I10794`. A
guard that has never fired is not in service (`observability-policy` §9.1).
This is the fourth guard class with a harness, after ``empty_fresh``,
``success_without_output`` and ``cardinality``; it reuses the ``empty_fresh``
machinery (the in-memory S3, the harness clock, the run-manifest environment).

**D01 is the case, and it is on purpose.** D01 and D02 are the clauses the
phase-2 ladder holds on, and every earlier class held them because the guards
those classes commission (``data_empty_fresh``, ``data_cardinality``) are staged
OBSERVE on D01/D02 by ruling (nousergon-data#2145: "an observe-only,
non-raising predicate on that path"). An ``enforce`` record for an observe-only
guard would be false evidence. D01's ``units`` guard is a different kind of
thing: it has no ``GuardStaging`` and no mode at all. It is the unit's own
units DETECTION (``collectors/constituents.py::_fetch_ssga_membership``,
``evaluates: lo <= raw_sum <= hi``), and it refuses as shipped. So this record
is taken against the code exactly as it runs on the trader's feed today,
changes nothing on that path, and does not touch the observe-only ruling.

**The fault is a unit-scaled weight column.** SPY's holdings file is served
with its ``Weight`` column in basis points (the equity rows sum to 9,850),
which is neither the percent band (90-101) nor the fraction band (0.90-1.01)
in ``_WEIGHT_SUM_BANDS``. The REAL ``constituents.collect`` runs through the
REAL ``weekly_collector._phase_collect`` with the phase-one call site's own
arguments; only the two vendors (the SSGA download and the Wikipedia GICS
table) are stubbed.

**The refusal has two consequences, and the record observes both**, because
which one fires depends on the box-local roster cache
(``data/constituents_cache.csv``, gitignored and written only by an earlier
successful fetch in the same box's life):

1. **Cold box** (the deployed weekly spot instance, fresh each run): the
   refused fetch falls back to a cache that does not exist, ``collect`` raises
   ``ConstituentsUnavailable`` naming the units refusal, the manifest files
   ``failed``, the phase marker is ``error``, and nothing D01 publishes is
   written — the previous cycle's snapshot is what consumers keep reading.
   The next run with a correctly scaled file is ``ok`` with every weight
   normalised to 1.0 within its index.
2. **Warm box** (a cache present): the refused weights are never published.
   The run serves the cached roster with ``weight_method: cache_no_weights``
   and an EMPTY ``weight_map`` (an unknown weight, never a mis-scaled one), and
   the downstream consumer, D48's ``index_contributions.collect``, refuses that
   empty map rather than publishing an index that moved 0%.

What this record does NOT claim: a page count on the deployed Step Functions
definition. The weekly phase-one run is not walked here (the three earlier
classes walk the EOD ``post-market-data`` workload), so the record's
``observed`` lines stop at the manifest, the marker and the published bytes.

Not here, and why:

* **D02**: ``guards.units`` is ``absent``; there is no check to induce.
* **D19**: the executor's feed, held for every guard class.
* **D03, D17, D25-D27, D37, D48, D49, D51** (and the ``partial`` /
  ``present_static`` rows): each declares its units guard as a schema reference
  or runs on another workload; each needs its own walk.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import inspect
import io
import json
import logging
import re
import types
from dataclasses import dataclass
from unittest import mock

import pandas as pd
import pytest

import run_units
import weekly_collector
from collectors import constituents, index_contributions
from data_gate import descriptors, evidence
from data_gate import guard_commissioning as gc
from data_gate.store import LocalStore
from shadow.run_state import RunStatePhaseRegistry
from tests import test_guard_commissioning_empty_fresh as ef
from tests.test_guard_commissioning_empty_fresh import (  # noqa: F401 -- fixtures, used by name
    _environment,
    clock,
)

GUARD_CLASS = "units"
RUN_DATE = ef.TRADING_DAY
BUCKET = ef.BUCKET
MARKET_PREFIX = "market_data/"
SNAPSHOT_KEY = f"{MARKET_PREFIX}weekly/{RUN_DATE}/constituents.json"
#: The previous cycle's snapshot, seeded before every scenario.
PRIOR_DATE = (dt.date.fromisoformat(RUN_DATE) - dt.timedelta(days=7)).isoformat()
PRIOR_KEY = f"{MARKET_PREFIX}weekly/{PRIOR_DATE}/constituents.json"

SP500 = ("AAPL", "MSFT", "NVDA", "AMZN")
SP400 = ("TOST", "IESC", "EME", "WSM")
#: Percent, equity rows only (cash carries the rest): the normal observed shape.
_PERCENT = {"S&P 500": [40.0, 30.0, 20.0, 8.5], "S&P 400": [30.0, 30.0, 20.0, 18.5]}
#: The fault: SPY's column served in basis points. 9,850 is in neither band.
_FAULTED = {"S&P 500": [4000.0, 3000.0, 2000.0, 850.0], "S&P 400": _PERCENT["S&P 400"]}

#: Every key D01's phase-one run publishes from `collect` itself.
_PUBLISHED = (
    SNAPSHOT_KEY,
    "data/sector_map.json",
    "reference/price_cache/sector_map.json",
    "data/sub_industry_map.json",
    "reference/price_cache/sub_industry_map.json",
    "data/sub_sector_etf_map.json",
    "reference/price_cache/sub_sector_etf_map.json",
)

#: The real previous-snapshot loader, taken at import: `tests/conftest.py` stubs it
#: to "no previous snapshot" for every test so nothing reads the real S3. Here it
#: reads only the in-memory store, so the D01 cardinality reading on these runs is
#: graded against the seeded prior cycle rather than recorded `unmeasurable`.
_REAL_LOAD_PREVIOUS = constituents._load_previous_snapshot

#: The units refusal's own words (`_fetch_ssga_membership`).
REFUSAL = "which is neither percent"


@dataclass(frozen=True)
class Case:
    """One unit's induced unit-scaled column. ``unit`` and ``fault`` are what the publisher reads."""

    unit: str
    phase: str
    fault: str


CASES: tuple[Case, ...] = (
    Case(
        "D01", "constituents",
        "unit-scaled column: SPY's holdings file serves its Weight column in basis points "
        "(equity rows sum to 9850), in neither the percent (90-101) nor the fraction "
        "(0.90-1.01) band of _WEIGHT_SUM_BANDS",
    ),
)
CASE_IDS = [c.unit for c in CASES]
D01 = CASES[0]


# ---------------------------------------------------------------------------
# The two vendors, stubbed beneath the real collector
# ---------------------------------------------------------------------------


class S3(ef.InMemoryS3):
    """``ef.InMemoryS3`` plus the delimited listing `_load_previous_snapshot` pages through."""

    def get_paginator(self, _name):
        store = self

        class _Paginator:
            def paginate(self, Bucket, Prefix="", Delimiter="", **_):  # noqa: N803 -- boto3 kwarg names
                prefixes = sorted({
                    Prefix + k[len(Prefix):].split(Delimiter, 1)[0] + Delimiter
                    for k in store.objects
                    if k.startswith(Prefix) and Delimiter in k[len(Prefix):]
                })
                yield {"CommonPrefixes": [{"Prefix": p} for p in prefixes]}

        return _Paginator()

    def d01_manifests(self) -> list[dict]:
        prefix = f"data_collection/runs/D01/{RUN_DATE}/"
        return [json.loads(self.objects[k]) for k in sorted(self.objects) if k.startswith(prefix)]

    def d01_marker(self) -> dict:
        return json.loads(self.objects[f"data/{RUN_DATE}/.phases/constituents.json"])

    def document(self, key: str) -> dict:
        return json.loads(self.objects[key])


def _xlsx(tickers: tuple[str, ...], weights: list[float]) -> bytes:
    """An SSGA-holdings-shaped sheet: the 4-row banner, then the header row, plus a cash row."""
    rows = {"Ticker": [*tickers, "CASH_USD"], "Weight": [*weights, 1.5]}
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, startrow=4)
    return buf.getvalue()


class _Resp:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


@contextlib.contextmanager
def _vendors(fake: S3, weights: dict[str, list[float]]):
    """SSGA serves ``weights``; Wikipedia classifies every member; S3 is ``fake``. Nothing else."""
    files = {
        constituents._SSGA_HOLDINGS_URLS["S&P 500"]: _xlsx(SP500, weights["S&P 500"]),
        constituents._SSGA_HOLDINGS_URLS["S&P 400"]: _xlsx(SP400, weights["S&P 400"]),
    }

    def get(url, **_kwargs):
        if url not in files:
            raise AssertionError(f"unexpected URL in the units harness: {url}")
        return _Resp(files[url])

    members = (*SP500, *SP400)
    wikipedia = (
        dict.fromkeys(members, "Information Technology"),
        dict.fromkeys(members, "XLK"),
        dict.fromkeys(members, "Semiconductors"),
    )
    client = types.SimpleNamespace(client=lambda *_a, **_k: fake)
    with (
        mock.patch.object(constituents.requests, "get", get),
        mock.patch.object(constituents, "_fetch_wikipedia_sectors", lambda: wikipedia),
        mock.patch.object(constituents, "boto3", client),
        mock.patch.object(constituents, "_load_previous_snapshot", _REAL_LOAD_PREVIOUS),
        mock.patch.object(weekly_collector, "_s3_object_exists", lambda _b, key: key in fake.objects),
    ):
        yield


def _seed_prior_cycle(fake: S3) -> bytes:
    """The previous week's healthy snapshot: same members, real GICS, percent weights."""
    members = [*SP500, *SP400]
    body = json.dumps({
        "date": PRIOR_DATE,
        "tickers": members,
        "sector_map": dict.fromkeys(members, "Information Technology"),
        "sector_etf_map": dict.fromkeys(members, "XLK"),
        "weight_map": {t: 0.125 for t in members},
        "weight_method": "ssga_holdings_file",
    }).encode("utf-8")
    fake.objects[PRIOR_KEY] = body
    return body


#: The phase-one call site's ``extra_outputs`` (``only is None``), mirrored and pinned below.
def _extra_outputs() -> tuple:
    def count(key: str):
        return lambda r: r.get(key) or 0

    return (
        (f"{MARKET_PREFIX}latest_weekly.json", lambda r: True, count("count")),
        ("data/sector_map.json", lambda r: True, count("sector_map_count")),
        ("reference/price_cache/sector_map.json", lambda r: True, count("sector_map_count")),
        ("data/sub_industry_map.json", lambda r: True, count("sub_industry_map_count")),
        ("reference/price_cache/sub_industry_map.json", lambda r: True, count("sub_industry_map_count")),
        ("data/sub_sector_etf_map.json", lambda r: True, count("sub_sector_etf_map_count")),
        ("reference/price_cache/sub_sector_etf_map.json", lambda r: True, count("sub_sector_etf_map_count")),
    )


def _run_phase(fake: S3, weights: dict[str, list[float]]) -> dict:
    """One D01 attempt through the real `_phase_collect`, exactly as the phase-one run calls it."""
    reg = RunStatePhaseRegistry(date=RUN_DATE, bucket=BUCKET, marker_prefix="data", s3_client=fake)
    reg.data_mode = "phase1"
    with _vendors(fake, weights):
        return weekly_collector._phase_collect(
            reg, "constituents",
            lambda: constituents.collect(
                bucket=BUCKET, s3_prefix=MARKET_PREFIX, run_date=RUN_DATE, dry_run=False,
            ),
            artifact_key=SNAPSHOT_KEY,
            extra_outputs=_extra_outputs(),
        )


@pytest.fixture
def cache(monkeypatch, tmp_path):
    """The box-local roster cache, pointed into ``tmp_path``. Absent until a real fetch writes it."""
    path = tmp_path / "constituents_cache.csv"
    monkeypatch.setattr(constituents, "_CACHE_PATH", path)
    return path


def _assert_weights_normalised(document: dict) -> None:
    assert document["weight_method"] == "ssga_holdings_file"
    for index in ("S&P 500", "S&P 400"):
        total = sum(w for t, w in document["weight_map"].items() if document["index_of"][t] == index)
        assert total == pytest.approx(1.0, abs=1e-9), (index, total)


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_unit_scaled_column_is_refused_and_never_published(case, cache, caplog):
    """The commissioning record for ``units``, on the unit's own units detection, as shipped."""
    assert case is D01
    caplog.set_level(logging.WARNING)

    # ── Cold box: the deployed weekly spot instance, no roster cache ─────────
    cold = S3()
    prior = _seed_prior_cycle(cold)
    assert not cache.exists()
    faulted = _run_phase(cold, _FAULTED)

    # 1. The refusal: the phase fails, naming the units refusal, and is recorded.
    assert faulted["status"] == "error", faulted
    manifests = cold.d01_manifests()
    assert [m["status"] for m in manifests] == ["failed"], manifests
    assert REFUSAL in manifests[0]["reason"], manifests[0]["reason"]
    assert "ConstituentsUnavailable" in manifests[0]["reason"], manifests[0]["reason"]
    assert manifests[0]["outputs"] == []
    assert cold.d01_marker()["status"] == "error"

    # 2. Stand-down: nothing D01 publishes was written, and last week's snapshot stands.
    for key in _PUBLISHED:
        assert key not in cold.objects, f"{key} was published from a refused weight column"
    assert cold.objects[PRIOR_KEY] == prior

    # 3. Recovery: a correctly scaled file is `ok`, with every weight normalised in its index.
    recovered = _run_phase(cold, _PERCENT)
    assert recovered["status"] == "ok", recovered
    assert recovered["weight_method"] == "ssga_holdings_file"
    assert [m["status"] for m in cold.d01_manifests()] == ["failed", "ok"]
    _assert_weights_normalised(cold.document(SNAPSHOT_KEY))
    assert cache.exists(), "a successful fetch writes the box-local roster cache"

    # ── Warm box: the same fault with a roster cache present ─────────────────
    warm = S3()
    _seed_prior_cycle(warm)
    with _vendors(warm, _PERCENT):
        constituents._fetch_constituents()  # the real fetch that writes the cache
    caplog.clear()
    served = _run_phase(warm, _FAULTED)

    # 4. The refused weights are never published: the cached roster, no weights.
    assert served["status"] == "ok", served
    assert served["weight_method"] == "cache_no_weights"
    assert served["weight_map_count"] == 0
    snapshot = warm.document(SNAPSHOT_KEY)
    assert snapshot["weight_method"] == "cache_no_weights"
    assert snapshot["weight_map"] == {}
    assert snapshot["weight_sum_raw_sp500"] is None
    assert sorted(snapshot["tickers"]) == sorted((*SP500, *SP400))
    assert [m["status"] for m in warm.d01_manifests()] == ["ok"]
    warned = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(REFUSAL in m for m in warned), warned

    # 5. ...and D48, the weight consumer, refuses the empty map rather than publish 0%.
    warm.objects[f"{MARKET_PREFIX}latest_weekly.json"] = json.dumps({"date": RUN_DATE}).encode()
    spx = next(spec for spec in index_contributions.INDEXES if spec.index == "SPX")
    with (
        mock.patch.object(constituents, "boto3", types.SimpleNamespace(client=lambda *_a, **_k: warm)),
        pytest.raises(index_contributions.IndexContributionsUnavailable) as refused,
    ):
        index_contributions.collect(
            bucket=BUCKET, run_date=RUN_DATE, dry_run=True,
            closes_source=lambda *_a, **_k: {}, indexes=(spx,),
        )
    assert "no weights available (method='cache_no_weights')" in str(refused.value)

    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit={case.unit} phase={case.phase} mode=phase1"
        f"\n  fault:      {case.fault}"
        f"\n  cold box:   {faulted['status']}; manifest failed: {manifests[0]['reason'][:120]}..."
        f"\n  stand-down: 0 of {len(_PUBLISHED)} keys written; {PRIOR_KEY} untouched"
        f"\n  recovery:   {recovered['status']}, weight_method={recovered['weight_method']}"
        f"\n  warm box:   {served['status']}, weight_method={served['weight_method']}, "
        f"weight_map={snapshot['weight_map']}; D48 refused: {str(refused.value)[:80]}..."
    )


# ---------------------------------------------------------------------------
# What keeps the record honest
# ---------------------------------------------------------------------------


def test_the_case_runs_the_code_the_descriptor_names():
    """The harness induces against exactly the check D01's descriptor declares."""
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D01")
    block = unit.guards[GUARD_CLASS]
    assert block["state"] == "present"
    assert block["code"] == ["collectors/constituents.py::_fetch_ssga_membership"]
    assert block["evaluates"] == "lo <= raw_sum <= hi"
    assert block["evaluates"] in inspect.getsource(constituents._fetch_ssga_membership)
    assert run_units.PHASE_UNITS[("phase1", D01.phase)].unit_id == "D01"


def test_the_fault_crosses_both_bands_and_the_healthy_file_sits_in_one():
    """9,850 is in neither band; the healthy sum (98.5) is percent. Otherwise the record proves nothing."""
    def band(total: float) -> str | None:
        return next((n for n, lo, hi in constituents._WEIGHT_SUM_BANDS if lo <= total <= hi), None)

    assert band(sum(_FAULTED["S&P 500"])) is None
    assert band(sum(_PERCENT["S&P 500"])) == "percent"
    assert band(sum(_PERCENT["S&P 400"])) == "percent"


def test_the_units_guard_has_no_observe_staging_to_promote():
    """Why D01 is commissionable here when the other classes hold it.

    The observe-only ruling on D01/D02 (nousergon-data#2145) stages
    ``data_cardinality`` and ``data_empty_fresh`` through ``GuardStaging``. The
    units detection carries no staging and records no shared guard: it refuses
    as shipped, so the record is taken against the live behaviour, not a flip.
    """
    block = next(u for u in descriptors.load_units() if u.unit_id == "D01").guards[GUARD_CLASS]
    assert "records_as" not in block, block
    source = inspect.getsource(constituents._fetch_ssga_membership)
    assert "GuardStaging" not in source and "GuardMode" not in source
    assert gc.HARNESSES[GUARD_CLASS].mode_under_test == "enforce"


def test_held_units_stay_out():
    """D02 has no units check; D19 (the executor's feed) is held for every class."""
    assert not {"D02", "D19"} & set(CASE_IDS)
    d02 = next(u for u in descriptors.load_units() if u.unit_id == "D02")
    assert d02.guards[GUARD_CLASS]["state"] == "absent"


def test_the_call_site_mirror_matches_weekly_collector():
    """The case calls `_phase_collect` with the phase-one call site's own arguments."""
    source = (ef.REPO_ROOT / "weekly_collector.py").read_text()
    match = re.search(
        r'const_result = _phase_collect\(\n\s*reg, "constituents",(?P<block>.*?)\n {8}\)\n',
        source, flags=re.DOTALL,
    )
    assert match, "the phase-one constituents call site was not found"
    block = match.group("block")
    assert 'artifact_key=f"{market_prefix}weekly/{run_date}/constituents.json"' in block
    assert "supports_auto_skip" not in block
    for key, _present, _rows in _extra_outputs():
        literal = key.replace(MARKET_PREFIX, "{market_prefix}")
        assert literal in block, f"{key!r} is not in the call site's extra_outputs"
    assert len(re.findall(r'\(f?"[^"]+", lambda r: True,', block)) == len(_extra_outputs())


@pytest.mark.parametrize("unit", CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(unit, tmp_path):
    """``faults/<unit>/units/latest.json`` with ``outcome: induced`` reads MET."""
    case = next(c for c in CASES if c.unit == unit)
    descriptor = next(u for u in descriptors.load_units() if u.unit_id == unit)
    store = LocalStore(tmp_path)
    day = dt.date.fromisoformat(RUN_DATE)
    before = evidence.read_guard_commissioning(store, descriptor, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    harness = gc.HARNESSES[GUARD_CLASS]
    document = gc.record_document(
        unit, GUARD_CLASS, fault=case.fault, test=harness.nodeid(unit), code_sha="c" * 40,
    )
    assert document["observed"] == list(gc.UNITS_OBSERVED)
    store.put_bytes(gc.record_key(unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(store, descriptor, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def test_the_publisher_names_this_files_nodes():
    """The publisher runs these exact nodes; a rename here would publish nothing, loudly."""
    harness = gc.HARNESSES[GUARD_CLASS]
    assert harness.test_file == "tests/test_guard_commissioning_units.py"
    assert harness.test_name == test_the_fault_record_unit_scaled_column_is_refused_and_never_published.__name__
    for case in CASES:
        assert gc._fault_for(case.unit, GUARD_CLASS) == case.fault

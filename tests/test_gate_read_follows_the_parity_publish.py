"""The gate read follows the parity publish, and a stale report is UNMEASURABLE.

`alpha-engine-config-I11355`. The cutover gate's cron fires at 23:30 UTC; the
same-day parity report publishes at ~23:43 UTC (`shadow-sameday` launches
22:30:34Z and ran 73 minutes on its first scheduled night). Tonight's
`gates/data-cutover-ready/2026-09-21/gate.json` — written at 23:09Z by the S3
timestamp, GHA cron start drift — read:

    data.cutover_ready.parity: 922/959 published keys match
    (report parity/2026-09-18.json, trading_day 2026-09-18)

while `parity/2026-09-21.json` landed at 23:43:04Z. Every weekday reading was
structurally one report behind, so `-I11233` closes-when 2 — "the parity clause
reads a report whose trading day IS the gate's own trading day" — could never
be satisfied by that ordering, and `-I11352`'s 07:45 ET morning rewrite makes
the 23:30 read stale twice a day.

Three properties are pinned here:

1. a reading whose report is for another day grades UNMEASURABLE, naming both
   dates — never MET, and never UNMET on yesterday's numbers;
2. the freshness window stays the OUTER bound and still reads UNMET, because a
   report days old is a finding about cutover readiness rather than a gap in
   the read;
3. the reads that follow the publish run after midnight UTC and file under the
   session they are actually about — including on a Saturday, where the naive
   `previous_trading_day(today)` returns Thursday.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest
import yaml

from data_gate import evidence, read as read_module
from data_gate.__main__ import resolve_trading_day

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "data-gate.yml"
POST_PARITY_CRONS = {"30 1 * * 2-6", "30 12 * * 1-5"}


def _workflow() -> dict:
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return doc


# ---------------------------------------------------------------------------
# 1 / 2 — staleness grading
# ---------------------------------------------------------------------------


class _Store:
    uri = "file://test"

    def __init__(self, documents: dict[str, dict]) -> None:
        self.documents = documents

    def list_keys(self, prefix: str = ""):
        return [k for k in sorted(self.documents) if k.startswith(prefix)]

    def get_bytes(self, key: str) -> bytes:
        if key not in self.documents:
            raise FileNotFoundError(key)
        return json.dumps(self.documents[key]).encode("utf-8")


def _report(day: dt.date, *, total: int = 10, match: int = 10, settling: int = 0) -> dict:
    return {
        "schema_version": "data_parity_report.v2",
        "trading_day": day.isoformat(),
        "generated_at": f"{day.isoformat()}T23:43:04Z",
        "met": match == total,
        "summary": {"total": total, "match": match, "settling_bar_keys": settling},
        "keys": [],
    }


GATE_DAY = dt.date(2026, 9, 21)
PRIOR = dt.date(2026, 9, 18)


def test_a_report_for_another_day_is_unmeasurable_naming_both_dates():
    store = _Store({evidence.parity_store_key(PRIOR): _report(PRIOR)})
    reading = evidence.read_parity(store, trading_day=GATE_DAY)
    assert reading.unmeasurable is True
    assert reading.met is False
    assert "2026-09-18" in reading.detail and "2026-09-21" in reading.detail
    assert "UNMEASURABLE" in reading.detail


def test_a_report_for_the_gates_own_day_grades_normally():
    store = _Store({evidence.parity_store_key(GATE_DAY): _report(GATE_DAY)})
    met = evidence.read_parity(store, trading_day=GATE_DAY)
    assert met.unmeasurable is False and met.met is True

    store = _Store({evidence.parity_store_key(GATE_DAY): _report(GATE_DAY, match=9)})
    unmet = evidence.read_parity(store, trading_day=GATE_DAY)
    assert unmet.unmeasurable is False and unmet.met is False


def test_the_settling_split_is_on_the_gate_row():
    """`alpha-engine-config-I11351` deliverable 3, read through the clause the
    daily Telegram report renders."""
    store = _Store(
        {evidence.parity_store_key(GATE_DAY): _report(GATE_DAY, total=953, match=953, settling=920)}
    )
    reading = evidence.read_parity(store, trading_day=GATE_DAY)
    assert "953/953 published keys match (920 settling-only, 0 mismatch)" in reading.detail


def test_the_freshness_window_stays_the_outer_bound_and_stays_unmet():
    """A report older than the window means no shadow run is happening at all
    — a finding about cutover readiness, not a gap in this read. It must NOT
    be swallowed by the new UNMEASURABLE branch."""
    from nousergon_lib.trading_calendar import subtract_trading_days

    stale = subtract_trading_days(GATE_DAY, evidence.PARITY_FRESHNESS_TRADING_DAYS + 1)
    store = _Store({evidence.parity_store_key(stale): _report(stale)})
    reading = evidence.read_parity(store, trading_day=GATE_DAY)
    assert reading.unmeasurable is False
    assert reading.met is False
    assert "Stale" in reading.detail


def test_no_report_at_all_is_still_unmet_not_unmeasurable():
    reading = evidence.read_parity(_Store({}), trading_day=GATE_DAY)
    assert reading.unmeasurable is False
    assert reading.met is False


# ---------------------------------------------------------------------------
# 3 — which trading day a post-publish read files under
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "today, expected",
    [
        # 01:30 UTC Tuesday, after Monday's ~23:43 UTC same-day publish.
        (dt.date(2026, 9, 22), dt.date(2026, 9, 21)),
        # 01:30 UTC SATURDAY, after Friday's publish. The case that makes this
        # a named rule: `previous_trading_day(latest_on_or_before(Saturday))`
        # is `previous_trading_day(Friday)` = THURSDAY, one session too far.
        (dt.date(2026, 9, 26), dt.date(2026, 9, 25)),
        # 12:30 UTC Monday, after the 11:45Z shadow-morning rewrite of the
        # FRIDAY session's report.
        (dt.date(2026, 9, 28), dt.date(2026, 9, 25)),
    ],
)
def test_yesterday_resolves_to_the_session_a_post_midnight_read_is_about(today, expected):
    assert resolve_trading_day("yesterday", today=today) == expected


def test_today_and_an_explicit_date_are_unchanged():
    from data_gate.cadence import latest_trading_day_on_or_before

    today = dt.date(2026, 9, 22)
    assert resolve_trading_day(None, today=today) == latest_trading_day_on_or_before(today)
    assert resolve_trading_day("today", today=today) == latest_trading_day_on_or_before(today)
    assert resolve_trading_day("2026-09-18", today=today) == dt.date(2026, 9, 18)


# ---------------------------------------------------------------------------
# The trigger, recorded on the dated reading
# ---------------------------------------------------------------------------


def test_the_trigger_is_recorded_on_the_dated_gate_reading(tmp_path):
    from data_gate.store import LocalStore
    from nousergon_lib.gates import gate_key

    store = LocalStore(tmp_path)
    read_module.run(store, gate="data-phase0", trading_day=GATE_DAY, trigger="post-parity")
    document = json.loads(
        (tmp_path / gate_key("data-phase0", GATE_DAY.isoformat())).read_text(encoding="utf-8")
    )
    assert document["trigger"] == "post-parity"


def test_an_undeclared_trigger_is_refused_rather_than_published(tmp_path):
    from data_gate.store import LocalStore

    with pytest.raises(ValueError, match="unknown trigger"):
        read_module.run(
            LocalStore(tmp_path), gate="data-phase0", trading_day=GATE_DAY, trigger="whenever"
        )


def test_parity_published_is_declared_alongside_the_cron_triggers():
    """`shadow parity --dispatch-gate` emits `parity-published`
    (`alpha-engine-config-I11361`, pinned end to end in
    `test_parity_gate_dispatch.py`); the post-parity crons stay as the backstop,
    so the event artifact and the cron artifact must be the same shape."""
    assert "parity-published" in read_module.TRIGGERS
    assert {"schedule", "post-parity", "push", "manual"} <= read_module.TRIGGERS


# ---------------------------------------------------------------------------
# The workflow wiring
# ---------------------------------------------------------------------------


def test_the_two_post_publish_crons_exist_alongside_the_backstop():
    on_block = _workflow().get("on") or _workflow().get(True)
    crons = {entry["cron"] for entry in on_block["schedule"]}
    assert POST_PARITY_CRONS <= crons
    assert {"30 23 * * *", "0 18 * * 6"} <= crons


def test_every_gate_read_passes_the_trigger_and_the_trading_day():
    """Six reads, one wiring. A read that forgot either would publish a
    reading filed under the wrong day, or with no trigger to tell a
    pre-publish reading from a post-publish one."""
    doc = _workflow()
    steps = doc["jobs"]["read-gate"]["steps"]
    reads = [s for s in steps if "python -m data_gate read" in (s.get("run") or "")]
    assert len(reads) == 6
    for step in reads:
        assert '--trigger "$GATE_TRIGGER"' in step["run"], step.get("name")
        assert '--trading-day "$GATE_TRADING_DAY"' in step["run"], step.get("name")


def test_the_post_publish_crons_are_the_only_ones_filing_under_yesterday():
    """The two env expressions must key off the SAME two cron strings, or a
    post-publish read files under the wrong day while claiming
    `post-parity` — the exact confusion this issue removes."""
    env = _workflow()["jobs"]["read-gate"]["env"]
    for expression in (env["GATE_TRIGGER"], env["GATE_TRADING_DAY"]):
        for cron in POST_PARITY_CRONS:
            assert cron in expression
    assert "post-parity" in env["GATE_TRIGGER"]
    assert "yesterday" in env["GATE_TRADING_DAY"]


def test_the_post_publish_crons_run_after_the_publishes_they_follow():
    """23:43 UTC same-day publish -> 01:30 UTC next day; 11:45 UTC morning
    rewrite -> 12:30 UTC. Both gaps are asserted so a cadence change that
    reintroduces the race fails here rather than on the board."""
    minutes = {}
    for cron in POST_PARITY_CRONS:
        minute, hour = cron.split()[0], cron.split()[1]
        minutes[cron] = int(hour) * 60 + int(minute)
    assert minutes["30 1 * * 2-6"] == 90  # 01:30 UTC, ~1h47m after 23:43 UTC
    assert minutes["30 12 * * 1-5"] == 750  # 12:30 UTC, 45m after 11:45 UTC


def test_the_job_timeout_covers_six_reads_at_the_measured_duration():
    """Six sequential reads at the 4.0 min measured on 2026-09-23 plus the
    install need ~25 min; the old 15-minute timeout cancelled every read that
    day before `data-phase3` and the reliability gate ran, and a cancellation
    pages nothing. Keep headroom above the measured worst case."""
    job = _workflow()["jobs"]["read-gate"]
    reads = [s for s in job["steps"] if "python -m data_gate read" in (s.get("run") or "")]
    measured_worst_read_minutes = 4.0
    install_minutes = 1.0
    assert job["timeout-minutes"] >= len(reads) * measured_worst_read_minutes + install_minutes


# ---------------------------------------------------------------------------
# alpha-engine-config-I11269 — after the cutover the clause is FROZEN
# ---------------------------------------------------------------------------

from data_gate import cutover as _cutover  # noqa: E402

CUTOVER_DAY = _cutover.cutover_trading_day()
_AFTER = dt.date(2026, 10, 2)  # a trading day well after the cutover


def test_the_freeze_dates_are_the_committed_cutover():
    assert CUTOVER_DAY == dt.date(2026, 9, 28)
    assert _AFTER > CUTOVER_DAY


def test_after_the_cutover_the_last_pre_cutover_report_is_graded_frozen():
    """A post-cutover report compares the collector with itself (v1 no longer
    writes the keys) — it is ignored, even though it is newer and reads 100%."""
    post = dt.date(2026, 10, 1)
    store = _Store({
        evidence.parity_store_key(CUTOVER_DAY): _report(CUTOVER_DAY, match=9),
        evidence.parity_store_key(post): _report(post),
        evidence.parity_store_key(_AFTER): _report(_AFTER),
    })
    reading = evidence.read_parity(store, trading_day=_AFTER)
    assert reading.unmeasurable is False
    assert reading.met is False  # the pre-cutover report's own 9/10
    assert reading.evidence == (evidence.parity_store_key(CUTOVER_DAY),)
    assert "FROZEN" in reading.detail and _cutover.CUTOVER_UTC in reading.detail


def test_the_frozen_reading_is_not_subject_to_same_day_or_freshness():
    """Weeks after the cutover the frozen report is 'stale' by both live rules;
    neither applies, because there is no live comparison left to keep current."""
    last = dt.date(2026, 9, 25)
    store = _Store({evidence.parity_store_key(last): _report(last)})
    reading = evidence.read_parity(store, trading_day=dt.date(2026, 11, 20))
    assert reading.unmeasurable is False and reading.met is True
    assert "FROZEN" in reading.detail


def test_no_pre_cutover_report_is_unmet_after_the_cutover():
    store = _Store({evidence.parity_store_key(_AFTER): _report(_AFTER)})
    reading = evidence.read_parity(store, trading_day=_AFTER)
    assert reading.met is False and reading.unmeasurable is False
    assert "no parity report at or before the cutover" in reading.detail


def test_on_the_cutover_day_itself_the_live_rules_still_apply():
    """The cutover day's own report compares against v1 output written by the
    OLD definition that afternoon — still a live, same-day reading."""
    prior = dt.date(2026, 9, 25)
    store = _Store({evidence.parity_store_key(prior): _report(prior)})
    reading = evidence.read_parity(store, trading_day=CUTOVER_DAY)
    assert reading.unmeasurable is True  # one report behind: the I11355 rule
    assert "FROZEN" not in reading.detail

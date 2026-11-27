# Existing fixture doubles intentionally accept heterogeneous store and tracker values.
# ruff: noqa: ANN001, ANN002, ANN003
"""`alpha-engine-config-I10952` — the data collector's daily update.

Principle 7 is the deliverable, so most of what is asserted here is a rendering
rule rather than a feature: **no data is never rendered as green.** The three
ways that gets broken each have a named test below, and each is a LIVE case
rather than a defensive one —

* `gates/data-phase1/` was measured EMPTY on 2026-09-17T05:19:27Z while
  `gates/ladder.json` already read `current_phase: data-phase1`. The rung the
  ladder says we are on has never had a dated reading written. It must render
  ABSENT with the reason, never as zero movement and never as green.
* S3 returns 403 for a MISSING key when the caller also lacks `s3:ListBucket`
  on the prefix, so absent and denied are genuinely confusable and must not be.
* `0/0` is the shape of a green number. A rung that grades nothing is
  UNMEASURED, not complete.

The workflow-shape tests exist because a cron literal and the constant the code
reasons about are two copies of one fact. Note the PyYAML detail: the bare key
`on` resolves to the BOOLEAN `True` under the 1.1 resolver, so the schedule is
read out of `spec[True]["schedule"]`.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib
from io import BytesIO
from typing import Any, Never
from zoneinfo import ZoneInfo

import pytest
import yaml

from data_gate import report as report_module

REPO = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = REPO / ".github" / "workflows" / "data-report.yml"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _schedule(spec: dict) -> list[dict]:
    # YAML 1.1 resolves the bare key `on` to the boolean True. `.get("on")` is
    # kept as the fallback so a PyYAML version that stops doing so does not
    # make this test silently read an empty trigger block and pass.
    triggers = spec.get("on", spec.get(True))
    return triggers["schedule"]


# ── the cron is one fact, written twice ──────────────────────────────────────


def test_the_workflow_declares_exactly_the_cron_the_code_declares() -> None:
    crons = [entry["cron"] for entry in _schedule(_workflow())]
    assert crons == [report_module.DELIVERY_CRON_UTC], (
        "the workflow's schedule and `data_gate.report.DELIVERY_CRON_UTC` are two "
        "copies of one fact; a second cron here would also deliver twice a day "
        "against one manifest key."
    )


def test_the_declared_cron_resolves_to_0330_pdt_and_0230_pst() -> None:
    """The November drift is DECLARED, not discovered.

    GitHub Actions crons are UTC and carry no timezone, so the local instant
    moves when the US leaves daylight time. Asserting both resolutions makes
    that a committed fact a reader can look up rather than a surprise.
    """
    minute, hour = (int(part) for part in report_module.DELIVERY_CRON_UTC.split()[:2])
    pacific = ZoneInfo("America/Los_Angeles")

    summer = dt.datetime(2026, 9, 17, hour, minute, tzinfo=dt.timezone.utc).astimezone(pacific)
    winter = dt.datetime(2026, 11, 2, hour, minute, tzinfo=dt.timezone.utc).astimezone(pacific)

    assert (summer.hour, summer.minute, summer.tzname()) == (3, 30, "PDT")
    assert (winter.hour, winter.minute, winter.tzname()) == (2, 30, "PST")


def test_the_cron_is_set_early_enough_for_the_measured_delivery_lag() -> None:
    """`alpha-engine-config-I9966`: the declared cron is an INPUT to delivery
    time. Every scheduled workflow on this account fires 3-5h late, measured
    and chronic, so a naive 06:30-PT literal (`30 13`) would deliver near
    09:30 PT. This asserts the subtraction was actually made."""
    hour = int(report_module.DELIVERY_CRON_UTC.split()[1])
    naive_intended_utc_hour = 13  # 06:30 PDT
    assert naive_intended_utc_hour - hour == 3


# ── the public repo carries no infrastructure identifier ─────────────────────


def test_the_workflow_carries_no_account_id_or_bucket_literal() -> None:
    """`data-gate.yml` hardcodes both in this PUBLIC repo; this file must not
    inherit that form (`alpha-engine-config-I10156`). The correction to
    `data-gate.yml` is filed separately as `alpha-engine-config-I10973`, which
    also widens this into a class-level guard over the whole directory — this
    one covers the new file."""
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    body = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )
    assert "711398986525" not in body
    assert "s3://" not in body
    assert "alpha-engine-research" not in body
    assert "${{ vars.AWS_ACCOUNT_ID }}" in body
    assert "${{ vars.DATA_STORE_URI }}" in body


def test_the_account_id_is_masked_before_any_step_can_print_it() -> None:
    """GitHub masks SECRETS and not repository VARIABLES, and this repo's run
    logs are public. The mask must precede the credential step, because the
    identity read-back prints an assumed-role ARN embedding the account id."""
    steps = _workflow()["jobs"]["report"]["steps"]
    names = [str(step.get("name", "")) for step in steps]
    mask = next(i for i, step in enumerate(steps) if "::add-mask::" in str(step.get("run", "")))
    credentials = next(
        i for i, step in enumerate(steps) if "configure-aws-credentials" in str(step.get("uses", ""))
    )
    assert mask < credentials, names


def test_the_reporter_does_not_run_as_the_gate_reader() -> None:
    """A reporting surface that could write the gate artifacts would be able to
    author the reading it reports."""
    spec = _workflow()
    assert spec["env"]["REPORT_ROLE_ARN"].endswith(":role/github-actions-data-report")
    # The gate reader's role name appears once, inside the identity read-back's
    # operator message, which is prose. What must not name it is the ARN the
    # job actually assumes.
    assumed = [
        step
        for step in spec["jobs"]["report"]["steps"]
        if "configure-aws-credentials" in str(step.get("uses", ""))
    ]
    assert len(assumed) == 1
    assert assumed[0]["with"]["role-to-assume"] == "${{ env.REPORT_ROLE_ARN }}"


# ── the trading-day axis and the key shape ───────────────────────────────────


@pytest.mark.parametrize(
    ("calendar_date", "expected"),
    [
        ("2026-09-19", "2026-09-18"),  # Saturday -> Friday
        ("2026-09-20", "2026-09-18"),  # Sunday   -> Friday
        ("2026-09-21", "2026-09-18"),  # Monday   -> Friday
        ("2026-09-22", "2026-09-21"),  # Tuesday  -> Monday
    ],
)
def test_sat_sun_and_mon_all_resolve_to_the_same_friday(calendar_date, expected) -> None:
    resolved = report_module.previous_trading_day(dt.date.fromisoformat(calendar_date))
    assert resolved.isoformat() == expected


def test_the_calendar_date_is_the_discriminator_in_the_output_key() -> None:
    """Three calendar days share one trading day and the cron fires on all
    three. Without the calendar segment the weekend's two reports would be
    overwritten and the Monday one would look like the complete record."""
    keys = {
        report_module.manifest_key("run.json", "2026-09-18", day)
        for day in ("2026-09-19", "2026-09-20", "2026-09-21")
    }
    assert len(keys) == 3
    assert (
        report_module.manifest_key("message.txt", "2026-09-18", "2026-09-21")
        == "runs/report.daily/2026-09-18/2026-09-21/message.txt"
    )


def test_an_empty_key_segment_is_refused_rather_than_collapsed() -> None:
    with pytest.raises(ValueError):
        report_module.manifest_key("run.json", "2026-09-18", "")


# ── principle 7: what nothing renders as ─────────────────────────────────────


def test_an_empty_clause_list_renders_no_clauses_never_zero_over_zero() -> None:
    assert report_module._fraction(0, 0) == report_module.NO_CLAUSES
    assert report_module._fraction(5, 0) == report_module.NO_CLAUSES
    assert report_module._fraction(154, 248) == "154/248"


# ── the fixtures the rendering tests read through ────────────────────────────

LADDER = {
    "ladder": "data",
    "current_phase": "data-phase1",
    "generated_utc": "2026-09-17T05:19:27Z",
    "phases": [
        {
            "phase": "data-phase0",
            "gate": "data-phase0",
            "state": "MET",
            "clauses_met": 5,
            "clauses_total": 5,
        },
        {
            "phase": "data-phase1",
            "gate": "data-phase1",
            "state": "UNMEASURABLE",
            "clauses_met": 154,
            "clauses_total": 248,
        },
    ],
}

BOARD = {
    "schema_version": "data_board.v1",
    "clauses_met": 202,
    "clauses_unmet": 355,
    "clauses_total": 649,
    "clauses_retired": 60,
    "clauses_unconnected": 10,
    "transparency_gap": 92,
}

NOW = dt.datetime(2026, 9, 17, 10, 40, tzinfo=dt.timezone.utc)


class FakeStore:
    """Keys to bytes, plus a set of keys that answer `AccessDenied`.

    The denied set exists because a denial is not simulable by removing a key:
    that is the whole confusion under test.
    """

    uri = "s3://example-bucket/data_collection"

    def __init__(self, documents: dict, denied: set[str] | None = None) -> None:
        self.documents = documents
        self.denied = denied or set()
        self.written: dict[str, bytes] = {}

    def get_bytes(self, key: str) -> bytes:
        if key in self.denied:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "GetObject"
            )
        if key in self.documents:
            return json.dumps(self.documents[key]).encode("utf-8")
        if key in self.written:
            return self.written[key]
        raise FileNotFoundError(key)

    def list_keys(self, prefix: str = "") -> list[str]:
        seen = set(self.documents) | set(self.written) | self.denied
        return [key for key in sorted(seen) if key.startswith(prefix)]

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.written[key] = payload


def _inputs(
    store: FakeStore, *, now: dt.datetime = NOW
) -> report_module.DataReportInputs:
    return report_module.read_inputs(
        store,
        trading_day=dt.date(2026, 9, 16),
        calendar_date=dt.date(2026, 9, 17),
        now=now,
        console_url="https://console.example",
    )


def _live_shaped_store(**extra) -> FakeStore:
    documents = {
        "gates/ladder.json": LADDER,
        "gates/board/latest.json": BOARD,
        "gates/data-phase0/2026-09-17/gate.json": {
            "gate": "data-phase0",
            "clauses": [{"name": "data.inventory.writers_declared", "met": True}],
        },
    }
    documents.update(extra)
    return FakeStore(documents)


def test_a_rung_with_no_dated_history_renders_absent_and_never_zero() -> None:
    """The LIVE 2026-09-17 case: `current_phase` is `data-phase1` and
    `gates/data-phase1/` holds nothing at all."""
    inputs = _inputs(_live_shaped_store())

    assert inputs.moved is None
    assert inputs.moved_absent is not None
    assert "ABSENT" in inputs.moved_absent
    assert "data-phase1" in inputs.history_absent

    update = report_module.render_full_update(inputs, now=NOW)
    message = report_module.render_message(inputs, now=NOW, update_url="https://t/1")
    for document in (update, message):
        assert "ABSENT" in document
        assert "0/0" not in document
    assert "no clause changed state" not in message


def test_a_denied_dated_reading_is_never_rendered_as_an_absence() -> None:
    """S3 answers 403 for a missing key when the caller also lacks ListBucket.
    Rendering that as "not there yet" hides an IAM gap behind a normal report."""
    store = _live_shaped_store()
    store.documents["gates/data-phase1/2026-09-16/gate.json"] = {"gate": "data-phase1", "clauses": []}
    store.denied.add("gates/data-phase1/2026-09-16/gate.json")

    inputs = _inputs(store)
    update = report_module.render_full_update(inputs, now=NOW)

    assert "DENIED" in update
    assert "AccessDenied" in update
    assert "access gap" in update


def test_one_dated_reading_is_cannot_say_and_not_nothing_moved() -> None:
    store = _live_shaped_store(
        **{
            "gates/data-phase1/2026-09-17/gate.json": {
                "gate": "data-phase1",
                "clauses": [{"name": "data.D01.run_record", "met": False}],
            }
        }
    )
    inputs = _inputs(store)
    assert inputs.moved is not None
    assert inputs.moved.cannot_say
    assert "cannot say" in report_module.render_full_update(inputs, now=NOW)


def test_an_unmeasurable_clause_never_reduces_to_unmet() -> None:
    """UNMEASURABLE ranks worse than UNMET: "we could not read this" must not
    render as "we read it and it said no"."""
    rows = report_module._normalize_gate_rows(
        {"clauses": [{"name": "c", "met": False, "unmeasurable": True}]}
    )
    assert rows == {"rows": [{"id": "c", "state": "UNMEASURABLE"}]}


def test_a_stale_ladder_puts_the_staleness_line_first_in_both_documents() -> None:
    """A staleness notice below the numbers is read after the numbers have
    already been believed."""
    store = _live_shaped_store()
    late = NOW + dt.timedelta(days=3)

    inputs = _inputs(store, now=late)
    assert inputs.staleness_line is not None

    update = report_module.render_full_update(inputs, now=late)
    message = report_module.render_message(inputs, now=late, update_url="https://t/1")

    assert update.splitlines()[0].startswith("**")
    assert inputs.staleness_line in update.splitlines()[0]
    assert message.splitlines()[0].startswith("<b>")


def test_a_fresh_ladder_carries_no_staleness_line() -> None:
    inputs = _inputs(_live_shaped_store())
    assert inputs.staleness_line is None
    assert "STALE" not in report_module.render_full_update(inputs, now=NOW)


def test_the_report_quotes_the_board_totals_it_did_not_compute() -> None:
    update = report_module.render_full_update(_inputs(_live_shaped_store()), now=NOW)
    assert "202/649" in update
    assert "92" in update


def test_the_board_line_mentions_standing_when_present() -> None:
    board = dict(BOARD, clauses_standing=1)
    assert "1 standing" in report_module._board_line(board)


def test_the_board_line_defaults_standing_to_zero_when_absent() -> None:
    """`alpha-engine-config-I10793`/`-I10788`: a board document from before
    this landed carries no `clauses_standing` key at all — the report must
    not raise on it."""
    assert "0 standing" in report_module._board_line(BOARD)


def test_the_full_update_surfaces_a_standing_row_with_its_real_state() -> None:
    board = dict(
        BOARD,
        clauses_standing=1,
        rows=[
            {
                "clause": "data.phase1.cost_baseline_measured",
                "state": "UNMET",
                "standing": True,
                "detail": "metrics/cost/monthly/latest.json: no cost document",
            }
        ],
    )
    inputs = _inputs(_live_shaped_store(**{"gates/board/latest.json": board}))
    update = report_module.render_full_update(inputs, now=NOW)
    assert "### standing (measured daily, gates no phase)" in update
    assert "data.phase1.cost_baseline_measured" in update
    assert "UNMET" in update


def test_the_full_update_has_no_standing_section_when_none_are_standing() -> None:
    inputs = _inputs(_live_shaped_store())
    update = report_module.render_full_update(inputs, now=NOW)
    assert "### standing" not in update


def test_an_unreadable_ladder_raises_rather_than_reporting_an_empty_board() -> None:
    """A report that could not read the ladder has nothing to say. Rendering a
    message about an absent ladder is a surface reporting its own outage as the
    system's state."""
    with pytest.raises(Exception):
        _inputs(FakeStore({"gates/board/latest.json": BOARD}))


# ── the exit-code contract ───────────────────────────────────────────────────


def test_the_report_exit_code_never_carries_the_gate_verdict(monkeypatch) -> None:
    """0 = the update went out, 2 = it could not. `alpha-engine-config-I10906`:
    a verdict in the exit code makes a genuine reporter break indistinguishable
    from a phase that is red by design."""
    from data_gate import __main__ as cli

    monkeypatch.setattr(
        report_module,
        "run_report",
        lambda *a, **k: {
            "status": "ok",
            "trading_day": "2026-09-16",
            "calendar_date": "2026-09-17",
            "trigger": "schedule",
            "update_url": "https://t/1",
        },
    )
    monkeypatch.setattr(cli, "open_store", lambda *a, **k: FakeStore({}))
    assert cli.main(["report", "--store", "/tmp/store"]) == 0


def test_a_report_that_could_not_be_delivered_exits_two(monkeypatch) -> None:
    from data_gate import __main__ as cli

    def _boom(*a, **k) -> Never:
        raise RuntimeError("telegram refused the publish")

    monkeypatch.setattr(report_module, "run_report", _boom)
    monkeypatch.setattr(cli, "open_store", lambda *a, **k: FakeStore({}))
    assert cli.main(["report", "--store", "/tmp/store"]) == 2


def test_the_report_subcommand_has_no_fail_on_unmet_flag() -> None:
    """There is no reading here for a verdict flag to fail on."""
    from data_gate.__main__ import _parser

    with pytest.raises(SystemExit):
        _parser().parse_args(["report", "--store", "/tmp/store", "--fail-on-unmet"])


# ── the whole job, end to end ────────────────────────────────────────────────


class _RecordingTracker:
    """A stand-in for `nousergon_lib.gates.tracker.Tracker`.

    The real one refuses to act without a credential, which is correct — but it
    means any end-to-end test that does not substitute it is measuring the
    credential check rather than the job. Two tests below did exactly that, and
    one of them would have reached the real `deliver()` had the tracker not
    raised first (alpha-engine-config-I10952, caught against v0.124.133).
    """

    def __init__(self) -> None:
        self.comments = []

    def find_or_create_issue(self, *, title, body) -> int:
        return 7

    def post_comment(self, issue, body) -> str:
        self.comments.append((issue, body))
        return "https://github.com/nousergon/alpha-engine-config/issues/7#issuecomment-1"

    def update_issue_body(self, issue, body) -> None:
        return None


def test_a_run_files_every_declared_key_and_names_its_trigger_as_a_key(monkeypatch) -> None:
    """The closes-when predicate asks whether `trigger.schedule` EXISTS as a
    key. A trigger recorded only as a field inside `run.json` cannot be
    asserted by a key listing, so it is written both ways."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setattr(report_module, "_tracker", _RecordingTracker)
    monkeypatch.setattr(report_module, "deliver", lambda *a, **k: None)
    store = _live_shaped_store()

    manifest = report_module.run_report(
        store,
        trading_day=dt.date(2026, 9, 18),
        calendar_date=dt.date(2026, 9, 21),
        now=NOW,
        console_url="https://console.example",
    )

    assert manifest["status"] == "ok"
    assert manifest["trigger"] == "schedule"
    prefix = "runs/report.daily/2026-09-18/2026-09-21/"
    for basename in ("message.txt", "update.md", "history_row.json", "trigger.schedule", "run.json"):
        assert prefix + basename in store.written, basename


def test_a_failed_delivery_still_files_a_manifest_that_says_so(monkeypatch) -> None:
    """A job that dies without filing one is indistinguishable from a job that
    never ran, and the absence detector would then page for the wrong reason."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setattr(report_module, "_tracker", _RecordingTracker)
    monkeypatch.setattr(
        report_module,
        "deliver",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("the transport refused it")),
    )
    store = _live_shaped_store()

    with pytest.raises(RuntimeError):
        report_module.run_report(
            store,
            trading_day=dt.date(2026, 9, 18),
            calendar_date=dt.date(2026, 9, 21),
            now=NOW,
        )

    filed = json.loads(store.written["runs/report.daily/2026-09-18/2026-09-21/run.json"])
    assert filed["status"] == "failed"
    assert "the transport refused it" in filed["reason"]


def test_the_tracker_comment_is_posted_before_the_headline_is_rendered(monkeypatch) -> None:
    """The headline's indispensable content is that comment's permalink, so a
    tracker that refuses the comment must stop the message going out entirely
    rather than produce a headline pointing at nothing."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    delivered = []
    monkeypatch.setattr(report_module, "deliver", lambda *a, **k: delivered.append(a))

    class _RefusingTracker:
        def find_or_create_issue(self, *, title, body) -> int:
            return 7

        def post_comment(self, issue, body) -> Never:
            raise RuntimeError("the tracker refused the comment")

        def update_issue_body(self, issue, body) -> Never:  # pragma: no cover - never reached
            raise AssertionError("the body must not be regenerated after a refusal")

    monkeypatch.setattr(report_module, "_tracker", _RefusingTracker)

    with pytest.raises(RuntimeError):
        report_module.run_report(
            store := _live_shaped_store(),
            trading_day=dt.date(2026, 9, 18),
            calendar_date=dt.date(2026, 9, 21),
            now=NOW,
        )

    assert delivered == []
    assert "runs/report.daily/2026-09-18/2026-09-21/message.txt" not in store.written


# ── alpha-engine-config-I11353: a failed run with no readable log is RED ─────
#
# 2026-09-21: three shadow legs failed, every manifest they wrote recorded
# `log_location: "local:ip-172-31-33-124.ec2.internal:<pid>"` on a host that
# was terminated minutes later, and nothing anywhere said so. The row below is
# rendered every day — including the day it is clean, which is what makes its
# own disappearance visible.


class _Unit:
    """The one property `read_unresolved_logs` reads off a unit descriptor."""

    def __init__(self, unit_id: str) -> None:
        self.unit_id = unit_id
        self.run_manifest_prefix = f"data_collection/runs/{unit_id}"


_UNITS = [_Unit("D17"), _Unit("D18")]
_DAY = dt.date(2026, 9, 21)


def _manifest(
    status: str = "failed",
    log_location: str = "local:ip-172-31-33-124.ec2.internal:4242",
) -> dict[str, str]:
    return {
        "schema_version": "data_run_manifest.v1",
        "unit_id": "D17",
        "status": status,
        "log_location": log_location,
    }


def test_a_failed_run_pointing_at_a_dead_host_is_a_named_finding() -> None:
    store = FakeStore({"runs/D17/2026-09-21/01ABC.json": _manifest()})
    found, problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert problems == []
    assert len(found) == 1
    assert found[0].unit_id == "D17"
    assert found[0].manifest_key == "runs/D17/2026-09-21/01ABC.json"
    assert "dies with the box" in found[0].reason


def test_a_failed_run_whose_shipped_log_exists_is_not_a_finding() -> None:
    uri = "s3://example-bucket/data_collection/logs/shadow-sameday/2026-09-21/i-x.log"
    store = FakeStore(
        {
            "runs/D17/2026-09-21/01ABC.json": _manifest(log_location=uri),
            "logs/shadow-sameday/2026-09-21/i-x.log": {"_": "bytes stand in for a log"},
        }
    )
    found, problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert (found, problems) == ([], [])


def test_a_gzipped_shipped_log_is_not_a_finding() -> None:
    """alpha-engine-config-I11359: `krepis.spot_bootstrap.RunLog(gzip=True)`'s
    final trap writes `<s3_uri>.gz` and DELETES the plain object — the
    manifest still names the plain key (decided before the run started), so
    only the `.gz` sibling exists on disk. That must resolve, not read as
    "never shipped"."""
    uri = "s3://example-bucket/data_collection/logs/shadow-sameday/2026-09-21/i-x.log"
    store = FakeStore(
        {
            "runs/D17/2026-09-21/01ABC.json": _manifest(log_location=uri),
            "logs/shadow-sameday/2026-09-21/i-x.log.gz": {"_": "gzip bytes stand-in"},
        }
    )
    found, problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert (found, problems) == ([], [])


def test_a_manifest_naming_an_s3_key_that_is_not_there_is_a_finding() -> None:
    """The loudest case: the manifest looks resolvable and is not."""
    uri = "s3://example-bucket/data_collection/logs/shadow-sameday/2026-09-21/i-x.log"
    store = FakeStore({"runs/D17/2026-09-21/01ABC.json": _manifest(log_location=uri)})
    found, _problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert len(found) == 1
    assert "was never shipped" in found[0].reason


def test_a_denied_log_read_is_a_finding_and_says_denied() -> None:
    """Absent and denied are different facts with different owners."""
    uri = "s3://example-bucket/data_collection/logs/w/2026-09-21/i-x.log"
    store = FakeStore(
        {"runs/D17/2026-09-21/01ABC.json": _manifest(log_location=uri)},
        denied={"logs/w/2026-09-21/i-x.log"},
    )
    found, _problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert len(found) == 1
    assert "could not be read" in found[0].reason
    assert "AccessDenied" in found[0].reason


def test_an_ok_run_is_not_checked() -> None:
    store = FakeStore({"runs/D17/2026-09-21/01ABC.json": _manifest(status="ok")})
    found, _problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert found == []


def test_a_not_applicable_run_is_not_checked() -> None:
    """A cycle the unit correctly sat out is not a failed run
    (alpha-engine-config-I11583: 783 D37 outside-session skips on 2026-09-28)."""
    store = FakeStore({"runs/D17/2026-09-21/01ABC.json": _manifest(status="not_applicable")})
    found, problems = report_module.read_unresolved_logs(
        store, trading_day=_DAY, units=_UNITS
    )
    assert (found, problems) == ([], [])


def test_a_listing_failure_is_a_named_problem_not_an_empty_result() -> None:
    class Blind(FakeStore):
        def list_keys(self, prefix: str = "") -> Never:
            raise RuntimeError("s3 listing exploded")

    found, problems = report_module.read_unresolved_logs(
        Blind({}), trading_day=_DAY, units=_UNITS
    )
    assert found == []
    assert len(problems) == 2
    assert "listing failed" in problems[0]


def _inputs_with_logs(
    unresolved: Any = (), problems: Any = ()
) -> report_module.DataReportInputs:
    base = _inputs(_live_shaped_store())
    return dataclasses.replace(
        base, unresolved_logs=tuple(unresolved), unresolved_log_problems=tuple(problems)
    )


def test_the_full_update_renders_the_clean_case_as_a_stated_none() -> None:
    """A section that disappears when clean cannot be missed when it vanishes
    for the wrong reason."""
    update = report_module.render_full_update(_inputs_with_logs(), now=NOW)
    assert "### failed runs whose log does not resolve" in update
    assert "names a log this job could read" in update


def test_the_full_update_tables_each_unresolved_log() -> None:
    row = report_module.UnresolvedLog(
        unit_id="D17",
        manifest_key="runs/D17/2026-09-21/01ABC.json",
        log_location="local:ip-172-31-33-124.ec2.internal:4242",
        reason="the box has not been redeployed",
    )
    update = report_module.render_full_update(_inputs_with_logs([row]), now=NOW)
    assert "runs/D17/2026-09-21/01ABC.json" in update
    assert "ip-172-31-33-124" in update


def test_a_blind_check_renders_BLIND_never_a_clean_row() -> None:
    update = report_module.render_full_update(
        _inputs_with_logs(problems=["runs/D17/2026-09-21/: listing failed (RuntimeError: x)"]),
        now=NOW,
    )
    assert "BLIND" in update
    assert "names a log this job could read" not in update


def test_the_headline_carries_the_count() -> None:
    row = report_module.UnresolvedLog("D17", "k", "local:x", "why")
    message = report_module.render_message(
        _inputs_with_logs([row]), now=NOW, update_url="https://example/u"
    )
    assert "1 failed run(s) whose log does not resolve" in message


def test_resolutions_render_for_matching_and_unmatched_problems() -> None:
    matched = report_module.UnresolvedLog("D17", "k1", "local:x", "why")
    unmatched = report_module.UnresolvedLog("D99", "k2", "local:y", "why")
    inputs = dataclasses.replace(
        _inputs_with_logs([matched, unmatched]),
        resolutions=(("unresolved_log:D17", "alpha-engine-config#11353"),),
    )

    update = report_module.render_full_update(inputs, now=NOW)
    message = report_module.render_message(inputs, now=NOW, update_url="https://example/u")

    for rendered in (update, message):
        assert "Resolution: alpha-engine-config#11353" in rendered
        assert "Resolution: none tracked yet" in rendered


def test_s3_registry_precedes_the_bundled_seed_in_both_renderings() -> None:
    registry = {
        "entries": [
            {
                "match": "unresolved_log:*",
                "ref": "nousergon-data#999",
                "plan": "overlay",
            }
        ]
    }
    inputs = _inputs_with_logs(
        [report_module.UnresolvedLog("D17", "k", "local:x", "why")]
    )
    inputs = dataclasses.replace(
        inputs,
        resolutions=report_module._load_resolutions(
            _live_shaped_store(**{report_module.RESOLUTIONS_S3_KEY: registry})
        ),
    )

    assert "Resolution: nousergon-data#999" in report_module.render_full_update(
        inputs, now=NOW
    )
    assert "Resolution: nousergon-data#999" in report_module.render_message(
        inputs, now=NOW, update_url="https://example/u"
    )


def test_unavailable_registry_falls_back_to_the_seed_for_both_renderings() -> None:
    row = report_module.UnresolvedLog("D17", "k", "local:x", "why")
    inputs = _inputs_with_logs([row])

    assert "Resolution: alpha-engine-config#11353" in report_module.render_full_update(
        inputs, now=NOW
    )
    assert "Resolution: alpha-engine-config#11353" in report_module.render_message(
        inputs, now=NOW, update_url="https://example/u"
    )


def test_s3_store_client_overlay_branch_is_used() -> None:
    class Client:
        calls: list[dict[str, str]] = []

        def get_object(self, **kwargs: str) -> dict[str, BytesIO]:
            self.calls.append(kwargs)
            payload = json.dumps(
                {
                    "entries": [
                        {"match": "unresolved_log:*", "ref": "nousergon-data#999"}
                    ]
                }
            ).encode()
            return {"Body": BytesIO(payload)}

    class Store:
        bucket = report_module.RESOLUTIONS_S3_BUCKET
        client = Client()

    resolutions = report_module._load_resolutions(Store())

    assert resolutions[0] == ("unresolved_log:*", "nousergon-data#999")
    assert Store.client.calls == [
        {
            "Bucket": report_module.RESOLUTIONS_S3_BUCKET,
            "Key": report_module.RESOLUTIONS_S3_KEY,
        }
    ]


def test_many_findings_share_resolution_lines_and_stay_within_budget() -> None:
    logs = [
        report_module.UnresolvedLog("D17", f"k{i}", "local:x", "why")
        for i in range(15)
    ]
    inputs = dataclasses.replace(
        _inputs_with_logs(logs),
        history_absent={f"gate-{i}": "no dated reading" for i in range(3)},
        resolutions=(
            ("unresolved_log:*", "alpha-engine-config#11353"),
            ("history_absent:*", "alpha-engine-config#11353"),
        ),
    )

    message = report_module.render_message(
        inputs, now=NOW, update_url="https://example/u"
    )

    assert message.count("Resolution: alpha-engine-config#11353") == 1
    assert "15 failed run(s)" in message
    assert len(message) <= report_module.wire_budget(
        severity=report_module.DELIVERY_SEVERITY,
        source=report_module.DELIVERY_SOURCE,
        max_chars=report_module.MESSAGE_MAX_CHARS,
    )


# ── one message per market close (Brian 2026-10-21) ──────────────────────────


def _delivered_saturday(store: FakeStore) -> None:
    store.written["runs/report.daily/2026-09-18/2026-09-19/run.json"] = json.dumps(
        {"status": "ok", "delivered": True}
    ).encode("utf-8")


@pytest.mark.parametrize("source", ["relay", "schedule"])
def test_a_close_already_delivered_is_not_sent_again(monkeypatch, source) -> None:
    """Sunday and Monday mornings carry Friday's close, which Saturday already
    sent. The run files an `ok` manifest saying why and sends nothing."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("DATA_REPORT_SOURCE", source)
    tracker = _RecordingTracker()
    monkeypatch.setattr(report_module, "_tracker", lambda: tracker)
    sent = []
    monkeypatch.setattr(report_module, "deliver", lambda *a, **k: sent.append(a))
    store = _live_shaped_store()
    _delivered_saturday(store)

    manifest = report_module.run_report(
        store, trading_day=dt.date(2026, 9, 18), calendar_date=dt.date(2026, 9, 21), now=NOW
    )

    assert sent == [] and tracker.comments == []
    assert manifest["status"] == "ok" and manifest["delivered"] is False
    assert "2026-09-19" in manifest["not_delivered_because"]
    assert "runs/report.daily/2026-09-18/2026-09-21/run.json" in store.written


def test_a_manual_run_always_delivers_even_for_a_reported_close(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("DATA_REPORT_SOURCE", "manual")
    monkeypatch.setattr(report_module, "_tracker", _RecordingTracker)
    sent = []
    monkeypatch.setattr(report_module, "deliver", lambda *a, **k: sent.append(a))
    store = _live_shaped_store()
    _delivered_saturday(store)

    manifest = report_module.run_report(
        store, trading_day=dt.date(2026, 9, 18), calendar_date=dt.date(2026, 9, 21), now=NOW
    )

    assert len(sent) == 1 and manifest["delivered"] is True


def test_a_failed_earlier_delivery_does_not_count(monkeypatch) -> None:
    """Saturday's run failed, so Sunday's sends the close."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("DATA_REPORT_SOURCE", "schedule")
    monkeypatch.setattr(report_module, "_tracker", _RecordingTracker)
    sent = []
    monkeypatch.setattr(report_module, "deliver", lambda *a, **k: sent.append(a))
    store = _live_shaped_store()
    store.written["runs/report.daily/2026-09-18/2026-09-19/run.json"] = json.dumps(
        {"status": "failed", "reason": "the transport refused it"}
    ).encode("utf-8")

    manifest = report_module.run_report(
        store, trading_day=dt.date(2026, 9, 18), calendar_date=dt.date(2026, 9, 20), now=NOW
    )

    assert len(sent) == 1 and manifest["delivered"] is True


def test_the_workflow_states_who_started_the_run() -> None:
    text = (pathlib.Path(__file__).resolve().parents[1] / ".github/workflows/data-report.yml").read_text()
    assert "DATA_REPORT_SOURCE: ${{ inputs.source || github.event_name }}" in text

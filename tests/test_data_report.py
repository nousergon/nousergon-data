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

import datetime as dt
import json
import pathlib
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


def test_the_workflow_declares_exactly_the_cron_the_code_declares():
    crons = [entry["cron"] for entry in _schedule(_workflow())]
    assert crons == [report_module.DELIVERY_CRON_UTC], (
        "the workflow's schedule and `data_gate.report.DELIVERY_CRON_UTC` are two "
        "copies of one fact; a second cron here would also deliver twice a day "
        "against one manifest key."
    )


def test_the_declared_cron_resolves_to_0330_pdt_and_0230_pst():
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


def test_the_cron_is_set_early_enough_for_the_measured_delivery_lag():
    """`alpha-engine-config-I9966`: the declared cron is an INPUT to delivery
    time. Every scheduled workflow on this account fires 3-5h late, measured
    and chronic, so a naive 06:30-PT literal (`30 13`) would deliver near
    09:30 PT. This asserts the subtraction was actually made."""
    hour = int(report_module.DELIVERY_CRON_UTC.split()[1])
    naive_intended_utc_hour = 13  # 06:30 PDT
    assert naive_intended_utc_hour - hour == 3


# ── the public repo carries no infrastructure identifier ─────────────────────


def test_the_workflow_carries_no_account_id_or_bucket_literal():
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


def test_the_account_id_is_masked_before_any_step_can_print_it():
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


def test_the_reporter_does_not_run_as_the_gate_reader():
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
def test_sat_sun_and_mon_all_resolve_to_the_same_friday(calendar_date, expected):
    resolved = report_module.previous_trading_day(dt.date.fromisoformat(calendar_date))
    assert resolved.isoformat() == expected


def test_the_calendar_date_is_the_discriminator_in_the_output_key():
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


def test_an_empty_key_segment_is_refused_rather_than_collapsed():
    with pytest.raises(ValueError):
        report_module.manifest_key("run.json", "2026-09-18", "")


# ── principle 7: what nothing renders as ─────────────────────────────────────


def test_an_empty_clause_list_renders_no_clauses_never_zero_over_zero():
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

    def list_keys(self, prefix: str = ""):
        seen = set(self.documents) | set(self.written) | self.denied
        return [key for key in sorted(seen) if key.startswith(prefix)]

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.written[key] = payload


def _inputs(store: FakeStore, *, now: dt.datetime = NOW):
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


def test_a_rung_with_no_dated_history_renders_absent_and_never_zero():
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


def test_a_denied_dated_reading_is_never_rendered_as_an_absence():
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


def test_one_dated_reading_is_cannot_say_and_not_nothing_moved():
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


def test_an_unmeasurable_clause_never_reduces_to_unmet():
    """UNMEASURABLE ranks worse than UNMET: "we could not read this" must not
    render as "we read it and it said no"."""
    rows = report_module._normalize_gate_rows(
        {"clauses": [{"name": "c", "met": False, "unmeasurable": True}]}
    )
    assert rows == {"rows": [{"id": "c", "state": "UNMEASURABLE"}]}


def test_a_stale_ladder_puts_the_staleness_line_first_in_both_documents():
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


def test_a_fresh_ladder_carries_no_staleness_line():
    inputs = _inputs(_live_shaped_store())
    assert inputs.staleness_line is None
    assert "STALE" not in report_module.render_full_update(inputs, now=NOW)


def test_the_report_quotes_the_board_totals_it_did_not_compute():
    update = report_module.render_full_update(_inputs(_live_shaped_store()), now=NOW)
    assert "202/649" in update
    assert "92" in update


def test_an_unreadable_ladder_raises_rather_than_reporting_an_empty_board():
    """A report that could not read the ladder has nothing to say. Rendering a
    message about an absent ladder is a surface reporting its own outage as the
    system's state."""
    with pytest.raises(Exception):
        _inputs(FakeStore({"gates/board/latest.json": BOARD}))


# ── the exit-code contract ───────────────────────────────────────────────────


def test_the_report_exit_code_never_carries_the_gate_verdict(monkeypatch):
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


def test_a_report_that_could_not_be_delivered_exits_two(monkeypatch):
    from data_gate import __main__ as cli

    def _boom(*a, **k):
        raise RuntimeError("telegram refused the publish")

    monkeypatch.setattr(report_module, "run_report", _boom)
    monkeypatch.setattr(cli, "open_store", lambda *a, **k: FakeStore({}))
    assert cli.main(["report", "--store", "/tmp/store"]) == 2


def test_the_report_subcommand_has_no_fail_on_unmet_flag():
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

    def __init__(self):
        self.comments = []

    def find_or_create_issue(self, *, title, body):
        return 7

    def post_comment(self, issue, body):
        self.comments.append((issue, body))
        return "https://github.com/nousergon/alpha-engine-config/issues/7#issuecomment-1"

    def update_issue_body(self, issue, body):
        return None


def test_a_run_files_every_declared_key_and_names_its_trigger_as_a_key(monkeypatch):
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


def test_a_failed_delivery_still_files_a_manifest_that_says_so(monkeypatch):
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


def test_the_tracker_comment_is_posted_before_the_headline_is_rendered(monkeypatch):
    """The headline's indispensable content is that comment's permalink, so a
    tracker that refuses the comment must stop the message going out entirely
    rather than produce a headline pointing at nothing."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    delivered = []
    monkeypatch.setattr(report_module, "deliver", lambda *a, **k: delivered.append(a))

    class _RefusingTracker:
        def find_or_create_issue(self, *, title, body):
            return 7

        def post_comment(self, issue, body):
            raise RuntimeError("the tracker refused the comment")

        def update_issue_body(self, issue, body):  # pragma: no cover - never reached
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

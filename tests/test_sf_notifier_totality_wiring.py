"""Pins notifier/failure-path TOTALITY across all 3 orchestration SFs
(config#1819).

Two live incidents motivated this:
  1. 2026-07-02 (`offcycle-shell-20260703-005835`): a manual-trigger input
     omitted `pipeline_label`, and the weekly SF's `HandleFailure` itself
     threw `States.Runtime` on
     ``States.Format('Saturday{} Pipeline failed...', $.pipeline_label, ...)``
     — masking the TRUE underlying pipeline error behind an opaque
     meta-crash. config#1629 (closed 7/3) fixed the analogous `$.error` gap
     from `LibPinDriftGate` but not this field/path.
  2. 2026-06-12 (`friday-shell-2026-06-12-eod-...`): `NotifyComplete` (a
     SUCCESS-path notifier) failed the whole otherwise-succeeded run with
     `SNS.InvalidParameterException: Invalid parameter: Subject` (SNS
     Subject must be <=100 chars, non-empty, no newlines).

Fix shape (mirrors the existing `InitializeInput` JsonMerge-defaults idiom):
  - `NormalizeFailureContext` is inserted as the SOLE Catch/Next target for
    every failure path in the weekly SF, floor-defaulting
    `$.error`/`$.sns_topic_arn`/`$.pipeline_label` via `States.JsonMerge`
    before anything reaches `HandleFailure`.
  - `NormalizeFailureContextRepin` (a Choice) then RE-DERIVES
    `$.pipeline_label` from `$.shell_run` (an SF-controlled boolean) instead
    of trusting a manual Execution.Input's free-text value for it — this
    closes the Subject-injection vector structurally (ASL has no
    substring/truncate intrinsic, so bounding a free-text field's length at
    the format site isn't expressible; removing the free-text trust is).
  - `NotifyComplete` / `NotifyShellRunComplete` (the weekly SF's
    SUCCESS-path notifiers) each gained a `Catch` so a publish failure can
    never fail an otherwise-succeeded pipeline, mirroring the
    `PublishResearchFailureImmediate`-style best-effort-notify idiom
    already used inside `ResearchPredictorParallel`.

This test mirrors the style of `test_sf_invocationdoesnotexist_retry.py`:
structural JSON assertions across all 3 SF definitions, no AWS calls.
"""
from __future__ import annotations

import json
import pathlib
import re

import pytest

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_WEEKLY = _REPO_ROOT / "infrastructure" / "step_function.json"
_DAILY = _REPO_ROOT / "infrastructure" / "step_function_daily.json"
_EOD = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

SF_JSONS = [_WEEKLY, _DAILY, _EOD]


def _load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text())


def _iter_states(definition: dict):
    """Yield (state_name, state_def) for every state, recursing into
    Parallel branches (mirrors test_sf_invocationdoesnotexist_retry.py)."""
    def _walk(states: dict):
        for name, state in states.items():
            yield name, state
            if state.get("Type") == "Parallel":
                for branch in state.get("Branches", []):
                    yield from _walk(branch.get("States", {}))
    yield from _walk(definition.get("States", {}))


def _format_refs(template: str) -> list[str]:
    """Extract every $.foo JSONPath reference passed as an argument to a
    States.Format(...) call in a Parameters template string (including
    references nested inside another intrinsic call used AS an argument,
    e.g. States.JsonToString($.error) inside States.Format(...)).

    Deliberately simple (a balanced-paren scan + a flat regex over the
    extracted argument text, not a full intrinsic-function parser) — but
    MUST walk balanced parens rather than stopping at the first literal
    ')', since the message text itself may contain a literal parenthesis
    (e.g. HandleFailure's Message.$ has '(Use the Step Functions console
    to redrive from the failed step.)' inside the format string, which a
    naive `[^)]*` regex would treat as the call's closing paren and truncate
    the match before ever reaching the $.field arguments)."""
    refs: list[str] = []
    start_token = "States.Format("
    idx = 0
    while True:
        start = template.find(start_token, idx)
        if start == -1:
            break
        depth = 1
        pos = start + len(start_token)
        while pos < len(template) and depth > 0:
            if template[pos] == "(":
                depth += 1
            elif template[pos] == ")":
                depth -= 1
            pos += 1
        args = template[start + len(start_token) : pos - 1]
        refs.extend(re.findall(r"\$\.[A-Za-z_][A-Za-z0-9_.\[\]]*", args))
        idx = pos
    return refs


@pytest.fixture(scope="module")
def weekly() -> dict:
    return _load(_WEEKLY)


@pytest.fixture(scope="module")
def weekly_states(weekly) -> dict:
    return weekly["States"]


class TestNormalizeFailureContextIsSoleChokepoint:
    """Every failure path in the weekly SF must funnel through
    NormalizeFailureContext before HandleFailure — no direct entry."""

    def test_normalize_states_exist(self, weekly_states):
        for name in (
            "NormalizeFailureContext",
            "NormalizeFailureContextRepin",
            "NormalizeFailureContextPreflightLabel",
            "NormalizeFailureContextRealLabel",
        ):
            assert name in weekly_states, f"{name} missing from weekly SF"

    def test_only_label_pinning_states_transition_to_handle_failure(
        self, weekly_states
    ):
        """HandleFailure must be reached ONLY via the two terminal
        label-pinning Pass states of the normalize chain — never directly
        from a Task Catch, an Extract*Error normalizer, or a bare Choice
        transition. A direct entry would bypass the $.error/$.pipeline_label/
        $.sns_topic_arn floor-defaults and could reintroduce the 2026-07-02
        States.Runtime-masking crash for any NEW failure path added later."""
        allowed = {
            "NormalizeFailureContextPreflightLabel",
            "NormalizeFailureContextRealLabel",
        }
        offenders = []
        for name, st in _iter_states(weekly_states):
            targets = []
            if st.get("Next") == "HandleFailure":
                targets.append("Next")
            for c in st.get("Catch", []):
                if c.get("Next") == "HandleFailure":
                    targets.append("Catch")
            if st.get("Type") == "Choice":
                if st.get("Default") == "HandleFailure":
                    targets.append("Default")
                for c in st.get("Choices", []):
                    if c.get("Next") == "HandleFailure":
                        targets.append("Choice")
            if targets and name not in allowed:
                offenders.append(f"{name} ({', '.join(targets)})")
        assert not offenders, (
            "State(s) reach HandleFailure without going through "
            "NormalizeFailureContext first: " + "; ".join(offenders)
        )

    def test_every_former_direct_catch_now_targets_normalize(self, weekly_states):
        """Every Catch/Extract*Error normalizer that used to target
        HandleFailure directly (pre-config#1819) must now target
        NormalizeFailureContext. Spot-checks the known failure-path
        entrypoints across the weekly SF (top-level Task Catches, the
        Parallel-level backstop Catch, and every Extract*Error Pass)."""
        expected_normalize_entrypoints = [
            "MorningEnrich",
            "WaitForMorningEnrich",
            "DataPhase1",
            "WaitForDataPhase1",
            "ResearchPredictorParallel",
            "Backtester",
            "WaitForBacktester",
            "PredictorBacktest",
            "WaitForPredictorBacktest",
            "PortfolioOptimizerBacktest",
            "WaitForPortfolioOptimizerBacktest",
            "Parity",
            "WaitForParity",
            "Evaluator",
            "WaitForEvaluator",
        ]
        for name in expected_normalize_entrypoints:
            catches = weekly_states[name].get("Catch", [])
            assert catches, f"{name} has no Catch block"
            assert any(
                c.get("Next") == "NormalizeFailureContext"
                and c.get("ResultPath") == "$.error"
                for c in catches
            ), f"{name}'s Catch does not route to NormalizeFailureContext"

        expected_extract_normalizers = [
            "ExtractLibPinDriftError",
            "ExtractParallelBranchError",
            "ExtractMorningEnrichError",
            "ExtractDataPhase1Error",
            "ExtractBacktesterError",
            "ExtractParityError",
            "ExtractPredictorBacktestError",
            "ExtractPortfolioOptimizerBacktestError",
            "ExtractEvaluatorError",
        ]
        for name in expected_extract_normalizers:
            st = weekly_states[name]
            assert st["Type"] == "Pass"
            assert st["ResultPath"] == "$.error"
            assert st["Next"] == "NormalizeFailureContext", (
                f"{name}.Next must be NormalizeFailureContext, "
                f"got {st['Next']!r}"
            )


class TestNormalizeFailureContextDefaultsCoverFormatRefs:
    """The whole point: every $.field States.Format on the failure path
    (HandleFailure) must be structurally guaranteed present by the time it
    is reached — either floor-defaulted by NormalizeFailureContext's
    JsonMerge, or unconditionally set by NormalizeFailureContextRepin."""

    def test_handle_failure_format_refs_all_covered(self, weekly_states):
        handle_failure = weekly_states["HandleFailure"]
        params = handle_failure["Parameters"]
        refs: set[str] = set()
        for key in ("Subject.$", "Message.$"):
            refs |= set(_format_refs(params[key]))
        # Also the bare $.sns_topic_arn JSONPath reference (TopicArn.$ is a
        # direct JSONPath, not wrapped in a States.Format call, so
        # _format_refs never sees it). $.error IS already picked up by
        # _format_refs via the nested States.JsonToString($.error) call
        # inside Message.$'s States.Format(...) argument list.
        refs.add(params["TopicArn.$"])
        assert refs, "expected at least one $.field reference in HandleFailure"

        # Fields floor-defaulted by NormalizeFailureContext's JsonMerge.
        normalize_merge = weekly_states["NormalizeFailureContext"]["Parameters"]["merged.$"]
        floor_defaults_blob = re.search(r"StringToJson\('(\{.*?\})'\)", normalize_merge)
        assert floor_defaults_blob, "could not extract NormalizeFailureContext defaults blob"
        floor_defaults = json.loads(floor_defaults_blob.group(1))

        # $.pipeline_label is further unconditionally OVERWRITTEN (not just
        # defaulted) by the repin chain, so it is covered regardless of the
        # floor-default's presence.
        repin_targets_pipeline_label = (
            weekly_states["NormalizeFailureContextPreflightLabel"]["ResultPath"]
            == "$.pipeline_label"
            and weekly_states["NormalizeFailureContextRealLabel"]["ResultPath"]
            == "$.pipeline_label"
        )
        assert repin_targets_pipeline_label

        covered = {f"$.{k}" for k in floor_defaults} | {"$.pipeline_label"}

        uncovered = [r for r in refs if r not in covered and r != "$."]
        assert not uncovered, (
            f"HandleFailure references field(s) not covered by "
            f"NormalizeFailureContext's defaults or the repin chain: {uncovered}"
        )

    def test_pipeline_label_is_repinned_not_trusted_verbatim(self, weekly_states):
        """pipeline_label must be RE-DERIVED from $.shell_run, not merely
        defaulted-if-absent — a present-but-malicious/oversized value from a
        manual Execution.Input must be overwritten, not merged around."""
        repin = weekly_states["NormalizeFailureContextRepin"]
        assert repin["Type"] == "Choice"
        (rule,) = repin["Choices"]
        conds = {c["Variable"] for c in rule["And"]}
        assert conds == {"$.shell_run"}
        assert rule["Next"] == "NormalizeFailureContextPreflightLabel"
        assert repin["Default"] == "NormalizeFailureContextRealLabel"

        preflight = weekly_states["NormalizeFailureContextPreflightLabel"]
        assert preflight["Type"] == "Pass"
        assert preflight["Result"] == " Preflight"
        assert preflight["ResultPath"] == "$.pipeline_label"
        assert preflight["Next"] == "HandleFailure"

        real = weekly_states["NormalizeFailureContextRealLabel"]
        assert real["Type"] == "Pass"
        assert real["Result"] == ""
        assert real["ResultPath"] == "$.pipeline_label"
        assert real["Next"] == "HandleFailure"

    def test_error_and_sns_topic_arn_floor_defaulted(self, weekly_states):
        normalize = weekly_states["NormalizeFailureContext"]
        assert normalize["Type"] == "Pass"
        merge_expr = normalize["Parameters"]["merged.$"]
        # Defaults-first, $ second (second-arg-wins JsonMerge) — mirrors
        # InitializeInput's idiom, so a real upstream $.error/$.sns_topic_arn
        # always survives over the floor default.
        assert merge_expr.startswith("States.JsonMerge(States.StringToJson(")
        assert merge_expr.rstrip(")").endswith("$,false")
        blob_match = re.search(r"StringToJson\('(\{.*?\})'\)", merge_expr)
        assert blob_match
        blob = json.loads(blob_match.group(1))
        assert "error" in blob
        assert "sns_topic_arn" in blob
        assert blob["sns_topic_arn"] == (
            "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts"
        )
        assert normalize["OutputPath"] == "$.merged"
        assert normalize["Next"] == "NormalizeFailureContextRepin"


class TestSnsSubjectBoundedness:
    """SNS Subject must be non-empty, <=100 chars, no newlines — at the
    format site. Every Subject in the weekly SF is EITHER a hardcoded
    constant (byte-bounded by construction) or a States.Format whose only
    variable component ($.pipeline_label) is now SF-derived (never
    user-supplied free text) via NormalizeFailureContextRepin."""

    def _all_sns_publish_states(self, weekly_states):
        for name, st in _iter_states(weekly_states):
            if st.get("Resource") == "arn:aws:states:::sns:publish":
                yield name, st

    def test_every_hardcoded_subject_is_bounded(self, weekly_states):
        for name, st in self._all_sns_publish_states(weekly_states):
            subject = st["Parameters"].get("Subject")
            if subject is None:
                continue
            assert isinstance(subject, str)
            assert 0 < len(subject) <= 100, (
                f"{name}: hardcoded Subject {subject!r} violates SNS's "
                f"<=100-char/non-empty contract"
            )
            assert "\n" not in subject, f"{name}: hardcoded Subject contains a newline"

    def test_every_formatted_subject_only_varies_on_pipeline_label(
        self, weekly_states
    ):
        """Every Subject.$ (States.Format) in the weekly SF must vary ONLY
        on $.pipeline_label — never $.error or any other unbounded field —
        so the SF-derived, length-bounded pipeline_label value (' Preflight'
        or '') is the sole source of variability in the Subject string."""
        for name, st in self._all_sns_publish_states(weekly_states):
            subject_fmt = st["Parameters"].get("Subject.$")
            if subject_fmt is None:
                continue
            refs = _format_refs(subject_fmt)
            assert refs, f"{name}: Subject.$ has no $.field reference to check"
            assert set(refs) <= {"$.pipeline_label"}, (
                f"{name}: Subject.$ references {refs} — only $.pipeline_label "
                f"is a structurally-bounded field (SF-derived, <=10 chars); "
                f"any other field risks the SNS Subject <=100-char/no-newline "
                f"contract if it ever carries unbounded/newline-bearing data"
            )

    def test_no_subject_ever_formats_on_error(self, weekly_states):
        """$.error is an arbitrary-size JSON blob (poll details, Lambda
        payloads, drift offender lists) — it must NEVER be interpolated
        into a Subject (only Message), or SNS's <=100-char Subject cap
        would be trivially violated by real error payloads."""
        for name, st in self._all_sns_publish_states(weekly_states):
            subject_fmt = st["Parameters"].get("Subject.$", "")
            assert "$.error" not in subject_fmt, (
                f"{name}: Subject.$ must not reference $.error"
            )


class TestSuccessPathNotifiersAreNonFatal:
    """A notification failure on the SUCCESS path must NOT fail an
    otherwise-succeeded pipeline (2026-06-12 NotifyComplete incident)."""

    @pytest.mark.parametrize(
        "name,degraded_next,terminal_next",
        [
            # config#2857: the real-completion pair now converges into the
            # SF-envelope completion marker instead of Ending directly. The
            # Friday-PM preflight (shell_run) pair is deliberately EXCLUDED
            # from the marker (a dry pass must not satisfy the SLA) and
            # still Ends here exactly as before.
            ("NotifyComplete", "NotifyCompleteDegraded", "WriteCompletionMarker"),
            ("NotifyShellRunComplete", "NotifyShellRunCompleteDegraded", None),
        ],
    )
    def test_notifier_catches_and_still_ends_success(
        self, weekly_states, name, degraded_next, terminal_next
    ):
        st = weekly_states[name]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::sns:publish"
        if terminal_next:
            assert "End" not in st
            assert st["Next"] == terminal_next
        else:
            assert st.get("End") is True
        catches = st.get("Catch", [])
        assert catches, f"{name} must Catch a publish failure (config#1819)"
        assert any(
            c["ErrorEquals"] == ["States.ALL"] and c["Next"] == degraded_next
            for c in catches
        )
        degraded = weekly_states[degraded_next]
        assert degraded["Type"] == "Pass"
        if terminal_next:
            assert "End" not in degraded
            assert degraded["Next"] == terminal_next, (
                f"{degraded_next} must still reach the SUCCEEDED completion "
                f"marker — a caught notify failure must not propagate into a Fail"
            )
        else:
            assert degraded["End"] is True, (
                f"{degraded_next} must still End the execution as SUCCEEDED — "
                f"a caught notify failure must not propagate into a Fail"
            )

    def test_notify_complete_subject_still_bounded(self, weekly_states):
        """NotifyComplete/NotifyShellRunComplete's Subjects are hardcoded
        constants — the 2026-06-12 SNS.InvalidParameterException class is
        closed at the format site regardless of the Catch added here."""
        for name in ("NotifyComplete", "NotifyShellRunComplete"):
            subject = weekly_states[name]["Parameters"]["Subject"]
            assert 0 < len(subject) <= 100
            assert "\n" not in subject


class TestDailyAndEodAudited:
    """config#1819 asks to AUDIT step_function_daily.json / _eod.json for
    the same shape, not force-fit the weekly fix if their structure
    genuinely differs. Both were audited and found ALREADY TOTAL:
      - daily: HandleFailure's Message uses States.JsonToString($) (the
        WHOLE state, always present — cannot itself raise a missing-field
        error) and a hardcoded Subject; sns_topic_arn is defaulted by its
        own InitializeInput. No SUCCESS-path notifier exists (PipelineComplete
        is a bare Succeed with no sns:publish).
      - eod: HandleFailure's TopicArn is HARDCODED (not $.sns_topic_arn) and
        every Catch/Extract*Error path into it sets $.error; HandleFailure's
        own Catch already routes to ForceStopInstance so even an SNS-side
        failure can't block the cost-guard instance stop.
    These tests pin that audited-safe shape so a future edit can't silently
    reintroduce the weekly SF's pre-fix gap in either file."""

    def test_daily_handle_failure_message_uses_whole_state_not_a_field(self):
        daily = _load(_DAILY)["States"]
        hf = daily["HandleFailure"]
        assert hf["Resource"] == "arn:aws:states:::sns:publish"
        # Hardcoded Subject — cannot violate the SNS Subject contract.
        subject = hf["Parameters"]["Subject"]
        assert 0 < len(subject) <= 100
        assert "\n" not in subject
        # Message.$ formats on $ (the whole state), not a possibly-absent
        # named field — structurally cannot raise a missing-JSONPath error.
        message_fmt = hf["Parameters"]["Message.$"]
        assert "States.JsonToString($)" in message_fmt

    def test_daily_sns_topic_arn_defaulted_by_initialize_input(self):
        daily = _load(_DAILY)["States"]
        merge_expr = daily["InitializeInput"]["Parameters"]["merged.$"]
        assert "sns_topic_arn" in merge_expr

    def test_daily_has_no_success_path_notifier(self):
        """PipelineComplete is a bare Succeed — no sns:publish on the
        success path, so there is no NotifyComplete-shaped crash surface
        in the daily SF at all."""
        daily = _load(_DAILY)["States"]
        assert daily["PipelineComplete"]["Type"] == "Succeed"
        assert "Resource" not in daily["PipelineComplete"]

    def test_eod_handle_failure_topic_arn_hardcoded(self):
        eod = _load(_EOD)["States"]
        hf = eod["HandleFailure"]
        assert hf["Resource"] == "arn:aws:states:::sns:publish"
        assert hf["Parameters"]["TopicArn"] == (
            "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts"
        ), "EOD HandleFailure's TopicArn must stay hardcoded (cost-guard: a malformed sns_topic_arn must never block instance stop)"
        subject = hf["Parameters"]["Subject"]
        assert 0 < len(subject) <= 100
        assert "\n" not in subject

    def test_eod_handle_failure_own_catch_proceeds_to_force_stop(self):
        eod = _load(_EOD)["States"]
        hf = eod["HandleFailure"]
        catches = hf.get("Catch", [])
        assert any(
            c["ErrorEquals"] == ["States.ALL"] and c["Next"] == "ForceStopInstance"
            for c in catches
        ), "EOD HandleFailure must Catch its own SNS failure and still stop the instance"
        assert hf["Next"] == "ForceStopInstance"

    def test_eod_every_handle_failure_entry_sets_error(self):
        eod_def = _load(_EOD)
        eod_states = eod_def["States"]
        offenders = []
        for name, st in _iter_states(eod_states):
            for c in st.get("Catch", []):
                if c.get("Next") == "HandleFailure" and c.get("ResultPath") != "$.error":
                    offenders.append(name)
            if st.get("Next") == "HandleFailure" and not (
                st.get("Type") == "Pass" and st.get("ResultPath") == "$.error"
            ):
                offenders.append(name)
        assert not offenders, (
            f"EOD state(s) reach HandleFailure without $.error set: {offenders}"
        )


class TestAllThreeSfsHaveNoUnboundedSubjectFormat:
    """Cross-SF sweep: no Subject.$ (formatted Subject) anywhere across all
    3 orchestration SFs may reference $.error or any field other than the
    SF-derived $.pipeline_label — the general form of the 2026-06-12 class."""

    @pytest.mark.parametrize("sf_path", SF_JSONS)
    def test_no_subject_format_references_error(self, sf_path):
        definition = _load(sf_path)
        offenders = []
        for name, st in _iter_states(definition["States"]):
            if st.get("Resource") != "arn:aws:states:::sns:publish":
                continue
            subject_fmt = st.get("Parameters", {}).get("Subject.$", "")
            if "$.error" in subject_fmt:
                offenders.append(name)
        assert not offenders, (
            f"{sf_path.name}: Subject.$ references $.error in {offenders} — "
            f"an arbitrary-size error blob must never be interpolated into "
            f"an SNS Subject (100-char cap)"
        )

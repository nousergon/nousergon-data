"""Pins the L274 SF MutualExclusionGuard wiring across all 3 Alpha Engine SFs.

Origin: 2026-05-26 duplicate-EventBridge-target incident. PR #322
closed the SPECIFIC duplicate-target shape via the CFN-target-uniqueness
CI gate (``test_deploy_step_function_eventbridge_input.py``); L274 closes
the BROADER class (operator double-paste, EventBridge internal retry-on-
throttle, cross-region replay coincidence, any future shape the CI
chokepoint misses) by gating each SF at its entry point on a DynamoDB
conditional-PUT mutex.

Design:
- Allowlist cadence roles (``daily`` / ``weekly`` / ``eod`` / ``shell-run``)
  acquire the mutex via ``DynamoDB.PutItem`` with
  ``ConditionExpression: attribute_not_exists(mutex_key)``.
- Operator-initiated runs (any other ``pipeline_role`` value, including
  absent) bypass entirely — they are deliberately concurrent with
  cadence runs.
- ``mutex_key`` = ``{state-machine-name}#{pipeline_role}#{YYYY-MM-DDTHH:MM}``
  (UTC minute bucket parsed from ``$$.Execution.StartTime``). Two
  duplicate cron-fired executions in the same minute produce identical
  keys; the second loses with ``ConditionalCheckFailedException`` →
  ``MutexConflict`` Fail.
- No TTL / no release: the date+minute in the key is itself the
  staleness window — a successfully-acquired key is naturally single-use
  because no future execution can land in the same past minute-bucket.
- Fail-open on non-Conditional-Check errors (DDB outage / IAM drift):
  the mutex is best-effort architectural insurance; per
  ``[[feedback_no_silent_fails]]`` "secondary observability" carve-out
  the primary deliverable survives a mutex-side failure.

This test pins:
- The 3 new states exist in each SF (``CheckMutexRole``, ``AcquireMutex``,
  ``MutexConflict``) with correct Type + Resource.
- Wiring: ``InitializeInput.Next`` (or ``StartAt`` for EOD) → ``CheckMutexRole``;
  ``CheckMutexRole.Default`` → former-first-state;
  ``AcquireMutex.Next`` → former-first-state.
- ``CheckMutexRole`` gates on ``$.pipeline_role`` in the cadence allowlist.
- ``AcquireMutex`` catches ``DynamoDB.ConditionalCheckFailedException`` →
  ``MutexConflict``; catches ``States.ALL`` → fail-open to former-first-state.
- ``mutex_key`` Format string references ``$$.StateMachine.Name``,
  ``$.pipeline_role``, and ``$$.Execution.StartTime`` so the minute-bucket
  parsing is anchored to SF runtime context (not a hardcoded value).
- CFN: ``ExecutionMutexTable`` exists with PK ``mutex_key`` (S),
  PAY_PER_REQUEST, no TTL attribute (intentional — minute-bucket key
  encodes its own staleness).
- IAM: ``alpha-engine-step-functions-role`` has ``dynamodb:PutItem`` on
  the mutex table resource (release/cleanup excluded because the design
  intentionally has no release path).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
IAM_DIR = INFRA / "iam"

SATURDAY_SF = INFRA / "step_function.json"
WEEKDAY_SF = INFRA / "step_function_daily.json"
EOD_SF = INFRA / "step_function_eod.json"
CFN_PATH = INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"
ROLE_PATH = IAM_DIR / "alpha-engine-step-functions-role.json"

MUTEX_TABLE_NAME = "alpha-engine-sf-execution-mutex"
MUTEX_TABLE_ARN = (
    "arn:aws:dynamodb:us-east-1:711398986525:table/alpha-engine-sf-execution-mutex"
)
CADENCE_ROLES = {"daily", "weekly", "eod", "shell-run"}

# Per SF, the state that the mutex chain (CheckMutexRole.Default and
# AcquireMutex.Next) routes into — i.e., the state that USED TO BE first
# before this PR inserted the mutex chain in front of it.
FORMER_FIRST_STATE_BY_SF = {
    "saturday": ("step_function.json", "CheckShellRun"),
    "weekday": ("step_function_daily.json", "DeployDriftCheck"),
    # 2026-06-30: the EOD SF's first post-mutex state is now the re-runnability
    # guard StartTradingInstance (ec2:startInstances → SSM-readiness poll), which
    # then flows into the CheckSkipPostMarketData rerun-gate chain. Inserted
    # because an operator recovery rerun landed on a stopped instance (the prior
    # run's ForceStopInstance) and the first ssm:sendCommand died with
    # Ssm.InvalidInstanceIdException. The mutex still routes to the pipeline's
    # first post-mutex work state — that state is just the ensure-running gate now.
    "eod": ("step_function_eod.json", "StartTradingInstance"),
}


def _load_sf(name: str) -> dict:
    path = INFRA / FORMER_FIRST_STATE_BY_SF[name][0]
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def saturday_sf() -> dict:
    return _load_sf("saturday")


@pytest.fixture(scope="module")
def weekday_sf() -> dict:
    return _load_sf("weekday")


@pytest.fixture(scope="module")
def eod_sf() -> dict:
    return _load_sf("eod")


@pytest.fixture(scope="module")
def all_sfs(saturday_sf, weekday_sf, eod_sf):
    return {
        "saturday": saturday_sf,
        "weekday": weekday_sf,
        "eod": eod_sf,
    }


# ---------------------------------------------------------------------------
# Mutex-chain presence + correct state types
# ---------------------------------------------------------------------------

class TestMutexStatesPresent:
    """Each SF must carry all three mutex states with correct Type."""

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_check_mutex_role_is_choice(self, sf_name, all_sfs):
        states = all_sfs[sf_name]["States"]
        assert "CheckMutexRole" in states, (
            f"{sf_name} SF missing CheckMutexRole — L274 mutex chain not wired"
        )
        assert states["CheckMutexRole"]["Type"] == "Choice"

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_acquire_mutex_is_dynamodb_task(self, sf_name, all_sfs):
        states = all_sfs[sf_name]["States"]
        assert "AcquireMutex" in states, (
            f"{sf_name} SF missing AcquireMutex — L274 mutex chain not wired"
        )
        st = states["AcquireMutex"]
        assert st["Type"] == "Task"
        assert st["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem", (
            f"{sf_name} AcquireMutex must use the AWS-SDK direct integration "
            f"for DynamoDB putItem (got {st['Resource']!r}). If a future PR "
            f"swaps to a Lambda-based mutex, this assertion needs to evolve "
            f"alongside the IAM grant in alpha-engine-step-functions-role.json."
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_mutex_conflict_is_fail(self, sf_name, all_sfs):
        states = all_sfs[sf_name]["States"]
        assert "MutexConflict" in states, (
            f"{sf_name} SF missing MutexConflict — Conditional-Check losers "
            f"would silently fall through"
        )
        st = states["MutexConflict"]
        assert st["Type"] == "Fail"
        assert st["Error"] == "MutexConflict"


# ---------------------------------------------------------------------------
# Wiring — entry-point reroute, Choice default, Acquire next-state
# ---------------------------------------------------------------------------

class TestMutexWiring:
    """The mutex chain must sit BETWEEN the SF's entry point and the
    former-first-state, on both the gated path (cadence role acquires)
    and the bypass path (operator/missing role)."""

    def test_saturday_initialize_input_routes_to_check_mutex_role(self, saturday_sf):
        # config#830: a cadence-preset gate (CheckRunMode) now sits between
        # InitializeInput and the lib-pin drift gate; CheckRunMode.Default →
        # CheckSkipLibPinDriftCheck so the chain is unchanged for non-preset input.
        # 2026-06-08 (L4517): the preventive lib-pin drift gate is the first
        # workload gate; its skip-default + gate-default converge on
        # CheckMutexRole — the mutex still sits between entry and the
        # former-first-state, one gate further down.
        assert saturday_sf["States"]["InitializeInput"]["Next"] == "CheckRunMode"
        assert saturday_sf["States"]["CheckRunMode"]["Default"] == "CheckSkipLibPinDriftCheck"
        assert saturday_sf["States"]["LibPinDriftGate"]["Default"] == "CheckMutexRole"

    def test_weekday_initialize_input_routes_to_check_mutex_role(self, weekday_sf):
        assert weekday_sf["States"]["InitializeInput"]["Next"] == "CheckMutexRole"

    def test_eod_start_at_routes_to_check_mutex_role(self, eod_sf):
        assert eod_sf["StartAt"] == "CheckMutexRole", (
            "EOD SF has no InitializeInput state, so the mutex chain "
            "must be reachable via StartAt directly."
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_check_mutex_role_default_routes_to_former_first_state(
        self, sf_name, all_sfs
    ):
        former = FORMER_FIRST_STATE_BY_SF[sf_name][1]
        states = all_sfs[sf_name]["States"]
        assert states["CheckMutexRole"]["Default"] == former, (
            f"{sf_name} SF: operator/missing-role bypass must land on "
            f"the SF's former first state ({former}), not skip past it"
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_acquire_mutex_next_routes_to_former_first_state(
        self, sf_name, all_sfs
    ):
        former = FORMER_FIRST_STATE_BY_SF[sf_name][1]
        states = all_sfs[sf_name]["States"]
        assert states["AcquireMutex"]["Next"] == former, (
            f"{sf_name} SF: cadence-role acquire path must continue to the "
            f"former first state ({former}) after grabbing the mutex"
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_former_first_state_still_exists(self, sf_name, all_sfs):
        """Guards against the mutex chain pointing at a renamed/deleted target."""
        former = FORMER_FIRST_STATE_BY_SF[sf_name][1]
        assert former in all_sfs[sf_name]["States"], (
            f"{sf_name} SF: former-first-state {former!r} not in States — "
            f"either renamed or deleted. Update FORMER_FIRST_STATE_BY_SF "
            f"in this test AND the mutex chain wiring in the SF JSON."
        )


# ---------------------------------------------------------------------------
# CheckMutexRole — pipeline_role allowlist
# ---------------------------------------------------------------------------

class TestCheckMutexRoleAllowlist:
    """CheckMutexRole's Choices block must allowlist exactly the four
    cadence roles. Adding a fifth role (e.g., a new cron cadence) MUST
    be a deliberate edit here AND in the cadence-role-set across other
    tests; promoting an operator role to a cadence role without
    updating both surfaces silently un-protects the SF."""

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_allowlist_matches_cadence_set(self, sf_name, all_sfs):
        choices = all_sfs[sf_name]["States"]["CheckMutexRole"]["Choices"]
        assert len(choices) == 1, (
            f"{sf_name} CheckMutexRole: expected exactly 1 Choices entry "
            f"(an Or over cadence-role string-equals); got {len(choices)}"
        )
        rule = choices[0]
        assert "And" in rule and any(
            "IsPresent" in cond.get("Variable", "") + str(cond)
            for cond in rule["And"]
        ), (
            f"{sf_name} CheckMutexRole: the gating Choice must require "
            f"$.pipeline_role IsPresent before string-comparing it (missing "
            f"IsPresent makes the StringEquals branches error on absent input)"
        )
        # Extract string-equals values from the Or sub-block
        or_block = next(
            (cond["Or"] for cond in rule["And"] if "Or" in cond), None
        )
        assert or_block is not None, (
            f"{sf_name} CheckMutexRole: gating Choice missing Or-over-roles"
        )
        seen = {c["StringEquals"] for c in or_block if "StringEquals" in c}
        assert seen == CADENCE_ROLES, (
            f"{sf_name} CheckMutexRole allowlist drift: expected "
            f"{sorted(CADENCE_ROLES)}, got {sorted(seen)}. If a new cadence "
            f"is being added (or an operator role promoted), update "
            f"CADENCE_ROLES in this test AND every SF's CheckMutexRole."
        )


# ---------------------------------------------------------------------------
# AcquireMutex — ConditionExpression + Catch blocks + key format
# ---------------------------------------------------------------------------

class TestAcquireMutexSemantics:

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_acquire_targets_the_named_mutex_table(self, sf_name, all_sfs):
        params = all_sfs[sf_name]["States"]["AcquireMutex"]["Parameters"]
        assert params["TableName"] == MUTEX_TABLE_NAME, (
            f"{sf_name} AcquireMutex: TableName must match the CFN "
            f"ExecutionMutexTable resource name ({MUTEX_TABLE_NAME})"
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_acquire_uses_conditional_put(self, sf_name, all_sfs):
        params = all_sfs[sf_name]["States"]["AcquireMutex"]["Parameters"]
        assert (
            params.get("ConditionExpression") == "attribute_not_exists(mutex_key)"
        ), (
            f"{sf_name} AcquireMutex must use "
            f"attribute_not_exists(mutex_key) — without the ConditionExpression "
            f"the PutItem unconditionally clobbers, defeating the whole guard"
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_mutex_key_format_includes_runtime_anchors(self, sf_name, all_sfs):
        params = all_sfs[sf_name]["States"]["AcquireMutex"]["Parameters"]
        key_format = params["Item"]["mutex_key"]["S.$"]
        # The key MUST reference all 3 anchors so duplicate-target detection
        # bins on (state-machine, pipeline-role, minute-bucket).
        for anchor in (
            "$$.StateMachine.Name",
            "$.pipeline_role",
            "$$.Execution.StartTime",
        ):
            assert anchor in key_format, (
                f"{sf_name} AcquireMutex mutex_key Format missing anchor "
                f"{anchor!r} — without it the bucketing collapses and "
                f"either over-protects (false MutexConflict) or under-protects "
                f"(misses dup-triggers)"
            )
        # Minute-bucket parsing splits StartTime on ':' and joins [0] + [1].
        # Verifying both ArrayGetItem(..., 0) AND ArrayGetItem(..., 1) appear
        # catches an accidental degrade to hour-bucket (only [0]).
        assert "ArrayGetItem(States.StringSplit($$.Execution.StartTime, ':'), 0)" in key_format, (
            f"{sf_name} AcquireMutex: minute-bucket parsing must extract "
            f"[0] segment of StartTime split on ':' (year-through-hour)"
        )
        assert "ArrayGetItem(States.StringSplit($$.Execution.StartTime, ':'), 1)" in key_format, (
            f"{sf_name} AcquireMutex: minute-bucket parsing must extract "
            f"[1] segment of StartTime split on ':' (minute). Missing this "
            f"degrades to hour-bucketing, which would falsely conflict every "
            f"intra-hour operator rerun of a cadence role."
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_acquire_catches_conditional_check_to_mutex_conflict(
        self, sf_name, all_sfs
    ):
        catches = all_sfs[sf_name]["States"]["AcquireMutex"]["Catch"]
        match = [
            c
            for c in catches
            if "DynamoDB.ConditionalCheckFailedException" in c.get("ErrorEquals", [])
        ]
        assert len(match) == 1, (
            f"{sf_name} AcquireMutex must Catch "
            f"DynamoDB.ConditionalCheckFailedException explicitly — without "
            f"it, a States.ALL Catch would swallow the duplicate-trigger "
            f"case and fail-open silently (the worst possible behavior)"
        )
        assert match[0]["Next"] == "MutexConflict", (
            f"{sf_name} AcquireMutex: Conditional-Check Catch must route to "
            f"MutexConflict (Fail), not bypass to the workload"
        )

    @pytest.mark.parametrize("sf_name", list(FORMER_FIRST_STATE_BY_SF))
    def test_acquire_states_all_catch_fails_open_to_former_first_state(
        self, sf_name, all_sfs
    ):
        former = FORMER_FIRST_STATE_BY_SF[sf_name][1]
        catches = all_sfs[sf_name]["States"]["AcquireMutex"]["Catch"]
        match = [c for c in catches if "States.ALL" in c.get("ErrorEquals", [])]
        assert len(match) == 1, (
            f"{sf_name} AcquireMutex must Catch States.ALL (DDB outage / IAM "
            f"drift / transient SDK error) — without it, mutex-side failures "
            f"would hard-fail the SF, defeating the fail-open posture"
        )
        assert match[0]["Next"] == former, (
            f"{sf_name} AcquireMutex: States.ALL fail-open must route to "
            f"the former first state ({former}), so cadence runs survive "
            f"a DDB outage. Got {match[0]['Next']!r}."
        )


# ---------------------------------------------------------------------------
# CFN — ExecutionMutexTable
# ---------------------------------------------------------------------------

class TestCfnExecutionMutexTable:
    """The CFN template must declare the mutex DynamoDB table with the
    schema the SF JSONs target. Raw-text parsing per the existing
    test_deploy_step_function_eventbridge_input.py precedent — CFN's
    !Ref / !Sub / !GetAtt tags require a custom YAML loader."""

    @pytest.fixture(scope="class")
    def cfn_text(self) -> str:
        return CFN_PATH.read_text()

    def test_mutex_table_resource_declared(self, cfn_text):
        assert "ExecutionMutexTable:" in cfn_text, (
            "CFN template missing ExecutionMutexTable resource — without it "
            "the SF AcquireMutex states would error with "
            "ResourceNotFoundException at runtime"
        )

    def test_mutex_table_type_is_dynamodb(self, cfn_text):
        # Slice the ExecutionMutexTable block
        anchor = cfn_text.index("ExecutionMutexTable:")
        block = cfn_text[anchor : anchor + 1200]
        assert "Type: AWS::DynamoDB::Table" in block

    def test_mutex_table_name_matches_sf_target(self, cfn_text):
        anchor = cfn_text.index("ExecutionMutexTable:")
        block = cfn_text[anchor : anchor + 1200]
        assert f"TableName: {MUTEX_TABLE_NAME}" in block, (
            f"CFN ExecutionMutexTable TableName must match the SF "
            f"AcquireMutex TableName ({MUTEX_TABLE_NAME}); drift here = "
            f"runtime ResourceNotFoundException"
        )

    def test_mutex_table_pay_per_request(self, cfn_text):
        anchor = cfn_text.index("ExecutionMutexTable:")
        block = cfn_text[anchor : anchor + 1200]
        assert "BillingMode: PAY_PER_REQUEST" in block, (
            "PAY_PER_REQUEST is the right mode here — mutex traffic is bursty "
            "(cron-driven, ~12 writes/week), provisioned capacity would "
            "either over-provision or throttle real traffic"
        )

    def test_mutex_table_pk_is_mutex_key(self, cfn_text):
        anchor = cfn_text.index("ExecutionMutexTable:")
        block = cfn_text[anchor : anchor + 1200]
        assert "AttributeName: mutex_key" in block
        assert "AttributeType: S" in block
        assert "KeyType: HASH" in block, (
            "mutex_key must be the HASH key — the conditional-PUT semantics "
            "this whole design relies on key off the partition-key existence"
        )

    def test_mutex_table_intentionally_has_no_ttl(self, cfn_text):
        """The design INTENTIONALLY omits TTL — the minute-bucket in the
        key encodes its own staleness window. If a future PR adds TTL,
        it MUST also update this test + the design docstring at the top
        + ROADMAP L274 — silent TTL addition would change the semantic
        contract (locks no longer permanent past their natural staleness)."""
        anchor = cfn_text.index("ExecutionMutexTable:")
        block = cfn_text[anchor : anchor + 1200]
        assert "TimeToLiveSpecification" not in block, (
            "ExecutionMutexTable must NOT declare TimeToLiveSpecification — "
            "the design intentionally relies on minute-bucket key encoding "
            "for natural single-use semantics. If you want TTL, also update "
            "this test + AcquireMutex Item.ttl_epoch + the L274 design"
        )


# ---------------------------------------------------------------------------
# IAM — SF role has dynamodb:PutItem on the mutex table only
# ---------------------------------------------------------------------------

class TestIamGrant:
    """alpha-engine-step-functions-role must carry exactly dynamodb:PutItem
    on the mutex table — no DeleteItem (design has no release path),
    no broader DDB perms (least-privilege)."""

    @pytest.fixture(scope="class")
    def role_policy(self) -> dict:
        return json.loads(ROLE_PATH.read_text())

    def test_role_has_dynamodb_putitem_statement(self, role_policy):
        ddb_stmts = [
            s
            for s in role_policy["Statement"]
            if "dynamodb" in str(s.get("Action", "")).lower()
        ]
        assert len(ddb_stmts) >= 1, (
            "alpha-engine-step-functions-role missing dynamodb:PutItem — "
            "the SF AcquireMutex states would fail AccessDenied at runtime"
        )
        # Must include putItem specifically
        actions = []
        for s in ddb_stmts:
            a = s.get("Action", [])
            actions.extend([a] if isinstance(a, str) else a)
        assert "dynamodb:PutItem" in actions, (
            f"Expected dynamodb:PutItem in SF role; got actions {actions}"
        )

    def test_role_does_not_grant_delete_item(self, role_policy):
        """L274 design has no release path. dynamodb:DeleteItem would be
        dead permission — least-privilege violation. If a future PR adds
        release/cleanup, also update this test + the design docstring."""
        for stmt in role_policy["Statement"]:
            a = stmt.get("Action", [])
            actions = [a] if isinstance(a, str) else a
            assert "dynamodb:DeleteItem" not in actions, (
                "alpha-engine-step-functions-role should NOT grant "
                "dynamodb:DeleteItem — L274 design intentionally has no "
                "release path (minute-bucket key encodes natural staleness). "
                "If you're adding release, update the test + the design"
            )

    def test_role_ddb_grant_scoped_to_mutex_table(self, role_policy):
        ddb_stmts = [
            s
            for s in role_policy["Statement"]
            if "dynamodb" in str(s.get("Action", "")).lower()
        ]
        for stmt in ddb_stmts:
            res = stmt.get("Resource", [])
            res_list = [res] if isinstance(res, str) else res
            for r in res_list:
                assert r == MUTEX_TABLE_ARN or r.endswith(MUTEX_TABLE_NAME), (
                    f"DDB grant must be scoped to the mutex table only "
                    f"(expected {MUTEX_TABLE_ARN}); got {r}. Broader scope "
                    f"violates least-privilege."
                )


class TestCfnMutexConflictAlarms:
    """CFN must wire a CloudWatch metric-filter + alarm per CFN-managed SF on the
    L274 MutexConflict Fail (config#729), AND the SFs must have execution logging
    enabled so the filters have a live log group to read.

    This is the post-#516 / post-ne-rename restore. #516 shipped filters on the
    OLD alpha-engine-* log groups while the ne-* rename (config#1381) left all
    SFs with logging OFF — so the filters had nothing to read. These assertions
    pin the invariants that #516's text-only test missed: (a) the SF names the
    filters target are the LIVE ne-* names, (b) those SFs actually have
    LoggingConfiguration at level=ERROR, (c) the destination log groups are
    declared CFN resources, (d) no DefaultValue (mutually exclusive with
    Dimensions). Raw-text parsing per the TestCfnExecutionMutexTable precedent.
    """

    # The two CFN-managed SFs (EOD is script-managed + deferred — see CFN note).
    SF_NAMES = (
        "ne-weekly-freshness-pipeline",
        "ne-preopen-trading-pipeline",
    )

    @pytest.fixture(scope="class")
    def cfn_text(self) -> str:
        return CFN_PATH.read_text()

    @pytest.mark.parametrize("sf_name", SF_NAMES)
    def test_metric_filter_present_for_each_sf(self, cfn_text, sf_name):
        assert f"/aws/stepfunctions/{sf_name}" in cfn_text, (
            f"missing AWS::Logs::MetricFilter LogGroupName for {sf_name} — "
            f"without it MutexConflict Fails on that SF produce no CW metric "
            f"and no alarm can fire (config#729)"
        )

    @pytest.mark.parametrize("sf_name", SF_NAMES)
    def test_sf_has_execution_logging_enabled(self, cfn_text, sf_name):
        # The metric filter is dead unless the SF logs its FailedEvents. Pin that
        # a LogGroup resource exists for each SF AND LoggingConfiguration is set
        # at level=ERROR. This is the invariant that would have caught the
        # ne-rename logging regression #516 tripped over.
        assert f"LogGroupName: /aws/stepfunctions/{sf_name}" in cfn_text, (
            f"missing AWS::Logs::LogGroup for {sf_name} — the SF's "
            f"LoggingConfiguration destination must be a declared CFN resource"
        )
        assert cfn_text.count("Level: ERROR") >= len(self.SF_NAMES), (
            "each CFN-managed SF must have LoggingConfiguration Level=ERROR so "
            "the MutexConflict Cause reaches the log group (config#729)"
        )

    def test_metric_filter_emits_mutexconflict_metric(self, cfn_text):
        assert "AWS::Logs::MetricFilter" in cfn_text
        assert "MutexConflictFails" in cfn_text, (
            "the MutexConflict metric transformation must emit a dedicated "
            "MutexConflictFails metric (config#729)"
        )

    def test_metric_filters_have_no_dimensions(self, cfn_text):
        # AWS::Logs::MetricFilter rejects a metric transformation with Dimensions
        # unless the dimension VALUE is a token extracted from the filter pattern
        # ("the specified filter pattern does not support dimensions" — the
        # #537-deploy break). Our pattern is a plain text match, so per-SF
        # attribution is via DISTINCT MetricName, NOT Dimensions. Pin that the
        # filter region carries no Dimensions and no DefaultValue.
        mf_start = cfn_text.index("MutexConflictMetricFilter")
        # Scan from the first metric filter to the first alarm block.
        mf_region = cfn_text[mf_start : cfn_text.index("MutexConflictAlarm")]
        assert "Dimensions" not in mf_region, (
            "MutexConflict metric filters must NOT set Dimensions — a literal "
            "dimension value is rejected at CFN create; use a distinct "
            "MetricName per SF instead"
        )
        assert "DefaultValue" not in mf_region, (
            "MutexConflict metric filters must NOT set DefaultValue"
        )

    def test_each_sf_has_distinct_metric_name(self, cfn_text):
        # Per-SF attribution requires a unique metric name per filter (no
        # dimensions), each referenced by its own alarm.
        for metric in ("WeeklyFreshnessMutexConflictFails",
                       "PreopenTradingMutexConflictFails"):
            assert cfn_text.count(metric) >= 2, (
                f"{metric} must appear in both its metric filter and its alarm "
                f"(distinct-metric-name attribution, config#729)"
            )

    @pytest.mark.parametrize("sf_name", SF_NAMES)
    def test_alarm_present_for_each_sf(self, cfn_text, sf_name):
        # ne-<cadence>-...-mutex-conflict (strip the -pipeline suffix)
        alarm_name = sf_name.replace("-pipeline", "") + "-mutex-conflict"
        assert alarm_name in cfn_text, (
            f"missing CloudWatch alarm {alarm_name} for {sf_name} — the "
            f"MutexConflict metric exists but nobody is paged on it (config#729)"
        )

    def test_alarms_route_to_alerts_topic(self, cfn_text):
        anchor = cfn_text.index("WeeklyFreshnessMutexConflictAlarm:")
        block = cfn_text[anchor : anchor + 900]
        assert "!Ref AlertsTopic" in block, (
            "the weekly-freshness mutex-conflict alarm must page AlertsTopic"
        )


class TestCfnEodMutexConflictAlarm:
    """EOD (ne-postclose-trading-pipeline) MutexConflict coverage (config#1416).

    EOD is script-managed (update_eod_pipeline_sf.sh + deploy-infrastructure.sh's
    update_or_create()), not a CFN AWS::StepFunctions::StateMachine resource. Per
    the design note in alpha-engine-orchestration.yaml, this means EOD's log
    group is deliberately NOT a CFN resource (unlike the weekly/preopen pair in
    TestCfnMutexConflictAlarms) — the scripts create it idempotently, out of
    band, before setting LoggingConfiguration on the state machine. Only the
    metric-filter + alarm (which read the log group by literal name) are CFN
    resources. These tests pin that shape so a future "cleanup" doesn't
    "fix" the asymmetry by adding a CFN LogGroup for EOD, which would break
    deploy ordering (see the CFN comment + PR description for the full
    reasoning).
    """

    EOD_LOG_GROUP_NAME = "/aws/stepfunctions/ne-postclose-trading-pipeline"

    @pytest.fixture(scope="class")
    def cfn_text(self) -> str:
        return CFN_PATH.read_text()

    def test_metric_filter_present(self, cfn_text):
        assert "PostcloseTradingMutexConflictMetricFilter:" in cfn_text, (
            "missing AWS::Logs::MetricFilter resource for the EOD pipeline "
            "(config#1416)"
        )
        assert "PostcloseTradingMutexConflictFails" in cfn_text, (
            "EOD metric filter must emit a dedicated "
            "PostcloseTradingMutexConflictFails metric"
        )

    def test_alarm_present(self, cfn_text):
        assert "PostcloseTradingMutexConflictAlarm:" in cfn_text, (
            "missing CloudWatch alarm resource for the EOD pipeline "
            "(config#1416)"
        )
        assert "ne-postclose-trading-mutex-conflict" in cfn_text, (
            "missing the ne-postclose-trading-mutex-conflict AlarmName"
        )

    def test_metric_filter_log_group_name_matches_script_managed_group(
        self, cfn_text
    ):
        anchor = cfn_text.index("PostcloseTradingMutexConflictMetricFilter:")
        block = cfn_text[anchor : anchor + 500]
        assert f"LogGroupName: {self.EOD_LOG_GROUP_NAME}" in block, (
            "EOD metric filter LogGroupName must match the literal log group "
            "name the scripts create (update_eod_pipeline_sf.sh / "
            "deploy-infrastructure.sh) — /aws/stepfunctions/"
            "ne-postclose-trading-pipeline"
        )

    def test_metric_filter_has_no_dimensions_or_default_value(self, cfn_text):
        mf_start = cfn_text.index("PostcloseTradingMutexConflictMetricFilter")
        mf_region = cfn_text[
            mf_start : cfn_text.index("PostcloseTradingMutexConflictAlarm")
        ]
        assert "Dimensions" not in mf_region, (
            "EOD MutexConflict metric filter must NOT set Dimensions — same "
            "invariant as the weekly/preopen pair (config#729)"
        )
        assert "DefaultValue" not in mf_region, (
            "EOD MutexConflict metric filter must NOT set DefaultValue"
        )

    def test_alarm_routes_to_alerts_topic(self, cfn_text):
        anchor = cfn_text.index("PostcloseTradingMutexConflictAlarm:")
        block = cfn_text[anchor : anchor + 900]
        assert "!Ref AlertsTopic" in block, (
            "the postclose-trading mutex-conflict alarm must page AlertsTopic"
        )

    def test_eod_log_group_is_not_a_cfn_resource(self, cfn_text):
        # Regression guard: unlike WeeklyFreshnessLogGroup / PreopenTradingLogGroup,
        # EOD's log group must NOT become a CFN AWS::Logs::LogGroup resource.
        # Reasoning (config#1416): deploy-infrastructure.sh runs the
        # script-managed state-machine update (step 3, update_or_create()) BEFORE
        # the CFN stack deploy (step 4). If EOD's log group were CFN-owned, the
        # first deploy after this change would try to set LoggingConfiguration
        # in step 3 pointing at a log group CFN hasn't created yet (step 4 hasn't
        # run) -> ResourceNotFoundException. The scripts create the log group
        # idempotently out of band instead; CFN owns only the filter + alarm.
        assert "PostcloseTradingLogGroup" not in cfn_text, (
            "EOD's log group must NOT be a CFN resource (no "
            "PostcloseTradingLogGroup) — it is created idempotently by "
            "update_eod_pipeline_sf.sh / deploy-infrastructure.sh instead. "
            "See config#1416: a CFN-owned log group here would break deploy "
            "ordering (script step runs before the CFN stack deploy step)."
        )


class TestEodLoggingScriptWiring:
    """Text-contains checks over the two shell scripts that manage the
    script-managed EOD state machine (config#1416). This repo has no
    bash-execution test harness, so — same style as the CFN raw-text tests
    above — these pin the presence of the AWS CLI flags/calls that enable EOD
    execution logging, without actually invoking AWS.
    """

    @pytest.fixture(scope="class")
    def update_eod_script_text(self) -> str:
        return (INFRA / "update_eod_pipeline_sf.sh").read_text()

    @pytest.fixture(scope="class")
    def deploy_infra_script_text(self) -> str:
        return (INFRA / "deploy-infrastructure.sh").read_text()

    def test_update_eod_script_enables_logging(self, update_eod_script_text):
        assert "--logging-configuration" in update_eod_script_text, (
            "update_eod_pipeline_sf.sh must pass --logging-configuration to "
            "update-state-machine so the manual-fallback path also enables "
            "EOD execution logging (config#1416)"
        )
        assert "create-log-group" in update_eod_script_text, (
            "update_eod_pipeline_sf.sh must idempotently ensure the EOD log "
            "group exists (create-log-group) before enabling logging"
        )
        assert "put-retention-policy" in update_eod_script_text, (
            "update_eod_pipeline_sf.sh must set retention on the EOD log group"
        )

    def test_deploy_infra_script_enables_logging_for_eod_only(
        self, deploy_infra_script_text
    ):
        assert "create-log-group" in deploy_infra_script_text, (
            "deploy-infrastructure.sh must idempotently ensure the EOD log "
            "group exists before the update_or_create() call for EOD "
            "(config#1416)"
        )

        eod_call_line = next(
            line
            for line in deploy_infra_script_text.splitlines()
            if "update_or_create" in line and "$EOD_ARN" in line
        )
        groom_call_line = next(
            line
            for line in deploy_infra_script_text.splitlines()
            if "update_or_create" in line and "$GROOM_ARN" in line
        )

        assert "EOD_LOGGING_CONFIG" in eod_call_line, (
            "the update_or_create() call site for $EOD_ARN must pass the "
            "logging-configuration variable as its 5th argument (config#1416)"
        )
        assert "EOD_LOGGING_CONFIG" not in groom_call_line, (
            "the update_or_create() call site for $GROOM_ARN must NOT pass "
            "any logging-configuration argument — groom's behavior must be "
            "unchanged by config#1416"
        )

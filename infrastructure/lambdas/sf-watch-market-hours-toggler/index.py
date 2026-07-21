"""alpha-engine-sf-watch-market-hours-toggler — structural market-hours
enforcement for `alpha-engine-sf-watch-executor-role`'s trading-pipeline
`StartExecution` grant (config#2932).

Why this exists: `sf-watch-executor-role-policy.json`'s
`RerunFleetSFFromFailedStep` statement grants unconditional
`states:StartExecution` on `ne-preopen-trading-pipeline` and
`ne-postclose-trading-pipeline` (config#2903 found this — the "never
during market hours" recovery-role charter rule was prompt-text-only, no
IAM-level boundary). Brian's 2026-07-20 ruling (Option E, see
alpha-engine-config#2932) authorized closing that gap by scheduling the
SAME codified writer path (`alpha-engine-config/infrastructure/iam/
apply.sh`) rather than adding an independent second writer with its own
policy-content logic — preserving the fleet's "exactly one writer per
codified role" invariant in spirit (see
`crucible-executor/infrastructure/iam/check-no-foreign-writers.py`'s
docstring for the four prior IAM-clobber incidents that invariant exists
to prevent).

`apply.sh` itself cannot run inside this Lambda (standard Lambda runtimes
ship no AWS CLI binary), so this handler reimplements its ONE operation —
`iam:PutRolePolicy` with a checked-in JSON document, verbatim — in boto3.
It never decides policy CONTENT itself: both variant documents
(`sf-watch-executor-role-policy.json` — permissive/off-hours,
`sf-watch-executor-role-policy-market-hours.json` — restrictive/market-
hours) are copied into this Lambda's deployment package by `deploy.sh`
from the alpha-engine-config checkout, unmodified. The only decision this
code makes is WHICH already-codified variant should be live right now.

Idempotent: reads the role's current inline policy first and only calls
`PutRolePolicy` when it differs from the desired variant, so back-to-back
5-minute-interval invocations during a stable period are near-silent
(single GetRolePolicy call, no write, no CloudTrail mutation event).

Runs on a `rate(5 minutes)` EventBridge schedule rather than a fixed
twice-daily UTC cron specifically to avoid the DST-drift problem that
sank the original `aws:CurrentTime` IAM Condition option (see the
config#2932 thread): evaluating live market-hours state on every tick
means the boundary is always DST/holiday-correct at the moment it's
checked, with no seasonal cron-edit maintenance job of its own.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, time

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

ROLE_NAME = os.environ.get("SF_WATCH_ROLE_NAME", "alpha-engine-sf-watch-executor-role")
POLICY_NAME = os.environ.get("SF_WATCH_POLICY_NAME", "sf-watch-executor-least-priv")

# US/Eastern via a fixed ET/EDT offset table would drift silently — use the
# stdlib zoneinfo tz database (Lambda python3.12 runtime ships tzdata),
# same source of truth class as crucible-executor's pytz-based
# executor/market_hours.py::is_market_hours(), just without a pytz
# dependency in this Lambda's package.
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")
_MARKET_OPEN = time(9, 30)
_MARKET_CLOSE = time(16, 0)

# NYSE observed holidays through 2030 — duplicated from
# crucible-executor/executor/market_hours.py::NYSE_HOLIDAYS (config#2932:
# no shared-lib home for this table exists yet across repos; keep both
# copies in sync by hand until a follow-up moves it to nousergon-lib).
# Source: https://www.nyse.com/markets/hours-calendars
NYSE_HOLIDAYS: set[date] = {
    # 2025
    date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
    date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
    date(2025, 11, 27), date(2025, 12, 25),
    # 2026
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
    date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
    # 2027
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
    date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
    date(2027, 11, 25), date(2027, 12, 24),
    # 2028
    date(2028, 1, 17), date(2028, 2, 21), date(2028, 4, 14), date(2028, 5, 29),
    date(2028, 6, 19), date(2028, 7, 4), date(2028, 9, 4), date(2028, 11, 23),
    date(2028, 12, 25),
    # 2029
    date(2029, 1, 1), date(2029, 1, 15), date(2029, 2, 19), date(2029, 3, 30),
    date(2029, 5, 28), date(2029, 6, 19), date(2029, 7, 4), date(2029, 9, 3),
    date(2029, 11, 22), date(2029, 12, 25),
    # 2030
    date(2030, 1, 1), date(2030, 1, 21), date(2030, 2, 18), date(2030, 4, 19),
    date(2030, 5, 27), date(2030, 6, 19), date(2030, 7, 4), date(2030, 9, 2),
    date(2030, 11, 28), date(2030, 12, 25),
}

# Loaded once per cold start from the deployment package (deploy.sh copies
# these verbatim from alpha-engine-config/infrastructure/iam/ — this
# module never hand-writes policy content).
_HANDLER_DIR = os.path.dirname(os.path.abspath(__file__))
PERMISSIVE_POLICY_FILE = os.path.join(_HANDLER_DIR, "sf-watch-executor-role-policy.json")
MARKET_HOURS_POLICY_FILE = os.path.join(_HANDLER_DIR, "sf-watch-executor-role-policy-market-hours.json")


def _now() -> datetime:
    """Wall-clock now() in ET — its own function so tests can monkeypatch
    it directly instead of stubbing the stdlib `datetime` class."""
    return datetime.now(_ET)


def is_market_hours(now: datetime | None = None) -> bool:
    """Return True if `now` (default: current time) falls within NYSE
    regular trading hours: weekday, not a holiday, 9:30-16:00 ET."""
    if now is None:
        now = datetime.now(_ET)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=_ET)
    else:
        now = now.astimezone(_ET)

    if now.weekday() > 4:
        return False
    if now.date() in NYSE_HOLIDAYS:
        return False

    current_time = now.time()
    return _MARKET_OPEN <= current_time < _MARKET_CLOSE


def _load_json(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _current_policy_document(iam_client, role_name: str, policy_name: str) -> dict | None:
    """Return the role's current inline policy document, or None if the
    role/policy doesn't exist yet (first-ever run before any apply.sh
    bootstrap, or the role was deleted out-of-band)."""
    try:
        resp = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
    except iam_client.exceptions.NoSuchEntityException:
        return None
    return resp["PolicyDocument"]


def handler(event, context):
    now = _now()
    market_open = is_market_hours(now)

    desired_file = MARKET_HOURS_POLICY_FILE if market_open else PERMISSIVE_POLICY_FILE
    desired_variant = "market-hours" if market_open else "permissive"
    desired_document = _load_json(desired_file)

    iam = boto3.client("iam")
    current_document = _current_policy_document(iam, ROLE_NAME, POLICY_NAME)

    if current_document == desired_document:
        logger.info(
            "No-op: %s already applied (market_open=%s, now=%s ET)",
            desired_variant, market_open, now.isoformat(),
        )
        return {"changed": False, "variant": desired_variant, "market_open": market_open}

    logger.info(
        "Applying %s variant to role=%s policy=%s (market_open=%s, now=%s ET)",
        desired_variant, ROLE_NAME, POLICY_NAME, market_open, now.isoformat(),
    )
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName=POLICY_NAME,
        PolicyDocument=json.dumps(desired_document),
    )
    return {"changed": True, "variant": desired_variant, "market_open": market_open}

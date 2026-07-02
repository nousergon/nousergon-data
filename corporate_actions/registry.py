"""corporate_actions.registry — S3-backed corporate-action registry.

The durable, auditable store for the unified corporate-actions program
(config#1431). Two write-once S3 JSON namespaces under ``prefix`` (default
``corporate_actions/``):

  - ``corporate_actions/actions/{action_id}.json`` — the IMMUTABLE detected
    record (the ``CorporateAction`` fields + ``detected_at`` UTC ISO +
    ``detected_run_id``). Written write-if-absent so a re-detection across
    reruns never clobbers the original detection provenance.
  - ``corporate_actions/applied/{store}/{action_id}.json`` — a write-once
    "we applied action X to store Y (e.g. the ArcticDB universe)" marker.
    Modeled now for the later restatement-path PR; NOT exercised by this PR.

The KEY consumer-facing method this PR adds is :meth:`explains_discrepancy`,
which lets ``collectors/daily_closes`` ask the registry — authoritatively,
not via a text heuristic — "is this >5% adjusted-close jump explained by a
detected corporate action?" so a confirmed split restatement logs at WARN
(``corporate_action_restatement``) instead of tripping the flow-doctor ERROR
band.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# Same 0.005 tolerance used by ``collectors/daily_closes._SPLIT_RATIO_TOL``:
# adjusted closes restate by the EXACT split factor, so 0.5% absorbs feed
# rounding noise while still rejecting a genuine anomaly. Redefined here (rather
# than imported) to avoid an import-time cycle with ``collectors.daily_closes``
# (which imports this package lazily). Keep the two in lockstep.
_SPLIT_RATIO_TOL = 0.005

# ``explains_discrepancy`` ex-date plausibility window. A split with execution
# date E restates the adjusted close of every date STRICTLY BEFORE E, so a
# discrepancy observed on ``date`` is explained by an action whose ex_date is
# on/after ``date`` — with a small early-leniency for edge timing (the
# re-fetched date can coincide with / sit a day or two from the ex date) and a
# generous upper bound (the split scan only looks a couple weeks back + a 7-day
# forward buffer, so an ancient split must not "explain" a fresh jump).
_EX_DATE_EARLY_LENIENCY_DAYS = 3
_EX_DATE_MAX_AHEAD_DAYS = 60


class CorporateActionRegistry:
    """S3 JSON registry of detected + applied corporate actions.

    Constructed with an explicit boto3 S3 client + bucket (the caller owns
    client construction, mirroring the rest of the repo — e.g.
    ``builders/daily_append.py``), so it is trivially testable against a fake
    S3.
    """

    def __init__(self, s3, bucket: str, prefix: str = "corporate_actions/"):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix if prefix.endswith("/") else prefix + "/"
        self._actions_prefix = f"{self.prefix}actions/"
        self._applied_prefix = f"{self.prefix}applied/"
        # Lazy S3-loaded full index {action_id: CorporateAction} for the
        # list_actions / get_action query API (populated once from S3 by
        # _ensure_loaded).
        self._cache: dict | None = None
        # Actions DETECTED this session (recorded via record_detected). This is
        # the authoritative input for explains_discrepancy: a split that could
        # restate one of THIS run's dates was necessarily returned by the same
        # window split scan that recorded it here, so explaining the run's
        # adjusted-close jumps needs only the session set — NEVER a full S3 list
        # (which would also be O(all-time-actions) on every comparison).
        self._session_actions: dict = {}

    # ── key helpers ──────────────────────────────────────────────────────
    def _action_key(self, action_id: str) -> str:
        return f"{self._actions_prefix}{action_id}.json"

    def _applied_key(self, store: str, action_id: str) -> str:
        return f"{self._applied_prefix}{store}/{action_id}.json"

    # ── low-level S3 ─────────────────────────────────────────────────────
    def _object_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise  # auth / throttle / network — never silently treat as absent

    def _get_json(self, key: str) -> dict | None:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey"):
                return None
            raise
        return json.loads(obj["Body"].read())

    def _put_json(self, key: str, payload: dict) -> None:
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )

    def _list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []) or []:
                k = obj.get("Key")
                if k and k.endswith(".json"):
                    keys.append(k)
            if not resp.get("IsTruncated"):
                break
            # Continue ONLY on a real (non-empty string) continuation token. S3
            # always supplies one when IsTruncated is True; refusing anything
            # else (None / empty / a non-str) is a hard guard against an
            # unbounded pagination loop on a malformed or degenerate paginator
            # response — terminate rather than spin.
            token = resp.get("NextContinuationToken")
            if not isinstance(token, str) or not token:
                break
        return keys

    # ── detected records ─────────────────────────────────────────────────
    def record_detected(self, action, *, run_id: str) -> bool:
        """Persist an immutable detected record write-if-absent.

        Returns ``True`` if a NEW record was written, ``False`` if a record for
        this ``action_id`` already existed (idempotent across reruns). Uses a
        head_object guard then put; a concurrent racing writer is tolerated (the
        record is content-addressed, so a double-write is harmless — only the
        ``detected_at`` timestamp would differ).
        """
        from corporate_actions import CorporateAction  # local: avoid import cycle

        if not isinstance(action, CorporateAction):
            raise TypeError(f"record_detected expects CorporateAction, got {type(action)!r}")
        key = self._action_key(action.action_id)
        # The action is "known to this session" regardless of whether THIS call
        # is the one that persists it — seed the session index so
        # explains_discrepancy can match the window's just-detected splits
        # WITHOUT a full S3 re-list.
        self._session_actions[action.action_id] = action
        if self._cache is not None:
            self._cache[action.action_id] = action
        if self._object_exists(key):
            return False
        payload = {
            **action.to_dict(),
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "detected_run_id": run_id,
        }
        self._put_json(key, payload)
        return True

    def get_action(self, action_id: str):
        """Return the detected ``CorporateAction`` for ``action_id`` or None."""
        from corporate_actions import CorporateAction

        if action_id in self._session_actions:
            return self._session_actions[action_id]
        if self._cache is not None and action_id in self._cache:
            return self._cache[action_id]
        d = self._get_json(self._action_key(action_id))
        if d is None:
            return None
        return CorporateAction.from_dict(d)

    def _ensure_loaded(self) -> dict:
        """Populate + return the in-memory ``{action_id: CorporateAction}``
        index from S3 (once)."""
        from corporate_actions import CorporateAction

        if self._cache is not None:
            return self._cache
        cache: dict = {}
        for key in self._list_keys(self._actions_prefix):
            d = self._get_json(key)
            if not d:
                continue
            try:
                action = CorporateAction.from_dict(d)
            except Exception as exc:  # malformed persisted record — skip + WARN
                log.warning(
                    "corporate_actions registry: skipping malformed record %s (%s)",
                    key, exc,
                )
                continue
            cache[action.action_id] = action
        self._cache = cache
        return cache

    def list_actions(
        self,
        *,
        since: str | None = None,
        types: list[str] | None = None,
        ticker: str | None = None,
    ) -> list:
        """List detected actions, optionally filtered.

        ``since`` filters on the persisted ``detected_at`` (ISO compare);
        ``types`` / ``ticker`` filter on the action fields.
        """
        actions = list(self._ensure_loaded().values())
        out = []
        for action in actions:
            if types and action.type not in types:
                continue
            if ticker and action.ticker != ticker:
                continue
            if since is not None:
                # ``since`` is a detected_at floor; re-read the record's
                # detected_at (not held on the dataclass) only if needed.
                rec = self._get_json(self._action_key(action.action_id)) or {}
                if (rec.get("detected_at") or "") < since:
                    continue
            out.append(action)
        return out

    # ── applied markers (modeled now; exercised by a later PR) ───────────
    def is_applied(self, store: str, action_id: str) -> bool:
        """Whether ``action_id`` has been marked applied to ``store``."""
        return self._object_exists(self._applied_key(store, action_id))

    def mark_applied(self, action, store: str, *, run_id: str | None = None) -> bool:
        """Write a write-once applied marker for ``action`` in ``store``.

        Returns ``True`` if newly written, ``False`` if already applied. NOT
        exercised by this PR — the restatement path that calls it ships later.
        """
        from corporate_actions import CorporateAction

        if not isinstance(action, CorporateAction):
            raise TypeError(f"mark_applied expects CorporateAction, got {type(action)!r}")
        key = self._applied_key(store, action.action_id)
        if self._object_exists(key):
            return False
        payload = {
            "action_id": action.action_id,
            "store": store,
            "ticker": action.ticker,
            "ex_date": action.ex_date,
            "applied_at": datetime.now(timezone.utc).isoformat(),
            "applied_run_id": run_id,
        }
        self._put_json(key, payload)
        return True

    # ── the authoritative discrepancy classifier ─────────────────────────
    def explains_discrepancy(self, ticker: str, date, prior: float, new: float):
        """Return the registered ``CorporateAction`` that explains an adjusted-
        close jump from ``prior`` to ``new`` on ``date`` for ``ticker``, or None.

        A match requires BOTH:
          1. the observed multiplicative factor ``new/prior`` equals the
             action's :func:`corporate_actions.expected_factor` — OR its
             INVERSE — within ``_SPLIT_RATIO_TOL`` (a split restates by the
             EXACT factor; the inverse match covers a feed record published
             with the from/to ratio inverted, observed live 2026-06/07 on
             polygon's HON ``2:1`` and DD ``3:1`` records — the restatement
             the overwrite performs is still that action's, so the ERROR
             storm it caused was misclassification, not signal), and
          2. the action's ``ex_date`` is plausibly near ``date`` — on/after
             ``date`` (a split restates dates strictly before its ex date), with
             a few days of early leniency and a bounded look-ahead.

        Only split actions are classified. Matches against the splits detected
        THIS SESSION (``_session_actions``) first, then the PERSISTED registry
        (``_ensure_loaded`` — one lazy S3 list per registry instance): a window
        that re-touches a date restated days earlier must still classify the
        overwrite as that action's expected restatement (2026-07-02: six
        per-date ERROR emails for HON's already-registered separation because
        only session-scope was consulted). Returns the matching action (the
        first, by ex_date) or None.
        """
        from corporate_actions import expected_factor

        if prior is None or new is None or prior <= 0 or new <= 0:
            return None
        observed = float(new) / float(prior)
        try:
            date_ts = pd.Timestamp(date).normalize()
        except Exception:
            return None

        known: dict = {}
        try:
            known.update(self._ensure_loaded())
        except Exception as exc:  # noqa: BLE001 - degrade to session-only scope
            log.warning(
                "corporate_actions registry: persisted-action load failed (%s) "
                "— explains_discrepancy degrades to session-detected scope",
                exc,
            )
        known.update(self._session_actions)  # session wins on id collision
        candidates = [
            a
            for a in known.values()
            if a.type == "split" and a.ticker == str(ticker)
        ]
        # Evaluate by ex_date ascending so the earliest plausible action wins.
        candidates.sort(key=lambda a: a.ex_date)
        for action in candidates:
            try:
                ex_ts = pd.Timestamp(action.ex_date).normalize()
            except Exception:
                continue
            # ex_date plausibility window relative to the discrepancy date.
            if ex_ts < date_ts - timedelta(days=_EX_DATE_EARLY_LENIENCY_DAYS):
                continue
            if ex_ts > date_ts + timedelta(days=_EX_DATE_MAX_AHEAD_DAYS):
                continue
            try:
                expected = expected_factor(action)
            except (NotImplementedError, ValueError):
                continue
            if expected <= 0:
                continue
            if abs(observed - expected) / expected <= _SPLIT_RATIO_TOL:
                return action
            inverse = 1.0 / expected
            if abs(observed - inverse) / inverse <= _SPLIT_RATIO_TOL:
                return action
        return None

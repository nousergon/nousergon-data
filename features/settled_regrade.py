"""D50: rebuild ``features/{D-1}`` from the settled bars, in the morning regrade.

Crucible v2 ruling 2026-10-06 on alpha-engine-config-I12023 (comment
6024224623 §2). D31 publishes ``features/{D}/*.parquet`` once, at the 18:15 ET
EOD collection, from day D's bar as it stood that evening. The official close
is final by then; consolidated ``Volume`` is not (`dates.py`, the
alpha-engine-config-I11354 measurement). D17's 07:30 ET Polygon T+1 overwrite
of ``staging/daily_closes/{D}.parquet`` settles the bar the next morning, and
ArcticDB follows it (D18), but nothing rebuilt the feature snapshot, so every
published ``features/{D}`` carried provisional volume for good. The ruling
makes that rebuild part of the scheduled morning regrade. Without it, parity
rows 3-5 stay BREACH.

WHAT THE REBUILD READS. It runs the same feature code as D31
(`features.compute.build_feature_frame`, the compute half of
`compute_and_write`) for the same trading day. Every input changes nothing
except the settled bar:

* every S3 object D-1's D31 run recorded (`features.input_record`) is served at
  the VersionId that run recorded. `shadow.recompute_lineage.PinnedS3` does the
  serving; this module adds the one live exception. The pin matters for the
  alternative data in particular: `compute._load_cached_alternative` follows
  ``market_data/latest_weekly.json``, and the Saturday weekly advances that
  pointer, so a Monday regrade of Friday read live would build Friday's
  features from the NEXT week's alternative partition;
* ``staging/daily_closes/*`` is read LIVE. That is the settled object D17 has
  just written, and it is the one input whose change is the point;
* ArcticDB is read live with ``before = end = D-1``, as D31 reads it
  (``exclude_trading_day_arctic_rows``), so day D-1's row always comes from
  the daily_closes delta. The read is recorded (content digest plus ``as_of``)
  like D31's.

The corporate-action registry is built from the unpinned client, exactly as
D31 builds it: it keeps its own state and applies a split exactly once.

GUARDS, each a refusal that writes nothing:

1. D17 ran ``ok`` for D-1 in this morning's execution (its newest manifest
   finished within :data:`D17_MAX_AGE_SECONDS`; a same-date no-op is graded by
   the run it points back to, the rule `data_gate.run_manifest_predicate`
   applies). The ``staging/daily_closes/{D-1}`` VersionId D17 recorded is still
   the current one, and D17 graded that write ``settled``.
2. D-1 has a D31 manifest recording ``features/{D-1}/technical.parquet`` and
   the inputs it read, all of them readable back.
3. Every pinned object is readable at its recorded version, and every recorded
   set still digests to what was recorded.
4. The dead-column postflight is FATAL here (``zero_variance_fatal=True``). D31
   writes a degraded snapshot because the EOD append waits on it. A rebuild
   has nobody waiting on it, so it refuses instead of replacing a published
   snapshot with a known-defective one.
5. The rebuild produces every feature group D31 published, so no group is
   left provisional beside rebuilt ones.

IDEMPOTENT. A run that finds ``features/{D-1}/settlement.json`` naming the
current daily_closes VersionId, with every key it rebuilt still at the version
it wrote, has nothing to do. It reports ``skipped`` with an
``already_regraded`` reason, which the run manifest records as
``not_applicable`` / ``no_new_data_declared``.

WRITES. Only ``features/{D-1}/{group}.parquet`` and then, LAST, the marker
``features/{D-1}/settlement.json``. The marker carries ``backfilled_at``, which
`DATA_QUALITY_WINDOWS.md` requires beside any re-derived prefix, and the
VersionIds this rebuild superseded, so every run that read the D31 original can
still read it. A crash before the marker leaves rebuilt groups with no marker;
the next run rebuilds the same bytes and writes it. It never writes
``features/registry.json``, ``features/{D-1}/schema_version.json`` or
``features/metron_supplemental/``: none of them depends on the bar, and the
first is a fleet-wide pointer whose newest writer is the EOD run.

PROVISIONAL ROWS. D17 can carry a ticker's row from the existing object
instead of fetching it (`collectors.daily_closes._settlement_guards`: retained
on empty, or a downgrade it refused). D17 grades those rows one by one, keyed
``<key>#<ticker>``. Those tickers are still provisional after the rebuild. Each
rebuilt per-ticker key carries the same per-row reading for each of them, and
the marker names them, so `shadow.parity`'s row-level settlement reader sees
exactly which rows stayed provisional.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from features.input_record import InputRecorder, RecordedInputs, parse_refs
from shadow.recompute_lineage import PinnedS3, ReadOnlyViolation

log = logging.getLogger(__name__)

UNIT_ID = "D50"
MODE = "features_settled_regrade"

#: Run-manifest prefixes this rebuild reads (`registry.d/units/D17-*.yaml`,
#: `D31-*.yaml`).
D17_MANIFEST_PREFIX = "data_collection/runs/D17"
D31_MANIFEST_PREFIX = "data_collection/runs/D31"

#: Inputs read LIVE instead of at the D31-recorded version. One prefix: the
#: settled bar is the only input this rebuild exists to change.
LIVE_INPUT_PREFIXES: tuple[str, ...] = ("staging/daily_closes/",)

MARKER_TEMPLATE = "features/{trading_day}/settlement.json"
MARKER_SCHEMA = "features_settlement_marker.v1"

#: The ``skip_reason`` prefix of an idempotent repeat.
#: `weekly_collector._run_whole_mode_unit` maps it to
#: `run_units.NOT_RUN_NO_NEW_DATA_DECLARED`.
SKIP_ALREADY_REGRADED = "already_regraded"

#: How old D17's newest manifest for D-1 may be and still count as THIS
#: morning's run. A Step Functions execution start time does not reach the box,
#: so the bound is derived from the schedule instead:
#: `tests/test_features_settled_regrade.py` pins it at or above the worst case
#: from the morning cron to this workload's launch (every earlier workload's
#: SSM-online budget plus its declared cap, plus this one's SSM budget), and
#: below the ~48 h between the Saturday weekly's D17 (which also keys Friday)
#: and Monday's.
D17_MAX_AGE_SECONDS = 8 * 3600

#: The run-manifest reason a same-date no-op files
#: (`data_gate.run_manifest_predicate.SAME_DATE_NOOP_REASON`).
SAME_DATE_NOOP_REASON = "no_new_data_declared"

BAR_SETTLEMENT_GUARD_NAME = "bar_settlement"
BAR_SETTLED = "settled"
BAR_PROVISIONAL = "provisional"

#: The guard this unit files for its own preconditions.
PRECONDITION_GUARD = "features_settled_regrade"

#: Keys the rebuild may write, and keys it must never write even under the
#: allowed prefix.
_WRITE_RE = re.compile(r"^features/(\d{4}-\d{2}-\d{2})/([a-z_]+)\.parquet$")
FORBIDDEN_WRITE_SUFFIXES: tuple[str, ...] = ("registry.json", "schema_version.json")
FORBIDDEN_WRITE_PREFIXES: tuple[str, ...] = ("features/metron_supplemental/",)

#: The anchor key that identifies the D31 run which published day D's snapshot.
_ANCHOR_GROUP = "technical"

#: The per-ticker feature groups (every group but ``macro``, which publishes one
#: row per date).
_ROW_GROUPS_EXCLUDED = frozenset({"macro"})


class RegradeRefused(RuntimeError):
    """A precondition did not hold. Nothing was written."""


def marker_key(trading_day: str) -> str:
    return MARKER_TEMPLATE.format(trading_day=trading_day)


def daily_closes_key(trading_day: str) -> str:
    return f"staging/daily_closes/{trading_day}.parquet"


def feature_key(trading_day: str, group: str) -> str:
    return f"features/{trading_day}/{group}.parquet"


def check_write_key(key: str, trading_day: str) -> None:
    """Refuse any key this unit does not own. Called before every PUT."""
    if key == marker_key(trading_day):
        return
    if any(key.startswith(p) for p in FORBIDDEN_WRITE_PREFIXES) or any(
        key.endswith(s) for s in FORBIDDEN_WRITE_SUFFIXES
    ):
        raise RegradeRefused(f"refusing to write {key!r}: D50 never writes it")
    match = _WRITE_RE.match(key)
    if match is None or match.group(1) != trading_day:
        raise RegradeRefused(
            f"refusing to write {key!r}: D50 writes only features/{trading_day}/<group>.parquet "
            f"and {marker_key(trading_day)}"
        )


# ---------------------------------------------------------------------------
# Reading manifests and heads
# ---------------------------------------------------------------------------


def _error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


def _head(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def _version(head: Mapping[str, Any] | None) -> str | None:
    if head is None:
        return None
    version = head.get("VersionId")
    return version if isinstance(version, str) and version and version != "null" else None


def _json_manifests(client: Any, bucket: str, prefix: str) -> list[tuple[str, dict[str, Any]]]:
    """Every ``*.json`` manifest under ``prefix``, in key order (run ids are ULIDs)."""
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents") or [] if o["Key"].endswith(".json"))
    out = []
    for key in sorted(keys):
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        doc = json.loads(body)
        if not isinstance(doc, dict):
            raise RegradeRefused(f"s3://{bucket}/{key} is not a run manifest object")
        out.append((key, doc))
    return out


def _parse_ts(value: Any, where: str) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise RegradeRefused(f"{where} has no timestamp")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise RegradeRefused(f"{where}={value!r} is not an RFC3339 timestamp") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _output(manifest: Mapping[str, Any], key: str) -> dict[str, Any] | None:
    for out in manifest.get("outputs") or []:
        if str(out.get("key") or "") == key:
            return out
    return None


def d17_reading(
    client: Any, bucket: str, trading_day: str, *, now: dt.datetime
) -> dict[str, Any]:
    """Guard 1: D17 settled ``staging/daily_closes/{D-1}`` this morning, and nothing wrote it since.

    Returns what the rebuild needs from D17: the daily_closes VersionId, D17's
    whole-key settlement reading, and the tickers D17 carried as provisional.
    """
    dc_key = daily_closes_key(trading_day)
    manifests = _json_manifests(client, bucket, f"{D17_MANIFEST_PREFIX}/{trading_day}/")
    if not manifests:
        raise RegradeRefused(
            f"no D17 run manifest for {trading_day}: the morning enrich has not settled the bar"
        )
    newest_key, newest = manifests[-1]
    finished = _parse_ts(newest.get("finished"), f"{newest_key}:finished")
    age = (now - finished).total_seconds()
    if age > D17_MAX_AGE_SECONDS:
        raise RegradeRefused(
            f"D17's newest manifest for {trading_day} ({newest_key}) finished {int(age)} s ago, "
            f"more than {D17_MAX_AGE_SECONDS} s: it is not this morning's run"
        )
    real_key, real = newest_key, newest
    if str(newest.get("status")) == "not_applicable" and str(newest.get("reason")) == SAME_DATE_NOOP_REASON:
        earlier = [
            (k, m)
            for k, m in manifests[:-1]
            if not (str(m.get("status")) == "not_applicable" and str(m.get("reason")) == SAME_DATE_NOOP_REASON)
        ]
        if not earlier:
            raise RegradeRefused(
                f"D17's newest manifest {newest_key} is a same-date no-op with no earlier run of "
                f"{trading_day} on record"
            )
        real_key, real = earlier[-1]
    if str(real.get("status")) != "ok":
        raise RegradeRefused(
            f"D17 manifest {real_key} reports status={real.get('status')!r}: the bar is not settled"
        )
    out = _output(real, dc_key)
    if out is None:
        raise RegradeRefused(f"D17 manifest {real_key} records no output {dc_key}")
    recorded = out.get("version_id")
    if not recorded:
        raise RegradeRefused(
            f"D17 manifest {real_key} recorded {dc_key} with no VersionId "
            f"(version_capture={out.get('version_capture')!r}), so it cannot be tied to the live object"
        )
    current = _version(_head(client, bucket, dc_key))
    if current != recorded:
        raise RegradeRefused(
            f"{dc_key} is at VersionId {current!r}, not the {recorded!r} D17 recorded in {real_key}: "
            "something wrote it after the settled enrich"
        )
    whole = [
        g
        for g in real.get("guards") or []
        if g.get("guard") == BAR_SETTLEMENT_GUARD_NAME and str(g.get("key") or "") == dc_key
    ]
    if not whole:
        raise RegradeRefused(f"D17 manifest {real_key} carries no bar_settlement reading for {dc_key}")
    verdicts = {str(g.get("verdict")) for g in whole}
    if verdicts != {BAR_SETTLED}:
        raise RegradeRefused(
            f"D17 graded {dc_key} {sorted(verdicts)} in {real_key}, not settled"
        )
    row_prefix = f"{dc_key}#"
    provisional = sorted(
        {
            str(g["key"])[len(row_prefix):]
            for g in real.get("guards") or []
            if g.get("guard") == BAR_SETTLEMENT_GUARD_NAME
            and str(g.get("key") or "").startswith(row_prefix)
            and str(g.get("verdict")) == BAR_PROVISIONAL
        }
    )
    return {
        "manifest": real_key,
        "run_id": real.get("run_id"),
        "finished": real.get("finished"),
        "visited_by": newest_key if newest_key != real_key else None,
        "key": dc_key,
        "version_id": recorded,
        "settlement": whole[-1],
        "provisional_tickers": provisional,
    }


def d31_reading(client: Any, bucket: str, trading_day: str) -> tuple[str, dict[str, Any], RecordedInputs]:
    """Guard 2: the D31 run that published ``features/{D-1}`` and what it read."""
    anchor = feature_key(trading_day, _ANCHOR_GROUP)
    manifests = [
        (k, m)
        for k, m in _json_manifests(client, bucket, f"{D31_MANIFEST_PREFIX}/{trading_day}/")
        if _output(m, anchor) is not None
    ]
    if not manifests:
        raise RegradeRefused(f"no D31 run manifest records {anchor}: there is no snapshot to rebuild")
    key, manifest = max(manifests, key=lambda km: str(km[1].get("finished") or ""))
    pins = parse_refs(manifest.get("inputs") or [])
    if pins.empty:
        raise RegradeRefused(f"D31 manifest {key} records no inputs, so they cannot be pinned")
    if pins.unreadable:
        raise RegradeRefused(
            f"D31 manifest {key} recorded input(s) this rebuild cannot read back: {list(pins.unreadable)[:5]}"
        )
    return key, manifest, pins


def published_groups(manifest: Mapping[str, Any], trading_day: str) -> dict[str, dict[str, Any]]:
    """group -> D31's output record, for every feature group D31 published."""
    out: dict[str, dict[str, Any]] = {}
    for entry in manifest.get("outputs") or []:
        match = _WRITE_RE.match(str(entry.get("key") or ""))
        if match and match.group(1) == trading_day:
            out[match.group(2)] = dict(entry)
    return out


def already_regraded(
    client: Any, bucket: str, trading_day: str, dc_version: str | None
) -> dict[str, Any] | None:
    """The marker, when it already describes the current settled bar and the current bytes."""
    key = marker_key(trading_day)
    if _head(client, bucket, key) is None:
        return None
    marker = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    if str((marker.get("daily_closes") or {}).get("version_id")) != str(dc_version):
        return None
    for written_key, record in (marker.get("rebuilt") or {}).items():
        if _version(_head(client, bucket, written_key)) != record.get("version_id"):
            return None
    return marker


# ---------------------------------------------------------------------------
# The pinned client with the one live prefix
# ---------------------------------------------------------------------------


class SettledInputsS3(PinnedS3):
    """`PinnedS3`, except :data:`LIVE_INPUT_PREFIXES` are read live.

    Everything D31 recorded is served at its recorded VersionId, recorded sets
    are digest-checked, unrecorded keys read as absent, and any non-read call
    raises. A live key is served from the real client, including its real
    ``NoSuchKey``, and is listed in :attr:`live_reads`.
    """

    def __init__(self, client: Any, pins: RecordedInputs, *, live_prefixes: Iterable[str] = LIVE_INPUT_PREFIXES) -> None:
        super().__init__(client, pins)
        self._live = tuple(live_prefixes)
        #: key -> VersionId served live.
        self.live_reads: dict[str, str | None] = {}

    def _is_live(self, key: str) -> bool:
        return any(key.startswith(p) for p in self._live)

    def get_object(self, *, Bucket: str, Key: str, **kwargs: Any) -> dict[str, Any]:
        if self._is_live(Key):
            if kwargs.get("VersionId"):
                raise ReadOnlyViolation(f"the feature code asked for a version of {Key!r} itself")
            resp = self._client.get_object(Bucket=Bucket, Key=Key)
            self.live_reads[Key] = resp.get("VersionId")
            return resp
        return super().get_object(Bucket=Bucket, Key=Key, **kwargs)

    def head_object(self, *, Bucket: str, Key: str, **kwargs: Any) -> dict[str, Any]:
        if self._is_live(Key):
            return self._client.head_object(Bucket=Bucket, Key=Key)
        return super().head_object(Bucket=Bucket, Key=Key, **kwargs)

    def _listing(self, bucket: str, prefix: str) -> list[str]:
        if self._is_live(prefix):
            keys: list[str] = []
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                keys.extend(o["Key"] for o in page.get("Contents") or [])
            return sorted(keys)
        return super()._listing(bucket, prefix)


# ---------------------------------------------------------------------------
# The rebuild
# ---------------------------------------------------------------------------


def _live_price_source(trading_day: str, recorder: InputRecorder) -> Callable[[Any, str], Any]:
    """ArcticDB read live, ``before = end = D-1``, recorded like D31's read."""
    import pandas as pd

    from features import compute

    day = pd.Timestamp(trading_day)

    def load(s3: Any, bucket: str) -> Any:
        return compute._load_price_source(s3, bucket, end=day, before=day, recorder=recorder)

    return load


def _default_build(
    trading_day: str,
    bucket: str,
    *,
    s3: Any,
    registry_client: Any,
    recorder: InputRecorder,
) -> Any:
    from features import compute

    return compute.build_feature_frame(
        trading_day,
        bucket,
        s3=s3,
        registry_client=registry_client,
        recorder=recorder,
        exclude_trading_day_arctic_rows=True,
        price_source_loader=_live_price_source(trading_day, recorder),
    )


def _settlement_guards(
    trading_day: str,
    written: Mapping[str, Any],
    d17: Mapping[str, Any],
    frames: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """One whole-key `bar_settlement` reading per rebuilt key, plus one per provisional row.

    The whole-key reading is D17's own, re-keyed: the rebuilt bytes rest on the
    bar D17 fetched and graded. A ticker D17 carried as provisional stays
    provisional in every per-ticker group that contains it.
    """
    source = d17["settlement"]
    guards: list[dict[str, Any]] = []
    for group, key in sorted(written.items()):
        guards.append(
            {
                "guard": BAR_SETTLEMENT_GUARD_NAME,
                "mode": source.get("mode"),
                "verdict": source.get("verdict"),
                "detail": (
                    f"rebuilt from {d17['key']} at VersionId {d17['version_id']} "
                    f"(D17 {d17['run_id']}), whose fetch D17 graded: {source.get('detail')}"
                )[:1500],
                "key": key,
                "value": source.get("value"),
                "baseline": source.get("baseline"),
            }
        )
        if group in _ROW_GROUPS_EXCLUDED or not d17["provisional_tickers"]:
            continue
        frame = frames.get(group)
        present = set(frame["ticker"]) if frame is not None and "ticker" in frame.columns else set()
        for ticker in d17["provisional_tickers"]:
            if ticker not in present:
                continue
            guards.append(
                {
                    "guard": BAR_SETTLEMENT_GUARD_NAME,
                    "mode": source.get("mode"),
                    "verdict": BAR_PROVISIONAL,
                    "detail": (
                        f"row {ticker} rebuilt from a row D17 carried, not fetched, in "
                        f"{d17['key']}#{ticker} (graded provisional there): still provisional"
                    ),
                    "key": f"{key}#{ticker}",
                    "value": None,
                    "baseline": None,
                }
            )
    return guards


def _code_sha() -> str:
    import os
    import subprocess

    for var in ("NE_DATA_CODE_SHA", "GITHUB_SHA"):
        if os.environ.get(var):
            return os.environ[var]
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown (git rev-parse failed)"


def regrade(
    trading_day: str,
    *,
    bucket: str,
    client: Any,
    registry_client: Any = None,
    now: dt.datetime | None = None,
    build: Callable[..., Any] | None = None,
    dry_run: bool = False,
    code_sha: str | None = None,
) -> dict[str, Any]:
    """Rebuild ``features/{trading_day}`` from the settled bar. See the module docstring.

    Returns the mode's collector result (``status`` ``ok`` | ``skipped`` |
    ``error``). A refused precondition returns ``error`` naming it and writes
    nothing; any other exception propagates.

    ``registry_client`` builds the corporate-action registry; D31 builds it
    from its unwrapped client, and so does this (``client`` when omitted).
    ``build`` replaces `features.compute.build_feature_frame` with the same
    keyword contract (tests).
    """
    from features import compute
    from features.writer import snapshot_group_frames

    now = now or dt.datetime.now(dt.UTC)
    started = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    result: dict[str, Any] = {"status": "error", "date": trading_day, "started_at": started}
    try:
        dc_version = _version(_head(client, bucket, daily_closes_key(trading_day)))
        done = already_regraded(client, bucket, trading_day, dc_version)
        if done is not None:
            result.update(
                status="skipped",
                skip_reason=(
                    f"{SKIP_ALREADY_REGRADED}: {marker_key(trading_day)} (backfilled_at "
                    f"{done.get('backfilled_at')}) already rebuilt features/{trading_day} from "
                    f"{daily_closes_key(trading_day)} VersionId {dc_version}, and every key it "
                    "wrote is still at the version it wrote"
                ),
                tickers_computed=0,
                groups_written={},
            )
            return result

        d17 = d17_reading(client, bucket, trading_day, now=now)
        d31_key, d31, pins = d31_reading(client, bucket, trading_day)
        d31_groups = published_groups(d31, trading_day)

        recorder = InputRecorder(bucket)
        facade = SettledInputsS3(client, pins)
        s3 = recorder.wrap(facade)
        try:
            frame_build = (build or _default_build)(
                trading_day,
                bucket,
                s3=s3,
                registry_client=registry_client if registry_client is not None else client,
                recorder=recorder,
            )
        except ReadOnlyViolation:
            raise
        except Exception as exc:
            # A pinned read that failed surfaces in the feature code as a
            # missing key; when the facade recorded why, that is the cause.
            if facade.problems:
                raise RegradeRefused(
                    "the D31-recorded inputs could not be served as recorded: "
                    + "; ".join(facade.problems[:5])
                ) from exc
            raise
        problems = list(facade.problems) + facade.set_problems()
        if problems:
            raise RegradeRefused(
                "the D31-recorded inputs could not be served as recorded: " + "; ".join(problems[:5])
            )
        if frame_build.features_df is None:
            raise RegradeRefused("the rebuild loaded no price data")
        input_refs = recorder.freeze()
        features_df = frame_build.features_df

        # Guard 4: the dead-column postflight is fatal on a rebuild.
        try:
            compute.assert_snapshot_columns_live(features_df)
        except RuntimeError as exc:
            raise RegradeRefused(f"the rebuilt snapshot fails the dead-column postflight: {exc}") from exc

        frames = snapshot_group_frames(trading_day, features_df)
        # Guard 5. A superset is expected: D31 also writes the `factor_loading`
        # group its manifest does not record (weekly_collector._FEATURE_GROUPS),
        # and the rebuild rewrites it with the rest.
        if not set(d31_groups) <= set(frames):
            raise RegradeRefused(
                f"the rebuild produced groups {sorted(frames)} but D31 ({d31_key}) published "
                f"{sorted(d31_groups)}: rebuilding without {sorted(set(d31_groups) - set(frames))} "
                "would leave some groups provisional"
            )
        written_keys = {group: feature_key(trading_day, group) for group in frames}
        for key in written_keys.values():
            check_write_key(key, trading_day)
        check_write_key(marker_key(trading_day), trading_day)

        if dry_run:
            result.update(
                status="ok_dry_run",
                tickers_computed=int(frame_build.n_ok),
                groups_written={},
            )
            return result

        replaced = {key: _version(_head(client, bucket, key)) for key in written_keys.values()}
        rebuilt: dict[str, dict[str, Any]] = {}
        for group, key in sorted(written_keys.items()):
            group_df, body = frames[group]
            resp = client.put_object(Bucket=bucket, Key=key, Body=body)
            rebuilt[key] = {
                "group": group,
                "rows": len(group_df),
                "sha256": hashlib.sha256(body).hexdigest(),
                "version_id": resp.get("VersionId"),
            }

        backfilled_at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        marker = {
            "schema_version": MARKER_SCHEMA,
            "unit_id": UNIT_ID,
            "trading_day": trading_day,
            "backfilled_at": backfilled_at,
            "reason": (
                "Scheduled settled-bar rebuild of features/{D-1} (Crucible v2 ruling on "
                "alpha-engine-config-I12023, comment 6024224623 §2). These bytes are re-derived: "
                "the D31 originals are named under `superseded` and stay readable by VersionId."
            ),
            "code_sha": code_sha or _code_sha(),
            "daily_closes": {
                "key": d17["key"],
                "version_id": d17["version_id"],
                "d17_manifest": d17["manifest"],
                "d17_run_id": d17["run_id"],
                "settlement": d17["settlement"].get("verdict"),
            },
            "superseded": {
                "d31_manifest": d31_key,
                "d31_run_id": d31.get("run_id"),
                "d31_finished": d31.get("finished"),
                "outputs": {
                    feature_key(trading_day, g): out.get("version_id") for g, out in sorted(d31_groups.items())
                },
                "replaced_version_ids": replaced,
            },
            "rebuilt": rebuilt,
            "inputs": {
                "pinned_to_d31": sorted(facade.served),
                "read_live": dict(sorted(facade.live_reads.items())),
                "unrecorded_requests": sorted(set(facade.unrecorded))[:50],
                "recorded": input_refs,
            },
            "provisional_tickers": d17["provisional_tickers"],
            "provisional_note": (
                "D17 carried these rows rather than fetching them, and graded them provisional; "
                "their per-ticker feature rows are still built on that bar. Cross-sectional "
                "columns (z-scores, ranks) depend on every row of the day."
            ),
        }
        # Written LAST: a marker only ever describes groups already in place.
        marker_resp = client.put_object(
            Bucket=bucket,
            Key=marker_key(trading_day),
            Body=json.dumps(marker, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )
        guards = _settlement_guards(
            trading_day, written_keys, d17, {g: f for g, (f, _b) in frames.items()}
        )
        guards.append(
            {
                "guard": PRECONDITION_GUARD,
                "mode": "enforce",
                "verdict": "ok",
                "detail": (
                    f"D17 {d17['run_id']} ok and settled at {d17['key']} VersionId {d17['version_id']}; "
                    f"D31 {d31.get('run_id')} inputs pinned ({len(facade.served)} object(s) served at the "
                    f"recorded version, {len(pins.sets)} set(s) digest-checked); "
                    f"{len(facade.live_reads)} daily_closes object(s) read live; dead-column postflight fatal "
                    "and passed; every group D31 published was rebuilt"
                ),
                "key": marker_key(trading_day),
                "value": None,
                "baseline": None,
            }
        )
        result.update(
            status="ok",
            tickers_computed=int(frame_build.n_ok),
            tickers_skipped=int(frame_build.n_skip),
            tickers_errored=int(frame_build.n_err),
            groups_written={g: len(f) for g, (f, _b) in frames.items()},
            marker_key=marker_key(trading_day),
            marker_version_id=marker_resp.get("VersionId"),
            superseded=marker["superseded"],
            provisional_tickers=d17["provisional_tickers"],
            input_refs=input_refs,
            guards=guards,
        )
        return result
    except (RegradeRefused, ReadOnlyViolation) as exc:
        log.error("D50 features settled regrade refused for %s: %s", trading_day, exc)
        result.update(error=f"{type(exc).__name__}: {exc}")
        return result

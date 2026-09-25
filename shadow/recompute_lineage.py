"""Recompute lineage: re-run D31 over each side's recorded inputs and prove it reproduces them.

`alpha-engine-config-I11203`, Brian's "proven v1 cause" ruling (2026-09-24): a
parity row reads `v1_cause` only on machine-checked evidence, with no band
widened and no key excluded. The two evidence kinds `shadow.parity` already
admits read a PRODUCER's own settlement stamp. A DERIVED artifact has none:
`features/{D}/*.parquet` is computed by D31 from other keys, and its v1 copy
(16:17 ET, on the day's provisional closes) and the shadow's (18:55 ET, on
settled closes) differ on every report, with nothing to say why.

THE PROOF, and why each half is needed. For one trading day, let ``f`` be the
feature code the recompute runs, ``I_v`` / ``I_s`` the inputs v1's and the
shadow's D31 runs RECORDED (`features.input_record`), and ``V`` / ``S`` the
files each published.

1. ``f(I_v) == V`` byte for byte. The code reproduces v1's file from v1's own
   inputs — so no code difference between v1 and the shadow reaches this file.
   A code change that alters the output fails here, and is refused.
2. ``f(I_s) == S`` byte for byte. The same, for the shadow. It also proves the
   recorded inputs are COMPLETE: any input the record misses is read at today's
   state in both recomputes, and it could not reproduce two different published
   files from one state.
3. With one ``f`` and every unrecorded input held fixed, ``V`` and ``S`` differ
   ONLY through the recorded inputs that differ, ``Δ = I_v ⊕ I_s``. Each input
   in ``Δ`` must itself grade `v1_cause` under the input key's own parity
   comparison — v1's writer stamped it `provisional`, the shadow's writer
   `settled` (kind 1). The evidence chains; it is never assumed.
   `shadow.parity` grades that chain itself, at report time, from the objects
   and manifests named here (:class:`shadow.parity.RecomputeLineage`).

This module does halves 1 and 2 — the expensive ones — and lists ``Δ``. It
writes one record per trading day, ``lineage/D31/{trading_day}.json`` in the
parity store. A record that cannot establish a half says why and grants nothing.

WHERE IT RUNS: the tail of the `shadow-sameday` dispatch, the same box and
dispatch that produced the shadow D31 output it explains, after that day's
`parity`. Both D31 outputs for the day exist by then; the report that reads the
record is the NEXT trading day's (its `prior_day_settled` re-grade). Measured
2026-09-23: v1's D31 took 24 min, but 21 min of that was
`features.compute.audit_action_jumps` — a logging-only split audit that writes
nothing into the snapshot — and the compute itself was 42 s. The recompute runs
without the corporate-action registry (only when the replayed run RECORDED
that nothing was restated), which skips that audit, so one side costs its two
ArcticDB ``as_of`` reads plus the compute. Measured 2026-09-25 from OUTSIDE the
region: 239 s of reads per side (a current read of the same window: ~97 s),
~45 s of compute. Two sides: under 10 minutes, less in region.

WHAT BYTE EQUALITY NEEDS, measured on 2026-09-23/25 (read-only replay). With
v1's ArcticDB read recovered exactly (``as_of``), every group but `technical`
reproduced byte for byte on any host. `technical` depends on the numeric
environment: its four log-return columns (`beta_60d`, `idio_vol_60d`,
`vol_ratio_10_60`, `residual_momentum_ratio`) move by up to ~7e-13 relative
with numpy's SIMD dispatch (AVX-512 vs AVX2 ``log``) and with numpy's version.
Replayed with v1's own stack — numpy 1.26.4, AVX-512 on, as on the c5.large v1
ran on — v1's published file came back byte for byte; with AVX-512 masked,
3,433 cells differed. No tolerance absorbs that (Brian, 2026-09-25): D31 and
this recompute both compute under `features.numeric_pin`, which fixes the
dispatch, and each run records the environment it actually computed under. A
side whose recorded environment is unpinned, or differs from the recompute's
own, is refused by name (:func:`features.numeric_pin.mismatch`) before any
compute — a byte mismatch that is really a CPU difference is never left to be
read as a code or input difference.

READ-ONLY. Every S3 read goes through :class:`PinnedS3`, which raises on any
mutating call. The only write is the record itself, by the CLI.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from features import numeric_pin
from features.input_record import RecordedInputs, frames_digest, parse_refs, set_digest

SCHEMA_VERSION = "d31_recompute_lineage.v1"

#: The unit whose derived outputs this covers.
LINEAGE_UNIT = "D31"

#: Where the record lives, relative to the parity store
#: (``s3://alpha-engine-research/data_collection``).
LINEAGE_RECORD_TEMPLATE = "lineage/D31/{trading_day}.json"

#: The run manifest prefix of D31 (`registry.d/units/D31-features-daily.yaml`).
D31_MANIFEST_PREFIX = "data_collection/runs/D31"

#: The derived keys the record covers: every feature-group parquet D31 publishes.
FEATURE_KEY_RE = re.compile(r"^features/(\d{4}-\d{2}-\d{2})/([a-z_]+)\.parquet$")


def lineage_record_key(trading_day: dt.date) -> str:
    return LINEAGE_RECORD_TEMPLATE.format(trading_day=trading_day.isoformat())


def is_lineage_key(live_key: str) -> bool:
    """A key whose v1_cause may be proven by recompute lineage (a D31 feature group)."""
    return FEATURE_KEY_RE.match(live_key) is not None


def host_fingerprint() -> dict[str, Any]:
    """The replaying host's numeric stack: what byte equality of float output depends on."""
    import platform

    import numpy
    import pandas
    import pyarrow

    try:
        from numpy._core._multiarray_umath import __cpu_features__ as cpu_features
    except ImportError:  # numpy < 2
        from numpy.core._multiarray_umath import __cpu_features__ as cpu_features
    return {
        "machine": platform.machine(),
        "python": platform.python_version(),
        "numpy": numpy.__version__,
        "pandas": pandas.__version__,
        "pyarrow": pyarrow.__version__,
        "cpu_features": sorted(name for name, on in cpu_features.items() if on),
        "numeric_env": numeric_pin.flat(numeric_pin.effective()),
    }


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


class ReadOnlyViolation(RuntimeError):
    """The recompute attempted an S3 call that is not a read."""


class _NoSuchKey(Exception):
    """Stand-in for a client without modelled exceptions (test fakes)."""

    def __init__(self, key: str) -> None:
        super().__init__(f"NoSuchKey: {key}")
        self.response = {"Error": {"Code": "NoSuchKey", "Message": key}}


# ---------------------------------------------------------------------------
# The pinned, read-only S3 facade
# ---------------------------------------------------------------------------

#: The read calls :class:`PinnedS3` implements. Anything else raises.
_READ_CALLS = frozenset({"get_object", "head_object", "list_objects_v2", "get_paginator", "exceptions", "meta"})


class PinnedS3:
    """Serves the feature code exactly the objects one run recorded, and nothing else.

    * A recorded object is served at its recorded VersionId. When that is
      still the current version (the common case) a plain GET serves it,
      checked against the VersionId it returns, so no ``s3:GetObjectVersion``
      grant is needed; otherwise a versioned GET does.
    * A key under a recorded SET (``<prefix>*``) is served current, and every
      member served is remembered: after the run, :meth:`set_problems`
      compares their digest with the recorded one.
    * Any other key raises ``NoSuchKey`` — what the recorded run saw of a key
      it never read successfully — and is named in :attr:`unrecorded`.
    * A listing returns the recorded keys under the prefix (plus the current
      members of a recorded set). A lister that then reads them gets exactly
      what the recorded run read.

    ``physical_prefixes`` lists where a recorded key may physically live, in
    order: a shadow run reads its OWN writes back from under its shadow root
    (`shadow.interceptor` "own write"), so the shadow side tries the shadow
    root first. A VersionId belongs to exactly one key, so the first key that
    serves it is the one the run read.
    """

    def __init__(self, client: Any, pins: RecordedInputs, *, physical_prefixes: Iterable[str] = ("",)) -> None:
        self._client = client
        self._pins = pins
        self._prefixes = tuple(physical_prefixes)
        #: key -> what was served: physical key, VersionId, sha256.
        self.served: dict[str, dict[str, Any]] = {}
        #: set prefix -> [(key, VersionId)] served from it.
        self.set_members: dict[str, list[tuple[str, str | None]]] = {}
        self.unrecorded: list[str] = []
        self.problems: list[str] = []

    # -- plumbing ---------------------------------------------------------
    @property
    def exceptions(self) -> Any:
        return getattr(self._client, "exceptions", None) or _FallbackExceptions

    def __getattr__(self, name: str) -> Any:
        if name in _READ_CALLS:
            return getattr(self._client, name)
        raise ReadOnlyViolation(f"the recompute is read-only; refused S3 call {name!r}")

    def _no_such_key(self, key: str) -> Exception:
        cls = getattr(self.exceptions, "NoSuchKey", None)
        if cls is None or cls is _NoSuchKey:
            return _NoSuchKey(key)
        try:
            return cls({"Error": {"Code": "NoSuchKey", "Message": key}}, "GetObject")
        except TypeError:
            return _NoSuchKey(key)

    def _set_for(self, key: str) -> str | None:
        for prefix in self._pins.sets:
            if key.startswith(prefix) and "/" not in key[len(prefix):]:
                return prefix
        return None

    # -- reads --------------------------------------------------------------
    def get_object(self, *, Bucket: str, Key: str, **kwargs: Any) -> dict[str, Any]:  # noqa: N803 - boto3 spelling
        if kwargs.get("VersionId"):
            raise ReadOnlyViolation(f"the feature code asked for a version of {Key!r} itself; not a D31 read")
        if Key in self._pins.objects:
            return self._pinned(Bucket, Key)
        prefix = self._set_for(Key)
        if prefix is not None:
            resp = self._client.get_object(Bucket=Bucket, Key=Key)
            body = resp["Body"].read()
            version = resp.get("VersionId")
            self.set_members.setdefault(prefix, []).append((Key, version))
            return {**resp, "Body": io.BytesIO(body)}
        self.unrecorded.append(Key)
        raise self._no_such_key(Key)

    def head_object(self, *, Bucket: str, Key: str, **kwargs: Any) -> dict[str, Any]:  # noqa: N803
        if Key in self._pins.objects:
            resp = self._pinned(Bucket, Key)
            return {k: v for k, v in resp.items() if k != "Body"}
        self.unrecorded.append(Key)
        raise self._no_such_key(Key)

    def _pinned(self, bucket: str, key: str) -> dict[str, Any]:
        pin = self._pins.objects[key]
        if not pin.version_id:
            self.problems.append(f"{key}: recorded with no VersionId, so it cannot be read as recorded")
            raise self._no_such_key(key)
        errors: list[str] = []
        for prefix in self._prefixes:
            physical = f"{prefix}{key}"
            resp = self._read_version(bucket, physical, pin.version_id, errors)
            if resp is None:
                continue
            body = resp["Body"].read()
            etag = str(resp.get("ETag") or "").strip('"') or None
            if pin.etag and etag and etag != pin.etag:
                self.problems.append(f"{key}: served ETag {etag} is not the recorded {pin.etag}")
            self.served[key] = {
                "physical_key": physical,
                "version_id": pin.version_id,
                "sha256": _sha256(body),
            }
            return {**resp, "Body": io.BytesIO(body)}
        self.problems.append(f"{key}: recorded version {pin.version_id} not readable ({'; '.join(errors)})")
        raise self._no_such_key(key)

    def _read_version(self, bucket: str, physical: str, version_id: str, errors: list[str]) -> dict[str, Any] | None:
        try:
            head = self._client.head_object(Bucket=bucket, Key=physical)
        except Exception as exc:  # noqa: BLE001 - a missing key is an answer; recorded
            head = None
            code = _error_code(exc)
            if code not in {"NoSuchKey", "404", "NotFound"}:
                errors.append(f"{physical}: head {code or type(exc).__name__}")
        try:
            if head is not None and head.get("VersionId") == version_id:
                resp = self._client.get_object(Bucket=bucket, Key=physical)
                if resp.get("VersionId") == version_id:
                    return resp
                # Overwritten between the HEAD and the GET: read the version.
            return self._client.get_object(Bucket=bucket, Key=physical, VersionId=version_id)
        except Exception as exc:  # noqa: BLE001 - "not this key's version" is an answer; recorded
            code = _error_code(exc)
            if code not in {"NoSuchKey", "NoSuchVersion", "404", "NotFound", "InvalidArgument", "400"}:
                errors.append(f"{physical}: {code or type(exc).__name__}: {exc}")
            else:
                errors.append(f"{physical}: {code}")
            return None

    def list_objects_v2(self, *, Bucket: str, Prefix: str = "", MaxKeys: int = 1000, **_: Any) -> dict[str, Any]:  # noqa: N803
        keys = self._listing(Bucket, Prefix)
        return {"Contents": [{"Key": k} for k in keys[:MaxKeys]], "KeyCount": min(len(keys), MaxKeys)}

    def get_paginator(self, name: str) -> "_Paginator":
        if name != "list_objects_v2":
            raise ReadOnlyViolation(f"the recompute serves list_objects_v2 pagination only, not {name!r}")
        return _Paginator(self)

    def _listing(self, bucket: str, prefix: str) -> list[str]:
        keys = {k for k in self._pins.objects if k.startswith(prefix)}
        for set_prefix in self._pins.sets:
            if set_prefix.startswith(prefix) or prefix.startswith(set_prefix):
                paginator = self._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=max(prefix, set_prefix, key=len)):
                    for item in page.get("Contents") or []:
                        if self._set_for(item["Key"]) == set_prefix:
                            keys.add(item["Key"])
        return sorted(keys)

    # -- after the run -------------------------------------------------------
    def set_problems(self) -> list[str]:
        out: list[str] = []
        for prefix, pin in self._pins.sets.items():
            members = self.set_members.get(prefix, [])
            if len(members) != pin.count or set_digest(members) != pin.digest:
                out.append(
                    f"{prefix}*: the recompute read {len(members)} object(s) whose (key, VersionId) set does "
                    f"not digest to the {pin.count} the run recorded — the set changed since, and it is "
                    "recorded as a set, so its members cannot be pinned one by one"
                )
        return out


class _FallbackExceptions:
    NoSuchKey = _NoSuchKey


class _Paginator:
    def __init__(self, owner: PinnedS3) -> None:
        self._owner = owner

    def paginate(self, *, Bucket: str, Prefix: str = "", **_: Any):  # noqa: N803
        keys = self._owner._listing(Bucket, Prefix)
        yield {"Contents": [{"Key": k} for k in keys], "KeyCount": len(keys)}


# ---------------------------------------------------------------------------
# The ArcticDB replay check
# ---------------------------------------------------------------------------


class ArcticVerifier:
    """Stands in for the recorder during a recompute: checks each library read against its pin."""

    def __init__(self, pins: RecordedInputs) -> None:
        self._pins = pins
        self.checked: dict[str, dict[str, Any]] = {}

    def arctic_loaded(
        self,
        library: str,
        frames: Any,
        *,
        end: str,
        lookback_days: int,
        before: str | None,
        as_of: str | None = None,
    ) -> None:
        pin = self._pins.arctic.get(library)
        digest = frames_digest(frames)
        self.checked[library] = {
            "recorded": pin.digest if pin else None,
            "replayed": digest,
            "symbols": len(frames),
            "window": {"end": end, "lookback_days": lookback_days, "before": before},
            "read_as_of": pin.as_of if pin else None,
            "matches": bool(pin) and pin.digest == digest,
        }

    def restatement(self, **_: Any) -> None:
        return None

    def object_read(self, *_: Any, **__: Any) -> None:
        return None

    def problems(self) -> list[str]:
        out = []
        for library in sorted(self._pins.arctic):
            seen = self.checked.get(library)
            if seen is None:
                out.append(f"arcticdb/{library}: the recompute never read the library the run recorded")
            elif not seen["matches"]:
                out.append(
                    f"arcticdb/{library}: the replayed read ({seen['symbols']} symbols) does not digest to what "
                    "the run recorded — the library changed since inside the recorded window"
                )
        return out


def as_of_library_reader(bucket: str, pins: RecordedInputs) -> Callable[..., dict[str, Any]]:
    """A `features.compute._load_price_source` ``library_reader`` that reads AS OF the recorded moment.

    ArcticDB keeps prior symbol versions, so the library as the run saw it is
    still readable after later writes rewrite its history — measured
    2026-09-25, the live `universe` library had rewritten 909 of 912 symbols'
    histories since v1's 09-23 D31 read, and only the ``as_of`` read gave
    v1's frames back. A pin with no ``as_of`` (recorded before it was) reads
    the library as it is now. Either way :class:`ArcticVerifier` checks the
    digest: this finds the read, it does not vouch for it.

    Mirrors ``nousergon_lib.arcticdb._load_arctic_frames`` (one ``read_batch``;
    tz-naive, duplicate dates collapsed keep-last, sorted; failed or empty
    symbols dropped) with ``as_of`` added to each request.
    """
    import pandas as pd

    from nousergon_lib import arcticdb as lib_arctic

    def read(library: str, symbols: Any, *, lookback_days: int, end: Any) -> dict[str, Any]:
        pin = pins.arctic.get(library)
        if pin is None or not pin.as_of:
            if library == "universe":
                return lib_arctic.load_universe_ohlcv(bucket, lookback_days=lookback_days, end=end)
            return lib_arctic.load_macro_series(bucket, symbols, lookback_days=lookback_days, end=end)
        if library == "universe":
            handle = lib_arctic.open_universe_lib(bucket)
            symbols = lib_arctic.get_universe_symbols(bucket) if symbols is None else symbols
        else:
            handle = lib_arctic.open_macro_lib(bucket)
        import arcticdb as adb

        as_of = pd.Timestamp(pin.as_of).to_pydatetime()
        end_ts = pd.Timestamp(end).normalize()
        if end_ts.tz is not None:
            end_ts = end_ts.tz_localize(None)
        window = (end_ts - pd.Timedelta(days=lookback_days), end_ts)
        names = sorted(set(symbols))
        results = handle.read_batch([adb.ReadRequest(symbol=sym, date_range=window, as_of=as_of) for sym in names])
        out: dict[str, Any] = {}
        for sym, res in zip(names, results):
            df = getattr(res, "data", None)
            if df is None or getattr(df, "empty", True):
                continue
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            if df.index.tz is not None:
                df.index = df.index.tz_convert("UTC").tz_localize(None)
            out[sym] = df[~df.index.duplicated(keep="last")].sort_index()
        return out

    return read


# ---------------------------------------------------------------------------
# One side's recompute
# ---------------------------------------------------------------------------


@dataclass
class SideRecompute:
    side: str
    manifest: dict[str, Any] | None
    refusal: str | None = None
    #: group -> sha256 of the parquet bytes the recompute produced.
    groups: dict[str, str] = field(default_factory=dict)
    pins: RecordedInputs | None = None
    served: dict[str, dict[str, Any]] = field(default_factory=dict)
    arctic: dict[str, dict[str, Any]] = field(default_factory=dict)
    unrecorded: list[str] = field(default_factory=list)
    seconds: float = 0.0
    #: The numeric environment the run recorded (`features.numeric_pin`).
    numeric: dict[str, str] | None = None

    @property
    def ok(self) -> bool:
        return self.refusal is None

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest": _manifest_ref(self.manifest),
            "refusal": self.refusal,
            "recomputed_sha256": dict(sorted(self.groups.items())),
            "arcticdb": self.arctic,
            "unrecorded_requests": sorted(set(self.unrecorded))[:25],
            "numeric_env": self.numeric,
            "seconds": round(self.seconds, 1),
        }


def _manifest_ref(manifest: dict[str, Any] | None) -> dict[str, Any] | None:
    if manifest is None:
        return None
    return {
        "unit_id": manifest.get("unit_id"),
        "run_id": manifest.get("run_id"),
        "started": manifest.get("started"),
        "finished": manifest.get("finished"),
        "code_sha": manifest.get("code_sha"),
    }


def _pin_problems(pins: RecordedInputs, trading_day: dt.date, lookback_days: int) -> str | None:
    if pins.empty:
        return (
            "the run's manifest records no inputs. D31 records them from alpha-engine-config-I11203 "
            "on; a run before that cannot be recomputed on the inputs it read"
        )
    if pins.unreadable:
        return f"the run recorded input(s) this recompute cannot read back: {list(pins.unreadable)[:5]}"
    if pins.restated is None:
        return "the run recorded no corporate-action restatement outcome"
    if pins.restated:
        return (
            f"a registered split restated {len(pins.restated)} ticker(s) on the run ({list(pins.restated)[:5]}); "
            "the recompute does not replay a restatement, so it cannot reproduce that run"
        )
    missing = sorted({"universe", "macro"} - set(pins.arctic))
    if missing:
        return f"the run recorded no ArcticDB read of {missing}"
    universe, macro = pins.arctic["universe"], pins.arctic["macro"]
    if (universe.end, universe.lookback_days, universe.before) != (macro.end, macro.lookback_days, macro.before):
        return "the run's two ArcticDB reads recorded different windows"
    if universe.lookback_days != lookback_days:
        return (
            f"the run read ArcticDB with a {universe.lookback_days}-day lookback and this code reads "
            f"{lookback_days}: a code difference in what D31 reads, so it cannot reproduce that run"
        )
    if universe.before != trading_day.isoformat():
        return (
            f"the run read ArcticDB rows before {universe.before!r}, not strictly before the trading day "
            f"{trading_day.isoformat()} as D31 now does: a code difference in what D31 reads"
        )
    return None


def recompute_side(
    side: str,
    manifest: dict[str, Any] | None,
    *,
    trading_day: dt.date,
    bucket: str,
    client: Any,
    physical_prefixes: Iterable[str] = ("",),
    arctic_loader: "Callable[..., Any] | None" = None,
    numeric_env: "Mapping[str, str] | None" = None,
) -> SideRecompute:
    """Re-run D31's feature code over one run's recorded inputs.

    ``arctic_loader(s3, bucket, *, window, recorder)`` replaces the whole
    ArcticDB load (tests); it must report each library to ``recorder``
    exactly as `features.compute._load_price_source` does. By default the
    load is D31's own, reading each library AS OF the moment the run
    recorded (:func:`as_of_library_reader`).

    ``numeric_env`` is this process's numeric environment
    (`features.numeric_pin.flat`), measured when omitted. A run that recorded
    a different one is refused before anything is read.
    """
    import pandas as pd

    from features import compute
    from features.writer import snapshot_group_frames

    started = time.monotonic()
    result = SideRecompute(side, manifest)
    if manifest is None:
        result.refusal = f"no {side} D31 run manifest records this day's feature snapshot"
        return result
    pins = parse_refs(manifest.get("inputs") or [])
    result.pins = pins
    result.numeric = pins.numeric
    why = _pin_problems(pins, trading_day, compute._ARCTICDB_LOOKBACK_DAYS)
    if why is None:
        own = numeric_env if numeric_env is not None else numeric_pin.flat(numeric_pin.effective())
        why = numeric_pin.mismatch(pins.numeric, own)
    if why is not None:
        result.refusal = why
        return result

    s3 = PinnedS3(client, pins, physical_prefixes=physical_prefixes)
    verifier = ArcticVerifier(pins)
    window = pins.arctic["universe"]

    def _price_source(s3_arg: Any, bucket_arg: str) -> Any:
        if arctic_loader is None:
            return compute._load_price_source(
                s3_arg,
                bucket_arg,
                end=pd.Timestamp(window.end),
                before=pd.Timestamp(window.before) if window.before else None,
                recorder=verifier,
                library_reader=as_of_library_reader(bucket_arg, pins),
            )
        return arctic_loader(s3_arg, bucket_arg, window=window, recorder=verifier)

    try:
        build = compute.build_feature_frame(
            trading_day.isoformat(),
            bucket,
            s3=s3,
            registry_client=None,
            recorder=verifier,
            exclude_trading_day_arctic_rows=True,
            price_source_loader=_price_source,
        )
    except ReadOnlyViolation as exc:
        result.refusal = f"the feature code attempted a non-read S3 call: {exc}"
        return result
    except Exception as exc:  # noqa: BLE001 - a recompute that cannot finish proves nothing; named
        result.refusal = f"the recompute raised {type(exc).__name__}: {exc}"
        return result
    finally:
        result.seconds = time.monotonic() - started
    result.served = dict(s3.served)
    result.arctic = dict(verifier.checked)
    result.unrecorded = list(s3.unrecorded)
    problems = s3.problems + s3.set_problems() + verifier.problems()
    if build.features_df is None:
        problems.append("the recompute loaded no price data")
    if problems:
        result.refusal = "; ".join(problems[:5])
        return result
    result.groups = {
        group: _sha256(body)
        for group, (_frame, body) in snapshot_group_frames(trading_day.isoformat(), build.features_df).items()
    }
    result.seconds = time.monotonic() - started
    return result


# ---------------------------------------------------------------------------
# The day's record
# ---------------------------------------------------------------------------


def _output_version(manifest: dict[str, Any] | None, key: str) -> dict[str, Any] | None:
    for out in (manifest or {}).get("outputs") or []:
        if str(out.get("key") or "") == key:
            return out
    return None


def _differing_inputs(v1: SideRecompute, shadow: SideRecompute) -> list[dict[str, Any]]:
    """The recorded inputs whose CONTENT differs between the two runs (``Δ``)."""
    assert v1.pins is not None and shadow.pins is not None
    out: list[dict[str, Any]] = []
    for key in sorted(set(v1.pins.objects) | set(shadow.pins.objects)):
        a, b = v1.pins.objects.get(key), shadow.pins.objects.get(key)
        if a is None or b is None:
            out.append({"kind": "object", "key": key, "only": "v1" if b is None else "shadow"})
            continue
        sa, sb = v1.served.get(key), shadow.served.get(key)
        if sa is not None and sb is not None:
            if sa["sha256"] == sb["sha256"]:
                continue
        elif a.version_id == b.version_id and a.version_id:
            continue
        out.append(
            {
                "kind": "object",
                "key": key,
                "v1": sa or {"physical_key": key, "version_id": a.version_id, "sha256": None},
                "shadow": sb or {"physical_key": None, "version_id": b.version_id, "sha256": None},
            }
        )
    for prefix in sorted(set(v1.pins.sets) | set(shadow.pins.sets)):
        a, b = v1.pins.sets.get(prefix), shadow.pins.sets.get(prefix)
        if a is None or b is None or a.digest != b.digest:
            out.append({"kind": "set", "key": f"{prefix}*"})
    for library in sorted(set(v1.pins.arctic) | set(shadow.pins.arctic)):
        a, b = v1.pins.arctic.get(library), shadow.pins.arctic.get(library)
        if a is None or b is None or a.digest != b.digest:
            out.append({"kind": "arcticdb", "key": f"arcticdb/{library}"})
    return out


def build_record(
    trading_day: dt.date,
    *,
    v1_manifest: dict[str, Any] | None,
    shadow_manifest: dict[str, Any] | None,
    v1: SideRecompute,
    shadow: SideRecompute,
    published: Callable[[str, str, dict[str, Any] | None], "bytes | None"],
    code_sha: str,
    shadow_prefix: str,
) -> dict[str, Any]:
    """Assemble the day's record. ``published(side, key, output_record)`` returns a published file's bytes."""
    keys: dict[str, Any] = {}
    groups = sorted(set(v1.groups) | set(shadow.groups)) or []
    candidate_keys = {
        str(out.get("key") or "")
        for manifest in (v1_manifest, shadow_manifest)
        for out in (manifest or {}).get("outputs") or []
        if is_lineage_key(str(out.get("key") or ""))
    }
    candidate_keys |= {f"features/{trading_day.isoformat()}/{g}.parquet" for g in groups}
    for key in sorted(candidate_keys):
        group = FEATURE_KEY_RE.match(key).group(2)  # type: ignore[union-attr]
        entry: dict[str, Any] = {}
        for side, recompute, manifest in (("v1", v1, v1_manifest), ("shadow", shadow, shadow_manifest)):
            out = _output_version(manifest, key)
            body = published(side, key, out) if out is not None else None
            published_sha = _sha256(body) if body is not None else None
            recomputed = recompute.groups.get(group)
            entry[side] = {
                "published_version_id": (out or {}).get("version_id"),
                "published_sha256": published_sha,
                "recomputed_sha256": recomputed,
                "reproduced": published_sha is not None and recomputed == published_sha,
            }
        keys[key] = entry
    complete = v1.ok and shadow.ok
    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "trading_day": trading_day.isoformat(),
        "unit_id": LINEAGE_UNIT,
        "generated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "code_sha": code_sha,
        "recompute_host": host_fingerprint(),
        "shadow_prefix": shadow_prefix,
        "status": "complete" if complete else "refused",
        "sides": {"v1": v1.as_dict(), "shadow": shadow.as_dict()},
        "keys": keys,
        "differing_inputs": _differing_inputs(v1, shadow) if complete else [],
        "note": (
            "Recompute lineage (alpha-engine-config-I11203). `reproduced` on BOTH sides means this "
            "code maps each run's recorded inputs to that run's published bytes, so the two files "
            "differ only through `differing_inputs`. shadow.parity grants v1_cause on a key only if "
            "every differing input itself grades v1_cause; this record proves nothing on its own."
        ),
    }
    return record


def _manifests(reader: Any, prefix: str) -> list[dict[str, Any]]:
    out = []
    for key in sorted(k for k in reader.list(prefix, 500) if k.endswith(".json")):
        body = reader.get(key)
        if body is None:
            continue
        try:
            manifest = json.loads(body)
        except ValueError:
            continue
        if isinstance(manifest, dict):
            out.append(manifest)
    return out


def _recording(manifests: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    best = None
    for manifest in manifests:
        if _output_version(manifest, key) is None:
            continue
        if best is None or str(manifest.get("finished") or "") >= str(best.get("finished") or ""):
            best = manifest
    return best


def evaluate_day(
    trading_day: dt.date,
    *,
    bucket: str,
    client: Any,
    code_sha: str,
    reader: Any = None,
    arctic_loader: "Callable[..., Any] | None" = None,
) -> dict[str, Any]:
    """Recompute both sides of one trading day and return its record."""
    from shadow.parity import S3Reader
    from shadow.root import ShadowRoot

    reader = reader or S3Reader(bucket, client)
    root = ShadowRoot(trading_day)
    own_numeric = numeric_pin.flat(numeric_pin.effective())
    anchor = f"features/{trading_day.isoformat()}/technical.parquet"
    v1_manifest = _recording(_manifests(reader, f"{D31_MANIFEST_PREFIX}/{trading_day.isoformat()}/"), anchor)
    shadow_manifest = _recording(
        _manifests(reader, root.key(f"{D31_MANIFEST_PREFIX}/{trading_day.isoformat()}/")), anchor
    )
    v1 = recompute_side(
        "v1",
        v1_manifest,
        trading_day=trading_day,
        bucket=bucket,
        client=client,
        arctic_loader=arctic_loader,
        numeric_env=own_numeric,
    )
    shadow = recompute_side(
        "shadow",
        shadow_manifest,
        trading_day=trading_day,
        bucket=bucket,
        client=client,
        physical_prefixes=(root.prefix, ""),
        arctic_loader=arctic_loader,
        numeric_env=own_numeric,
    )

    def _published(side: str, key: str, out: dict[str, Any] | None) -> bytes | None:
        physical = key if side == "v1" else root.key(key)
        meta = reader.get_with_meta(physical, version_id=(out or {}).get("version_id") or None)
        return None if meta is None else meta["body"]

    return build_record(
        trading_day,
        v1_manifest=v1_manifest,
        shadow_manifest=shadow_manifest,
        v1=v1,
        shadow=shadow,
        published=_published,
        code_sha=code_sha,
        shadow_prefix=root.prefix,
    )

"""What D31 read, recorded precisely enough to read it again.

`alpha-engine-config-I11203` (recompute lineage). `features/{D}/*.parquet` is a
DERIVED artifact: D31 computes it from ArcticDB price history, the
`staging/daily_closes/` delta and a handful of reference objects. When the v1
and shadow copies of a derived key differ, the only way to prove the difference
is caused on the v1 side is to re-run the feature code over the inputs v1
ACTUALLY read and get v1's published bytes back (`shadow.recompute_lineage`).
That needs v1 to have recorded those inputs. Until this module, D31's run
manifest carried `inputs: []` — nothing to pin a re-run to.

Three kinds of input, each recorded in the manifest's closed `InputRef` shape
(`key`, `etag`, `version`, `schema_version`, nothing else):

* **An S3 object read through the run's client** — ``s3://<bucket>/<key>``, its
  ETag, and its S3 VersionId: the exact bytes, re-readable for the bucket's
  30-day noncurrent retention. When one directory contributes more than
  :data:`AGGREGATE_MIN_MEMBERS` objects (the per-ticker alternative partition,
  ~900 keys) they are folded into ONE entry, ``s3://<bucket>/<prefix>*`` with
  ``version = "set-sha256:<digest>:<count>"`` over the sorted
  ``key<TAB>VersionId`` lines, so the manifest stays small. A reader can check
  a set it re-reads against that digest but cannot pin its members one by one.
* **An ArcticDB library read** — ``arcticdb://<bucket>/<library>?end=..&
  lookback_days=..[&before=..][&as_of=..]`` with ``version = "sha256:<digest>"``
  over the frames exactly as loaded (:func:`frames_digest`). ``as_of`` is the
  UTC moment just before the read. ArcticDB keeps prior symbol versions
  (measured 2026-09-25: 253 retained versions of `universe/AAPL`, oldest
  2026-04-07), so a later reader asks for the library AS OF that moment and
  checks what it got against the digest. The moment is how the read is
  FOUND; the digest is what PROVES it — a write racing the original read, a
  pruned version or a symbol deleted since all fail the digest, never pass it.
  A per-symbol version map (~900 entries) would not fit the manifest; the pair
  does.
* **Corporate-action restatement** — ``corporate-actions://<bucket>/restated?
  from=..&to=..`` with ``version = "restated:<tickers>"`` (empty when nothing
  was restated). Split restatement is the one step whose inputs are an external
  feed plus registry state, so what is recorded is its OUTCOME for this run.

One more entry is not a data input but decides the bytes all the same: the
**numeric environment** the run computed under, ``numeric-env://process?..``
with ``version = "numeric-pin:<policy>:<pinned|unpinned>:<digest>"``
(`features.numeric_pin`) — the CPU, numpy's SIMD dispatch actually in effect,
the BLAS kernel and threads, and the pin. It is added when the record is
frozen, measured then rather than asserted.

Reads the recorder does not see — the corporate-action registry's own reads,
and anything after the recording is frozen (`features.metron_supplemental`,
which writes no key the lineage covers) — are deliberately outside it. A
re-run proves the record is complete enough by reproducing the published bytes;
nothing here claims completeness on its own.
"""

from __future__ import annotations

import hashlib
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit

from features import numeric_pin

#: A directory contributing more objects than this is recorded as one set
#: entry rather than one entry per object.
AGGREGATE_MIN_MEMBERS = 50

S3_SCHEME = "s3://"
ARCTIC_SCHEME = "arcticdb://"
RESTATED_SCHEME = "corporate-actions://"

SET_VERSION_PREFIX = "set-sha256:"
DIGEST_VERSION_PREFIX = "sha256:"
RESTATED_VERSION_PREFIX = "restated:"


def frames_digest(frames: Mapping[str, Any]) -> str:
    """A content digest over ``symbol -> DataFrame``, order-independent in the symbols.

    Covers each frame's symbol, column names, dtypes, index and values
    (`pandas.util.hash_pandas_object`, index included). Two loads digest equal
    exactly when they would hand the feature code the same frames.
    """
    import pandas as pd

    h = hashlib.sha256()
    for symbol in sorted(frames):
        df = frames[symbol]
        h.update(b"\x00sym\x00" + str(symbol).encode())
        h.update(b"\x00cols\x00" + repr([str(c) for c in df.columns]).encode())
        h.update(b"\x00dtypes\x00" + repr([str(t) for t in df.dtypes]).encode())
        h.update(pd.util.hash_pandas_object(df, index=True).to_numpy().tobytes())
    return h.hexdigest()


def set_digest(members: Iterable[tuple[str, str | None]]) -> str:
    """Digest of a set of ``(key, version_id)`` pairs, order-independent."""
    lines = sorted(f"{key}\t{version or ''}" for key, version in members)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def arctic_key(
    bucket: str,
    library: str,
    *,
    end: str,
    lookback_days: int,
    before: str | None,
    as_of: str | None = None,
) -> str:
    query: dict[str, str] = {"end": end, "lookback_days": str(int(lookback_days))}
    if before is not None:
        query["before"] = before
    if as_of is not None:
        query["as_of"] = as_of
    return f"{ARCTIC_SCHEME}{bucket}/{library}?{urlencode(query)}"


def restated_key(bucket: str, *, start: str, end: str) -> str:
    return f"{RESTATED_SCHEME}{bucket}/restated?{urlencode({'from': start, 'to': end})}"


class InputRecorder:
    """Accumulates one run's reads; :meth:`refs` renders them as manifest `InputRef`s."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self._lock = threading.Lock()
        #: (bucket, key) -> (version_id, etag), first read wins: a run that
        #: reads a key twice read it at the first version.
        self._objects: dict[tuple[str, str], tuple[str | None, str | None]] = {}
        self._arctic: list[dict[str, Any]] = []
        self._restated: dict[str, Any] | None = None
        self._frozen: list[dict[str, Any]] | None = None

    # -- recording --------------------------------------------------------
    def object_read(self, bucket: str, key: str, version_id: Any, etag: Any) -> None:
        if self._frozen is not None:
            return
        version = version_id if isinstance(version_id, str) and version_id and version_id != "null" else None
        tag = etag.strip('"') if isinstance(etag, str) and etag else None
        with self._lock:
            self._objects.setdefault((bucket, key), (version, tag))

    def arctic_loaded(
        self,
        library: str,
        frames: Mapping[str, Any],
        *,
        end: str,
        lookback_days: int,
        before: str | None,
        as_of: str | None = None,
    ) -> None:
        if self._frozen is not None:
            return
        self._arctic.append(
            {
                "key": arctic_key(
                    self.bucket, library, end=end, lookback_days=lookback_days, before=before, as_of=as_of
                ),
                "etag": None,
                "version": DIGEST_VERSION_PREFIX + frames_digest(frames),
                "schema_version": None,
            }
        )

    def restatement(self, *, start: str, end: str, restated_tickers: Iterable[str]) -> None:
        if self._frozen is not None:
            return
        self._restated = {
            "key": restated_key(self.bucket, start=start, end=end),
            "etag": None,
            "version": RESTATED_VERSION_PREFIX + ",".join(sorted(set(restated_tickers))),
            "schema_version": None,
        }

    def wrap(self, client: Any) -> "RecordingS3":
        return RecordingS3(client, self)

    # -- rendering --------------------------------------------------------
    def freeze(self) -> list[dict[str, Any]]:
        """Render the refs and stop recording: later reads are not this run's feature inputs.

        The frozen record also carries the numeric environment the features
        were computed under (`features.numeric_pin`), measured now — after the
        compute, in the process that ran it.
        """
        if self._frozen is None:
            self._frozen = self._render() + [numeric_pin.as_input_ref(numeric_pin.effective())]
        return list(self._frozen)

    def refs(self) -> list[dict[str, Any]]:
        return list(self._frozen) if self._frozen is not None else self._render()

    def _render(self) -> list[dict[str, Any]]:
        with self._lock:
            objects = dict(self._objects)
        by_dir: dict[tuple[str, str], list[tuple[str, str | None, str | None]]] = defaultdict(list)
        for (bucket, key), (version, etag) in objects.items():
            directory = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
            by_dir[(bucket, directory)].append((key, version, etag))
        refs: list[dict[str, Any]] = []
        for (bucket, directory), members in sorted(by_dir.items()):
            if len(members) > AGGREGATE_MIN_MEMBERS:
                digest = set_digest((key, version) for key, version, _ in members)
                refs.append(
                    {
                        "key": f"{S3_SCHEME}{bucket}/{directory}*",
                        "etag": None,
                        "version": f"{SET_VERSION_PREFIX}{digest}:{len(members)}",
                        "schema_version": None,
                    }
                )
                continue
            for key, version, etag in sorted(members):
                refs.append(
                    {"key": f"{S3_SCHEME}{bucket}/{key}", "etag": etag, "version": version, "schema_version": None}
                )
        refs.extend(self._arctic)
        if self._restated is not None:
            refs.append(self._restated)
        return refs


class RecordingS3:
    """A boto3 S3 client that records every object it successfully GETs.

    Everything else delegates untouched (`exceptions`, paginators, writes), so
    the feature code cannot tell it from the client it wraps.
    """

    def __init__(self, inner: Any, recorder: InputRecorder) -> None:
        self._inner = inner
        self._recorder = recorder

    def get_object(self, **kwargs: Any) -> Any:
        response = self._inner.get_object(**kwargs)
        self._recorder.object_read(
            str(kwargs.get("Bucket") or ""),
            str(kwargs.get("Key") or ""),
            response.get("VersionId"),
            response.get("ETag"),
        )
        return response

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# Reading a manifest's `inputs` back.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObjectPin:
    bucket: str
    key: str
    version_id: str | None
    etag: str | None


@dataclass(frozen=True)
class SetPin:
    bucket: str
    prefix: str
    digest: str
    count: int


@dataclass(frozen=True)
class ArcticPin:
    bucket: str
    library: str
    end: str
    lookback_days: int
    before: str | None
    digest: str
    #: The UTC moment the run read the library (absent on a legacy record).
    as_of: str | None = None


@dataclass(frozen=True)
class RecordedInputs:
    """One run's recorded inputs, parsed. ``unreadable`` names every ref not understood."""

    objects: dict[str, ObjectPin] = field(default_factory=dict)
    sets: dict[str, SetPin] = field(default_factory=dict)
    arctic: dict[str, ArcticPin] = field(default_factory=dict)
    restated: tuple[str, ...] | None = None
    restated_window: tuple[str, str] | None = None
    unreadable: tuple[str, ...] = ()
    #: The numeric environment the run recorded (`features.numeric_pin.flat`
    #: fields), or None when it recorded none (a run before the pin).
    numeric: dict[str, str] | None = None

    @property
    def empty(self) -> bool:
        return not (self.objects or self.sets or self.arctic) and self.restated is None


def _split_bucket(rest: str) -> tuple[str, str]:
    bucket, _, key = rest.partition("/")
    return bucket, key


def parse_refs(refs: Iterable[Mapping[str, Any]]) -> RecordedInputs:
    """Parse a manifest's `inputs` list. Unknown shapes are named, never guessed at."""
    objects: dict[str, ObjectPin] = {}
    sets: dict[str, SetPin] = {}
    arctic: dict[str, ArcticPin] = {}
    restated: tuple[str, ...] | None = None
    window: tuple[str, str] | None = None
    numeric: dict[str, str] | None = None
    unreadable: list[str] = []
    for ref in refs:
        key = str(ref.get("key") or "")
        # `version_id` is the spelling `shadow.pinned_inputs.Pin.as_input_record`
        # writes; `version` the one `UnitRun.record_input` writes. Both mean the
        # S3 VersionId.
        version = ref.get("version") or ref.get("version_id")
        version = str(version) if version else None
        if key.startswith(S3_SCHEME):
            bucket, path = _split_bucket(key[len(S3_SCHEME):])
            if path.endswith("*"):
                if not (version and version.startswith(SET_VERSION_PREFIX)):
                    unreadable.append(key)
                    continue
                digest, _, count = version[len(SET_VERSION_PREFIX):].partition(":")
                try:
                    sets[path[:-1]] = SetPin(bucket, path[:-1], digest, int(count))
                except ValueError:
                    unreadable.append(key)
                continue
            etag = ref.get("etag")
            objects[path] = ObjectPin(bucket, path, version, str(etag).strip('"') if etag else None)
        elif key.startswith(ARCTIC_SCHEME):
            parts = urlsplit(key)
            query = dict(parse_qsl(parts.query))
            if not (version and version.startswith(DIGEST_VERSION_PREFIX)) or "end" not in query:
                unreadable.append(key)
                continue
            try:
                lookback = int(query["lookback_days"])
            except (KeyError, ValueError):
                unreadable.append(key)
                continue
            library = parts.path.lstrip("/")
            arctic[library] = ArcticPin(
                parts.netloc,
                library,
                query["end"],
                lookback,
                query.get("before"),
                version[len(DIGEST_VERSION_PREFIX):],
                query.get("as_of"),
            )
        elif key.startswith(RESTATED_SCHEME):
            query = dict(parse_qsl(urlsplit(key).query))
            if not (version and version.startswith(RESTATED_VERSION_PREFIX)):
                unreadable.append(key)
                continue
            body = version[len(RESTATED_VERSION_PREFIX):]
            restated = tuple(t for t in body.split(",") if t)
            window = (query.get("from", ""), query.get("to", ""))
        elif key.startswith(numeric_pin.INPUT_SCHEME):
            if not (version and version.startswith(numeric_pin.INPUT_VERSION_PREFIX)):
                unreadable.append(key)
                continue
            numeric = numeric_pin.from_input_ref(key)
        else:
            unreadable.append(key)
    return RecordedInputs(objects, sets, arctic, restated, window, tuple(unreadable), numeric)

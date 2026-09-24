"""The S3 redirect, installed once at the botocore client boundary.

**Why here and not at the call sites.** This repository has ~45 ``put_object``
call sites across ``collectors/``, ``builders/``, ``features/``, ``data/`` and
``rag/``, most of them constructing their own ``boto3.client("s3")``. Editing
each one would be forty-five chances to miss one, and "we edited all of them"
is not a property anyone can check — the next collector merged would reopen
the hole. Patching ``botocore.client.BaseClient._make_api_call`` is one edit
whose totality is a *structural* fact: every boto3 S3 call, from any client,
any session, any library in the process (including ``nousergon_lib`` and
``krepis``), goes through that method. There is no second path.

**The one surface this cannot see** is ArcticDB, which ships its own C++ S3
client. That is redirected by library name instead — see ``shadow.root``.

**Classification is closed, and the default is RAISE.** Mutating operations
have their key rewritten and then *asserted*. Anything in neither list raises,
because the alternative — passing an unclassified operation through — is
exactly how a live key gets written by a run everybody believed was a shadow.
Adding a new S3 operation to this file is a deliberate act with a reviewer.

**Reads split into INPUT and RUN STATE** (`alpha-engine-config-I10891`). A
shadow run must read the same live *inputs* the real run reads — that is what
makes its output comparable — but it must read its OWN *run state*: its
completion markers, its auto-skip artifact probes, and the prior value of any
key it read-modify-writes. The first complete shadow run (trading day
2026-09-14) read v1's live ``data/2026-09-14/.phases/*`` markers, concluded
twelve units (D19, D20, D22–D31) were already done, skipped them, and reported
``ok`` with nothing published: 123 parity rows measured nothing.

The split is decided HERE, for every client, in this order
(:func:`classify_read`):

1. **own write** — a key this process already wrote under the root reads back
   from the shadow (read-your-writes; e.g. the empty-fresh guard and the
   verify-by-artifact probe grading a key the run just published).
2. **own-state scope** — a read made inside :func:`own_state_reads` (the phase
   registry's auto-skip decision, ``shadow.run_state``) reads the shadow.
3. **own-state key** — a key matching :data:`OWN_STATE_KEY_PATTERNS` reads the
   shadow wherever it is read from.
4. **guard baseline** — a read made inside :func:`guard_baseline_reads` reads
   LIVE and is NOT recorded as an input (`alpha-engine-config-I11547`). See
   that function for the one contract a caller takes on by entering it.
5. **input** — everything else reads live, and is recorded.

**An unclassified read of a key the run also writes RAISES.** A keyed write to
a key this process previously read as a live *input* means the run consumed
v1's copy of its own output as the base of what it publishes. That is refused
at the write — before anything is sent — naming the key, so the fix is a row in
:data:`OWN_STATE_KEY_PATTERNS` (or a scope), never a silent live base.
"""

from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from typing import Any

from shadow.root import ShadowGuardViolation, active_root

__all__ = [
    "OWN_STATE_KEY_PATTERNS",
    "RunLedger",
    "classify_read",
    "guard_baseline_reads",
    "install",
    "installed",
    "own_state_reads",
    "uninstall",
]

#: S3 operations that only read. Passed through with no rewrite: the shadow run
#: reads live inputs on purpose — that is what makes its output comparable.
READ_OPERATIONS: frozenset[str] = frozenset(
    {
        "GetBucketLocation",
        "GetBucketVersioning",
        "GetObject",
        "GetObjectAcl",
        "GetObjectAttributes",
        "GetObjectTagging",
        "HeadBucket",
        "HeadObject",
        "ListBuckets",
        "ListMultipartUploads",
        "ListObjectVersions",
        "ListObjects",
        "ListObjectsV2",
        "ListParts",
        "SelectObjectContent",
    }
)

#: The read operations that address ONE object by a top-level ``Key``. These are
#: the reads the input/run-state split applies to; the remaining reads
#: (listings, bucket metadata) carry no single key.
KEYED_READ_OPERATIONS: frozenset[str] = frozenset(
    {
        "GetObject",
        "GetObjectAcl",
        "GetObjectAttributes",
        "GetObjectTagging",
        "HeadObject",
        "ListParts",
        "SelectObjectContent",
    }
)

#: Listing reads. Live, EXCEPT inside :func:`own_state_reads`, where a listing
#: raises: its response keys would be shadow-prefixed and no run-state caller
#: here lists, so a listing there is a new caller nobody classified.
LISTING_READ_OPERATIONS: frozenset[str] = frozenset(
    {"ListObjectVersions", "ListObjects", "ListObjectsV2", "ListMultipartUploads"}
)

#: Keys that are a run's OWN STATE wherever they are read from. Each entry is
#: ``(name, pattern over the live key, why)``. The whole rule for keys lives in
#: this table; ``tests/test_shadow_own_state.py`` asserts each row.
OWN_STATE_KEY_PATTERNS: tuple[tuple[str, "re.Pattern[str]", str], ...] = (
    (
        "phase_marker",
        re.compile(r"^data/[^/]+/\.phases/[^/]+\.json$"),
        "PhaseRegistry completion markers (weekly_collector._build_registry, "
        "marker_prefix='data'): the run's idempotency ledger. Reading v1's made the "
        "2026-09-14 shadow run auto-skip D19/D20/D22-D31 (alpha-engine-config-I10891).",
    ),
    (
        "daily_closes_merge_base",
        re.compile(r"^staging/daily_closes/[^/]+\.parquet$"),
        "read-modify-write base of collectors/daily_closes.py (source-priority coalesce "
        "and the `revision` bump): the run publishes this key, so its prior value is "
        "run state. Across the shadow-weekday legs it is D17's shadow output "
        "(alpha-engine-config-I10891, I10894).",
    ),
)


class RunLedger:
    """Which keys this process wrote under the root, and which it read as input.

    Keyed ``(bucket, live_key)``. Thread-safe: collectors fan out over thread
    pools and every client funnels through one ``_make_api_call``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._own_writes: set[tuple[str, str]] = set()
        self._input_reads: set[tuple[str, str]] = set()
        self._baseline_reads: set[tuple[str, str]] = set()

    def wrote(self, bucket: str, key: str) -> bool:
        with self._lock:
            return (bucket, key) in self._own_writes

    def record_write(self, bucket: str, key: str) -> None:
        with self._lock:
            if (bucket, key) in self._input_reads:
                raise ShadowGuardViolation(
                    f"unclassified read of a key the run also writes: s3://{bucket}/{key} was "
                    "read LIVE as an input earlier in this shadow run and is now being "
                    "published by it, so the published value would be built on v1's copy of "
                    "this run's own output. Classify the key as run state in "
                    "shadow/interceptor.py::OWN_STATE_KEY_PATTERNS (or read it inside "
                    "own_state_reads()) — alpha-engine-config-I10891."
                )
            self._own_writes.add((bucket, key))

    def record_input_read(self, bucket: str, key: str) -> None:
        with self._lock:
            self._input_reads.add((bucket, key))

    def record_baseline_read(self, bucket: str, key: str) -> None:
        """A live read made only to DECIDE whether to publish (never an input)."""
        with self._lock:
            self._baseline_reads.add((bucket, key))

    def baseline_reads(self) -> frozenset[tuple[str, str]]:
        with self._lock:
            return frozenset(self._baseline_reads)


_LEDGER = RunLedger()
_SCOPE = threading.local()
_BASELINE_SCOPE = threading.local()


@contextmanager
def own_state_reads():
    """Every keyed read inside this block is RUN STATE: it resolves to the shadow.

    Outside an active shadow root the interceptor is not consulted at all, so
    this is the identity on the production path.
    """
    depth = getattr(_SCOPE, "depth", 0)
    _SCOPE.depth = depth + 1
    try:
        yield
    finally:
        _SCOPE.depth = depth


def _in_own_state_scope() -> bool:
    return getattr(_SCOPE, "depth", 0) > 0


@contextmanager
def guard_baseline_reads():
    """Keyed reads inside this block are a write guard's BASELINE: live, unrecorded.

    `alpha-engine-config-I11547`. The price cache's write guards
    (``collectors/prices.py``: the short-fetch guard, the behind-fetch guard,
    the split guard) read the key they are about to overwrite, to decide
    WHETHER the fresh fetch may replace it. Recorded as an input, that read
    made the upload of the same key raise the read-then-write violation above
    — so every same-day shadow run refused FDXF, HONA, Q and SOLS (the only
    tickers under the 400-row threshold that makes the short-fetch guard read
    at all), while v1, which has no interceptor, wrote them ``ok``.

    Reading it as run state instead (:func:`own_state_reads`) would read the
    shadow's own copy, which on a fresh daily root does not exist — the guard
    would see "no history to regress" on every ticker and could never refuse
    in a shadow run, so the rehearsal would stop exercising the guard that
    production runs. A baseline read therefore stays LIVE, the object the
    production guard compares against, and is simply not an input.

    **The contract a caller takes on by entering this block:** the bytes it
    reads may decide whether the run publishes, and must never become part of
    WHAT it publishes. A value read here and then written back (a merge base,
    a read-modify-write) is run state and belongs in
    :data:`OWN_STATE_KEY_PATTERNS`, not here. Run state still wins inside this
    block: a key the run already wrote, or an own-state key, reads the shadow.

    Outside an active shadow root the interceptor is not consulted at all, so
    this is the identity on the production path.
    """
    depth = getattr(_BASELINE_SCOPE, "depth", 0)
    _BASELINE_SCOPE.depth = depth + 1
    try:
        yield
    finally:
        _BASELINE_SCOPE.depth = depth


def _in_guard_baseline_scope() -> bool:
    return getattr(_BASELINE_SCOPE, "depth", 0) > 0


def classify_read(key: str, *, bucket: str, ledger: RunLedger) -> str:
    """``own_write`` | ``own_state_scope`` | ``own_state:<name>`` |
    ``guard_baseline`` | ``input``."""
    if ledger.wrote(bucket, key):
        return "own_write"
    if _in_own_state_scope():
        return "own_state_scope"
    for name, pattern, _why in OWN_STATE_KEY_PATTERNS:
        if pattern.match(key):
            return f"own_state:{name}"
    if _in_guard_baseline_scope():
        return "guard_baseline"
    return "input"


def _live_key(key: Any, root) -> str:
    if not isinstance(key, str):
        raise ShadowGuardViolation(f"refusing to classify a non-string S3 key: {key!r}")
    cleaned = key.lstrip("/")
    return root.live_key(cleaned) if cleaned.startswith(root.prefix) else cleaned


#: Mutating operations whose target object is a single top-level ``Key``.
#: ``CopyObject``/``UploadPartCopy`` appear here because their ``Key`` is the
#: DESTINATION; their ``CopySource`` is a read and is deliberately left alone.
KEYED_WRITE_OPERATIONS: frozenset[str] = frozenset(
    {
        "AbortMultipartUpload",
        "CompleteMultipartUpload",
        "CopyObject",
        "CreateMultipartUpload",
        "DeleteObject",
        "DeleteObjectTagging",
        "PutObject",
        "PutObjectAcl",
        "PutObjectTagging",
        "RestoreObject",
        "UploadPart",
        "UploadPartCopy",
    }
)

#: The one mutating operation that carries many keys in a nested structure.
BULK_DELETE_OPERATION = "DeleteObjects"

#: Non-S3 calls that would reach the outside world from inside a shadow run.
#: A shadow run is a rehearsal; it must not page Brian, email anyone, or
#: enqueue work for a live consumer. Raising is right rather than silently
#: dropping: a collector that alerts is a collector whose alert path is part of
#: what we are grading, and the shadow harness should not pretend it ran.
OUTBOUND_OPERATIONS: dict[str, frozenset[str]] = {
    "sns": frozenset({"Publish", "PublishBatch"}),
    "ses": frozenset({"SendEmail", "SendRawEmail", "SendTemplatedEmail"}),
    "sesv2": frozenset({"SendEmail", "SendBulkEmail"}),
    "sqs": frozenset({"SendMessage", "SendMessageBatch"}),
}

_LOCK = threading.Lock()
_ORIGINAL: Any = None


def installed() -> bool:
    return _ORIGINAL is not None


def install() -> None:
    """Patch ``BaseClient._make_api_call``. Idempotent."""
    global _ORIGINAL
    import botocore.client

    global _LEDGER
    with _LOCK:
        if _ORIGINAL is not None:
            return
        _LEDGER = RunLedger()
        _ORIGINAL = botocore.client.BaseClient._make_api_call
        botocore.client.BaseClient._make_api_call = _shadow_make_api_call  # type: ignore[method-assign]


def uninstall() -> None:
    """Restore the original. Idempotent; used by tests and the runner teardown."""
    global _ORIGINAL
    import botocore.client

    with _LOCK:
        if _ORIGINAL is None:
            return
        botocore.client.BaseClient._make_api_call = _ORIGINAL  # type: ignore[method-assign]
        _ORIGINAL = None


def _service_name(client: Any) -> str:
    try:
        return str(client.meta.service_model.service_name)
    except AttributeError:  # pragma: no cover - a client shape botocore does not produce
        raise ShadowGuardViolation(
            "could not determine the service of an AWS client under an active shadow root; "
            "refusing the call rather than letting an unclassified write through"
        ) from None


def rewrite_params(
    operation_name: str,
    api_params: dict,
    *,
    service: str,
    root,
    ledger: "RunLedger | None" = None,
) -> dict:
    """The whole policy, as a pure function so a test can grade it directly.

    Returns the parameters to call with. Raises ``ShadowGuardViolation`` for
    anything it cannot place in the shadow. ``ledger`` defaults to this
    process's ledger (reset on every install).
    """
    if ledger is None:
        ledger = _LEDGER
    if service in OUTBOUND_OPERATIONS and operation_name in OUTBOUND_OPERATIONS[service]:
        raise ShadowGuardViolation(
            f"{service}:{operation_name} is an outbound call; a shadow run may not notify, "
            "email or enqueue (plan §6.2 step 4). Refusing rather than sending."
        )
    if service != "s3":
        return api_params

    bucket = api_params.get("Bucket")
    if operation_name in KEYED_READ_OPERATIONS:
        live_key = _live_key(api_params.get("Key", ""), root)
        classification = classify_read(live_key, bucket=bucket, ledger=ledger)
        if classification == "input":
            ledger.record_input_read(bucket, live_key)
            return api_params
        if classification == "guard_baseline":
            ledger.record_baseline_read(bucket, live_key)
            return api_params
        params = dict(api_params)
        params["Key"] = root.key(live_key)
        return params
    if operation_name in LISTING_READ_OPERATIONS:
        if _in_own_state_scope():
            raise ShadowGuardViolation(
                f"s3:{operation_name} inside own_state_reads() is not classified: a run-state "
                "listing would return shadow-prefixed keys to a caller written for live ones. "
                "Classify it in shadow/interceptor.py rather than passing it through."
            )
        return api_params
    if operation_name in READ_OPERATIONS:
        return api_params

    if operation_name in KEYED_WRITE_OPERATIONS:
        params = dict(api_params)
        params["Key"] = root.key(api_params.get("Key", ""))
        root.assert_shadow_key(params["Key"], operation=f"s3:{operation_name}", bucket=bucket)
        ledger.record_write(bucket, root.live_key(params["Key"]))
        return params
    if operation_name == BULK_DELETE_OPERATION:
        params = dict(api_params)
        delete = dict(params.get("Delete") or {})
        objects = []
        for entry in delete.get("Objects") or []:
            item = dict(entry)
            item["Key"] = root.key(item.get("Key", ""))
            root.assert_shadow_key(
                item["Key"], operation=f"s3:{operation_name}", bucket=bucket
            )
            ledger.record_write(bucket, root.live_key(item["Key"]))
            objects.append(item)
        delete["Objects"] = objects
        params["Delete"] = delete
        return params

    raise ShadowGuardViolation(
        f"s3:{operation_name} is not classified as a read or a keyed write in "
        "shadow/interceptor.py. Under an active shadow root an unclassified operation is "
        "refused, never passed through — classify it (and say which list it belongs in) "
        "rather than widening the default."
    )


def _shadow_make_api_call(self, operation_name: str, api_params: dict):  # noqa: ANN001
    root = active_root()
    if root is None:
        return _ORIGINAL(self, operation_name, api_params)
    params = rewrite_params(
        operation_name, api_params, service=_service_name(self), root=root
    )
    return _ORIGINAL(self, operation_name, params)

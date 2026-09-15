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

**Classification is closed, and the default is RAISE.** Read operations pass
through untouched (a shadow run must read the same live inputs the real run
reads). Mutating operations have their key rewritten and then *asserted*.
Anything in neither list raises, because the alternative — passing an
unclassified operation through — is exactly how a live key gets written by a
run everybody believed was a shadow. Adding a new S3 operation to this file is
a deliberate act with a reviewer.
"""

from __future__ import annotations

import threading
from typing import Any

from shadow.root import ShadowGuardViolation, active_root

__all__ = ["install", "installed", "uninstall"]

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

    with _LOCK:
        if _ORIGINAL is not None:
            return
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


def rewrite_params(operation_name: str, api_params: dict, *, service: str, root) -> dict:
    """The whole policy, as a pure function so a test can grade it directly.

    Returns the parameters to call with. Raises ``ShadowGuardViolation`` for
    anything it cannot place in the shadow.
    """
    if service in OUTBOUND_OPERATIONS and operation_name in OUTBOUND_OPERATIONS[service]:
        raise ShadowGuardViolation(
            f"{service}:{operation_name} is an outbound call; a shadow run may not notify, "
            "email or enqueue (plan §6.2 step 4). Refusing rather than sending."
        )
    if service != "s3":
        return api_params
    if operation_name in READ_OPERATIONS:
        return api_params

    bucket = api_params.get("Bucket")
    if operation_name in KEYED_WRITE_OPERATIONS:
        params = dict(api_params)
        params["Key"] = root.key(api_params.get("Key", ""))
        root.assert_shadow_key(params["Key"], operation=f"s3:{operation_name}", bucket=bucket)
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

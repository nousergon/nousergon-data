"""Every object VERSION a graded parity breach names, copied to a retained prefix.

**Why this exists.** A parity report and its adjudication records
(`data_gate.parity_adjudication`, `alpha-engine-config-I12023`) cite S3 object
VERSIONS: v1's live write the report compared (``live_version.manifest_version_id``),
the shadow object it was compared with, each settling input's ``source_key`` @
``source_version_id``, and v1's own later version a settled value was read from.
Most of those versions are NONCURRENT the day after they are written, and the
bucket's ``expire-noncurrent-versions-30d`` rule deletes them 30 days later; the
shadow tree is deleted outright by ``expire-staging-after-7-days``. An
adjudication that cites a purged version can no longer be re-checked by anyone,
which turns "proven on independent evidence" into "asserted once".

Until this module the copy was a hand-run script (``preserve_evidence.py
--apply``, 2026-10-05), with a date somebody had to remember. Now the gate's own
workflow runs ``python -m data_gate preserve-parity-evidence`` right after it
grades the parity clause, and that step:

* takes the report the clause actually graded — `evidence.read_parity`, the
  same reader, so a FROZEN clause preserves the frozen report's evidence and a
  live one the live report's — and every adjudication record filed for that
  report's day (all of them, superseded ones included: a superseded record is
  still part of the append-only chain);
* derives every referenced version (`report_references`, `record_references`).
  A reference that names no version id (the shadow object, a prior-day pair) is
  resolved to the version that was current when the report was GENERATED —
  the object the comparison read — and that resolution is recorded, so it is
  made once and never drifts;
* copies each version server-side to
  ``parity_adjudication/{report_day}/evidence/{source_key}@{version_id}`` in the
  data_collection store — the layout the hand script used, so its copies are
  ADOPTED (verified by their ``source-version-id`` metadata, or by ETag) rather
  than duplicated;
* is idempotent: a version already preserved with a matching version id / ETag
  is skipped, and a run with nothing new writes nothing at all;
* records what it preserved in ``parity_adjudication/{report_day}/evidence/_manifest.json``;
* **fails loudly**: any version that cannot be resolved, read or copied, and any
  retained object that does not verify against its source, is collected and
  raised as `EvidencePreservationError` AFTER the manifest records everything
  that did succeed. The CLI exits 2 and the workflow step is red.

**What it can and cannot write.** Retained keys always contain ``@`` and live
under ``evidence/``, so they never match the adjudication record pattern
(``parity_adjudication/{day}/NNNN.json``) the grader reads: preserving evidence
cannot add, change or supersede an adjudication. A copy is server-side and
byte-identical, so nothing here can author evidence either.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol

__all__ = [
    "EvidencePreservationError",
    "MANIFEST_NAME",
    "MANIFEST_SCHEMA_VERSION",
    "PreservationResult",
    "Reference",
    "S3EvidenceVault",
    "manifest_key",
    "preserve",
    "preserve_graded_parity",
    "record_references",
    "report_references",
    "retained_key",
    "retained_prefix",
]

MANIFEST_SCHEMA_VERSION = "data_parity_evidence_manifest.v1"
MANIFEST_NAME = "_manifest.json"

#: Metadata the copy carries — the same names the 2026-10-05 hand script wrote,
#: so a hand copy and an automatic one verify the same way.
META_SOURCE_KEY = "source-key"
META_SOURCE_VERSION_ID = "source-version-id"
META_SOURCE_LAST_MODIFIED = "source-last-modified"

#: Report verdicts that are not exceptions (`parity_adjudication.report_exceptions`).
_PASSING = frozenset({"match", "v1_cause", "not_applicable", "live_superseded"})

_REPORT_KEY_RE = re.compile(r"^parity/(\d{4}-\d{2}-\d{2})\.json$")


class EvidencePreservationError(RuntimeError):
    """At least one referenced version was not preserved. Never swallowed."""

    def __init__(self, failures: list[str], result: "PreservationResult | None" = None) -> None:
        self.failures = list(failures)
        self.result = result
        head = f"{len(self.failures)} referenced object version(s) NOT preserved"
        super().__init__(head + ": " + "; ".join(self.failures[:10]) + (" ..." if len(self.failures) > 10 else ""))


@dataclass(frozen=True)
class Reference:
    """One object version a breach names. ``version_id`` or ``as_of_utc``, never neither."""

    source_key: str
    version_id: str | None
    as_of_utc: str | None
    referenced_by: str


@dataclass
class PreservationResult:
    report_key: str
    manifest_key: str
    referenced: int = 0
    copied: list[str] = field(default_factory=list)
    adopted: list[str] = field(default_factory=list)
    already: list[str] = field(default_factory=list)
    manifest_written: bool = False
    dry_run: bool = False

    def summary(self) -> str:
        verb = "would copy" if self.dry_run else "copied"
        return (
            f"parity evidence for {self.report_key}: {self.referenced} referenced version(s); "
            f"{verb} {len(self.copied)}, adopted {len(self.adopted)} existing copies, "
            f"{len(self.already)} already preserved; manifest {self.manifest_key} "
            f"{'written' if self.manifest_written else 'unchanged'}"
        )


def retained_prefix(report_day: dt.date) -> str:
    """Store-relative prefix of the retained copies for one report's day."""
    return f"parity_adjudication/{report_day.isoformat()}/evidence/"


def retained_key(report_day: dt.date, source_key: str, version_id: str) -> str:
    return f"{retained_prefix(report_day)}{source_key}@{version_id}"


def manifest_key(report_day: dt.date) -> str:
    return f"{retained_prefix(report_day)}{MANIFEST_NAME}"


def _is_s3_object_key(key: Any) -> bool:
    # `arcticdb/<library>` rows name an ArcticDB library, not an S3 object:
    # ArcticDB never overwrites a version key, so it has nothing to preserve here.
    return isinstance(key, str) and bool(key) and not key.startswith("arcticdb/")


def report_references(report: Mapping[str, Any], report_key: str) -> list[Reference]:
    """Every object version a report's EXCEPTIONS compared.

    Per non-passing S3 row: v1's live key (by the manifest version id the
    report recorded, else as of the report's generation) and the shadow key it
    was compared with. Per row whose ``prior_day_settled`` re-grade is
    unsettled: that D-1 live/shadow pair. Passing rows are not evidence of any
    breach and are not copied.
    """
    as_of = report.get("generated_at")
    as_of = as_of if isinstance(as_of, str) and as_of else None
    out: list[Reference] = []

    def add(key: Any, version_id: Any, why: str) -> None:
        if not _is_s3_object_key(key):
            return
        vid = version_id if isinstance(version_id, str) and version_id and version_id != "null" else None
        out.append(Reference(str(key), vid, None if vid else as_of, f"{report_key}#{why}"))

    for row in report.get("keys") or []:
        key = row.get("key")
        verdict = str(row.get("verdict") or "")
        if verdict and verdict not in _PASSING and row.get("comparator") != "arcticdb":
            add(key, (row.get("live_version") or {}).get("manifest_version_id"), f"{key}:live")
            add(row.get("shadow_key"), None, f"{key}:shadow")
        prior = row.get("prior_day_settled") or {}
        if prior.get("available") and prior.get("settled") is False:
            add(prior.get("key"), None, f"{key}:prior_day_settled.live")
            add(prior.get("shadow_key"), None, f"{key}:prior_day_settled.shadow")
    return out


def record_references(record: Mapping[str, Any], record_key: str) -> list[Reference]:
    """Every version an adjudication record cites: each settling input's v1
    ``source_key``@``source_version_id``, and a v1-later settled read's own."""
    out: list[Reference] = []
    for entry in record.get("exceptions") or []:
        exc_key = entry.get("key")
        for item in entry.get("inputs") or []:
            ident = item.get("id")
            if _is_s3_object_key(item.get("source_key")) and item.get("source_version_id"):
                out.append(Reference(item["source_key"], str(item["source_version_id"]), None,
                                     f"{record_key}#{exc_key}:{ident}:v1_read"))
            settled = item.get("settled") or {}
            if _is_s3_object_key(settled.get("source_key")) and settled.get("source_version_id"):
                out.append(Reference(settled["source_key"], str(settled["source_version_id"]), None,
                                     f"{record_key}#{exc_key}:{ident}:settled"))
    return out


class EvidenceVault(Protocol):
    """The S3 surface preservation needs. Source keys are BUCKET-relative (what
    reports and records name); retained keys are STORE-relative."""

    dry_run: bool

    def list_versions(self, key: str) -> list[dict]: ...
    def head_version(self, key: str, version_id: str) -> dict: ...
    def list_retained(self, prefix: str) -> set[str]: ...
    def head_retained(self, key: str) -> dict: ...
    def copy_version(self, source_key: str, version_id: str, dest_key: str, metadata: dict) -> dict: ...
    def get_retained(self, key: str) -> bytes | None: ...
    def put_retained(self, key: str, payload: bytes) -> None: ...


class S3EvidenceVault:
    """`EvidenceVault` over the data_collection store's own bucket."""

    def __init__(self, client, bucket: str, store_prefix: str, *, dry_run: bool = False) -> None:
        self.client = client
        self.bucket = bucket
        self.store_prefix = store_prefix.strip("/")
        self.dry_run = dry_run

    @classmethod
    def for_store(cls, store) -> "S3EvidenceVault":
        return cls(store.client, store.bucket, store.prefix, dry_run=bool(getattr(store, "dry_run", False)))

    def _full(self, key: str) -> str:
        return f"{self.store_prefix}/{key}" if self.store_prefix else key

    def list_versions(self, key: str) -> list[dict]:
        out: list[dict] = []
        kwargs = {"Bucket": self.bucket, "Prefix": key}
        while True:
            page = self.client.list_object_versions(**kwargs)
            for item in page.get("Versions") or []:
                if item.get("Key") == key:
                    out.append({"VersionId": item["VersionId"], "LastModified": item["LastModified"],
                                "ETag": str(item.get("ETag") or "").strip('"'), "Size": item.get("Size")})
            if not page.get("IsTruncated"):
                return out
            kwargs.update(KeyMarker=page["NextKeyMarker"], VersionIdMarker=page["NextVersionIdMarker"])

    def head_version(self, key: str, version_id: str) -> dict:
        resp = self.client.head_object(Bucket=self.bucket, Key=key, VersionId=version_id)
        return {"ETag": str(resp.get("ETag") or "").strip('"'), "Size": resp.get("ContentLength"),
                "LastModified": resp.get("LastModified")}

    def list_retained(self, prefix: str) -> set[str]:
        out: set[str] = set()
        full = self._full(prefix)
        cut = len(self.store_prefix) + 1 if self.store_prefix else 0
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=full):
            for item in page.get("Contents") or []:
                out.add(item["Key"][cut:])
        return out

    def head_retained(self, key: str) -> dict:
        resp = self.client.head_object(Bucket=self.bucket, Key=self._full(key))
        return {"ETag": str(resp.get("ETag") or "").strip('"'), "Metadata": dict(resp.get("Metadata") or {})}

    def copy_version(self, source_key: str, version_id: str, dest_key: str, metadata: dict) -> dict:
        resp = self.client.copy_object(
            Bucket=self.bucket,
            Key=self._full(dest_key),
            CopySource={"Bucket": self.bucket, "Key": source_key, "VersionId": version_id},
            MetadataDirective="REPLACE",
            Metadata=metadata,
        )
        etag = (resp.get("CopyObjectResult") or {}).get("ETag")
        return {"ETag": str(etag or "").strip('"')}

    def get_retained(self, key: str) -> bytes | None:
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=self._full(key))
        except Exception as exc:  # noqa: BLE001 - absence is an answer; everything else re-raises
            code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise
        return resp["Body"].read()

    def put_retained(self, key: str, payload: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._full(key), Body=payload, ContentType="application/json")


def _iso(value: Any) -> str | None:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value) if value else None


def _parse_utc(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{value!r} carries no UTC offset")
    return parsed.astimezone(dt.timezone.utc)


def _as_utc(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    return _parse_utc(str(value))


def _comparable(manifest: Mapping[str, Any] | None) -> Any:
    if not manifest:
        return None
    return {k: v for k, v in manifest.items() if k != "updated_utc"}


def preserve(
    vault: EvidenceVault,
    references: Iterable[Reference],
    *,
    report_key: str,
    report_day: dt.date,
    now: dt.datetime | None = None,
) -> PreservationResult:
    """Copy every referenced version not yet retained; record it; raise on any miss."""
    now = now or dt.datetime.now(dt.timezone.utc)
    stamp = _iso(now)
    mkey = manifest_key(report_day)
    result = PreservationResult(report_key=report_key, manifest_key=mkey, dry_run=vault.dry_run)
    failures: list[str] = []

    raw = vault.get_retained(mkey)
    previous = json.loads(raw.decode("utf-8")) if raw else None
    if previous is not None and previous.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise EvidencePreservationError(
            [f"{mkey} carries schema_version {previous.get('schema_version')!r}, not {MANIFEST_SCHEMA_VERSION!r}"]
        )
    resolutions: dict[tuple[str, str], str] = {
        (r["source_key"], r["as_of_utc"]): r["version_id"] for r in (previous or {}).get("resolutions") or []
    }
    objects: dict[tuple[str, str], dict] = {
        (o["source_key"], o["source_version_id"]): dict(o) for o in (previous or {}).get("objects") or []
    }

    # 1. Resolve every reference to an exact version, grouping who cites it.
    wanted: dict[tuple[str, str], set[str]] = {}
    for ref in references:
        vid = ref.version_id
        if vid is None:
            if not ref.as_of_utc:
                failures.append(f"{ref.source_key}: cited by {ref.referenced_by} with no version id and no as-of time")
                continue
            vid = resolutions.get((ref.source_key, ref.as_of_utc))
            if vid is None:
                try:
                    cut = _parse_utc(ref.as_of_utc)
                    candidates = [v for v in vault.list_versions(ref.source_key) if _as_utc(v["LastModified"]) <= cut]
                except Exception as exc:  # noqa: BLE001 - collected, raised below
                    failures.append(f"{ref.source_key}: cannot list its versions: {type(exc).__name__}: {exc}")
                    continue
                if not candidates:
                    failures.append(
                        f"{ref.source_key}: no version written at or before {ref.as_of_utc} survives "
                        f"(cited by {ref.referenced_by}) — the evidence is already gone"
                    )
                    continue
                vid = max(candidates, key=lambda v: _as_utc(v["LastModified"]))["VersionId"]
                resolutions[(ref.source_key, ref.as_of_utc)] = vid
        wanted.setdefault((ref.source_key, vid), set()).add(ref.referenced_by)
    result.referenced = len(wanted)

    # 2. One listing of what is already retained.
    try:
        present = vault.list_retained(retained_prefix(report_day))
    except Exception as exc:  # noqa: BLE001 - nothing can be verified without it
        raise EvidencePreservationError([f"cannot list {retained_prefix(report_day)}: {type(exc).__name__}: {exc}"]) from exc

    # 3. Copy, adopt or skip each one.
    for (source_key, vid), cited_by in sorted(wanted.items()):
        dest = retained_key(report_day, source_key, vid)
        entry = objects.get((source_key, vid))
        if entry is not None and dest in present:
            merged = sorted(set(entry.get("referenced_by") or []) | cited_by)
            if merged != entry.get("referenced_by"):
                entry["referenced_by"] = merged
            result.already.append(dest)
            continue
        if dest in present:
            # Retained already but not in the manifest — the hand copy. Verify, then adopt.
            try:
                held = vault.head_retained(dest)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{dest}: present but unreadable: {type(exc).__name__}: {exc}")
                continue
            meta_vid = held["Metadata"].get(META_SOURCE_VERSION_ID)
            source: dict | None = None
            if meta_vid is not None:
                if meta_vid != vid:
                    failures.append(f"{dest}: retained copy says source-version-id {meta_vid!r}, not {vid!r}")
                    continue
                verified_by = "metadata"
            else:
                try:
                    source = vault.head_version(source_key, vid)
                except Exception as exc:  # noqa: BLE001
                    failures.append(f"{dest}: no source-version-id metadata and the source cannot be read to verify it: {exc}")
                    continue
                if not source["ETag"] or "-" in source["ETag"] or source["ETag"] != held["ETag"]:
                    failures.append(f"{dest}: no source-version-id metadata and ETag {held['ETag']!r} does not verify against {source['ETag']!r}")
                    continue
                verified_by = "etag"
            objects[(source_key, vid)] = {
                "source_key": source_key,
                "source_version_id": vid,
                "source_etag": (source or {}).get("ETag"),
                "source_last_modified_utc": held["Metadata"].get(META_SOURCE_LAST_MODIFIED) or _iso((source or {}).get("LastModified")),
                "retained_key": dest,
                "retained_etag": held["ETag"],
                "how": f"adopted ({verified_by})",
                "preserved_utc": stamp,
                "referenced_by": sorted(cited_by),
            }
            result.adopted.append(dest)
            continue
        # Not retained: read the source version, then copy it.
        try:
            source = vault.head_version(source_key, vid)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{source_key}@{vid}: the referenced version cannot be read: {type(exc).__name__}: {exc}")
            continue
        if vault.dry_run:
            result.copied.append(dest)
            continue
        metadata = {
            META_SOURCE_KEY: source_key,
            META_SOURCE_VERSION_ID: vid,
            META_SOURCE_LAST_MODIFIED: _iso(source.get("LastModified")) or "",
        }
        try:
            copied = vault.copy_version(source_key, vid, dest, metadata)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{source_key}@{vid}: copy to {dest} failed: {type(exc).__name__}: {exc}")
            continue
        if source["ETag"] and "-" not in source["ETag"] and copied.get("ETag") and copied["ETag"] != source["ETag"]:
            failures.append(f"{dest}: copied ETag {copied['ETag']!r} differs from the source's {source['ETag']!r}")
            continue
        objects[(source_key, vid)] = {
            "source_key": source_key,
            "source_version_id": vid,
            "source_etag": source["ETag"],
            "source_last_modified_utc": _iso(source.get("LastModified")),
            "retained_key": dest,
            "retained_etag": copied.get("ETag"),
            "how": "copied",
            "preserved_utc": stamp,
            "referenced_by": sorted(cited_by),
        }
        result.copied.append(dest)

    # Manifest entries whose retained copy has since disappeared are evidence lost.
    for (source_key, vid), entry in objects.items():
        if (source_key, vid) not in wanted and entry.get("retained_key") not in present:
            failures.append(f"{entry.get('retained_key')}: listed in the manifest but no longer retained")

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "report_key": report_key,
        "report_day": report_day.isoformat(),
        "retained_prefix": retained_prefix(report_day),
        "updated_utc": stamp,
        "resolutions": [
            {"source_key": k, "as_of_utc": a, "version_id": v} for (k, a), v in sorted(resolutions.items())
        ],
        "objects": [objects[k] for k in sorted(objects)],
    }
    if not vault.dry_run and _comparable(manifest) != _comparable(previous):
        try:
            vault.put_retained(mkey, json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"))
            result.manifest_written = True
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{mkey}: manifest write failed: {type(exc).__name__}: {exc}")
    if failures:
        raise EvidencePreservationError(failures, result)
    return result


def preserve_graded_parity(
    store,
    *,
    trading_day: dt.date,
    vault: EvidenceVault | None = None,
    now: dt.datetime | None = None,
) -> PreservationResult | None:
    """Preserve the evidence of the parity report the clause grades for ``trading_day``.

    ``None`` when the clause selects no report, or the selected report carries
    no exception and no adjudication has been filed for it — nothing breached,
    so nothing is evidence. An unmeasurable clause raises: we could not see
    which report is graded, so we cannot say its evidence is safe.
    """
    from data_gate import evidence
    from data_gate import parity_adjudication as adj

    reading = evidence.read_parity(store, trading_day=trading_day)
    report_key = next((k for k in reading.evidence if _REPORT_KEY_RE.match(k)), None)
    if reading.unmeasurable:
        raise EvidencePreservationError([f"the parity clause is UNMEASURABLE, so its evidence cannot be located: {reading.detail}"])
    if report_key is None:
        return None
    report_day = dt.date.fromisoformat(_REPORT_KEY_RE.match(report_key).group(1))
    report = json.loads(store.get_bytes(report_key).decode("utf-8"))

    references = report_references(report, report_key)
    prefix = f"{adj.ADJUDICATION_KEY_PREFIX}{report_day.isoformat()}/"
    for record_key in adj.adjudication_record_keys(store.list_keys(prefix), report_day):
        try:
            record = json.loads(store.get_bytes(record_key).decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - a record we cannot read is evidence we cannot preserve
            raise EvidencePreservationError([f"{record_key}: cannot read the adjudication record: {type(exc).__name__}: {exc}"]) from exc
        references.extend(record_references(record, record_key))
    if not references:
        return None

    if vault is None:
        if not hasattr(store, "bucket") or not hasattr(store, "client"):
            raise EvidencePreservationError(
                [f"store {getattr(store, 'uri', store)!r} is not an S3 store; object versions exist only in S3"]
            )
        if report.get("bucket") and report["bucket"] != store.bucket:
            raise EvidencePreservationError(
                [f"{report_key} names bucket {report['bucket']!r}, but the store is {store.bucket!r}"]
            )
        vault = S3EvidenceVault.for_store(store)
    return preserve(vault, references, report_key=report_key, report_day=report_day, now=now)

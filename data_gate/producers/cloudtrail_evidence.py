"""Bounded CloudTrail evidence export. No audit-resource writes or CE calls.

Only the private destination receives raw metadata. Public logs contain status.
A successful sample never establishes complete archive or IAM-cycle coverage.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import io
import json
import os
from pathlib import Path
import time

UTC = dt.timezone.utc
SCHEMA = Path(__file__).resolve().parents[2] / "contracts/cloudtrail_evidence.v1.schema.json"


class LimitReached(Exception):
    pass


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return parsed.astimezone(UTC)


def selectors_match(value, bucket):
    selectors = value.get("EventSelectors", [])
    management = any(s.get("ReadWriteType") == "All" and s.get("IncludeManagementEvents") is True
                     and not s.get("ExcludeManagementEventSources") for s in selectors)
    writes = any(s.get("ReadWriteType") in ("WriteOnly", "All") and
                 any(r.get("Type") == "AWS::S3::Object" and
                     f"arn:aws:s3:::{bucket}/" in r.get("Values", [])
                     for r in s.get("DataResources", [])) for s in selectors)
    return management and writes


def lifecycle_matches(value):
    enabled = [r for r in value.get("Rules", []) if r.get("Status") == "Enabled"]
    if not enabled or any("Expiration" in r for r in enabled):
        return False
    # Compare the entire enabled action set; a prefix filter cannot prove
    # retention for the bucket, and extra transitions can change that retention.
    actions = []
    for rule in enabled:
        if rule.get("Prefix", "") or rule.get("Filter", {}) not in ({}, {"Prefix": ""}):
            return False
        action = {k: v for k, v in rule.items() if k not in ("ID", "Status", "Prefix", "Filter")}
        actions.append(action)
    expected = [
        {"Transitions": [{"Days": 90, "StorageClass": "GLACIER"}]},
        {"NoncurrentVersionExpiration": {"NoncurrentDays": 365}},
        {"AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}},
    ]
    return sorted(map(lambda x: json.dumps(x, sort_keys=True), actions)) == sorted(
        map(lambda x: json.dumps(x, sort_keys=True), expected))


def validate(document):
    import jsonschema
    jsonschema.Draft202012Validator(json.loads(SCHEMA.read_text())).validate(document)


def collect(s3, cloudtrail, *, archive_bucket, archive_prefix, research_bucket,
            trail, since, now, source_sha, max_requests=200, max_bytes=16*1024*1024,
            max_seconds=120):
    if since.tzinfo is None or now.tzinfo is None or since > now:
        raise ValueError("invalid aware observation window")
    if min(max_requests, max_bytes, max_seconds) <= 0:
        raise ValueError("positive limits required")
    started = time.monotonic()
    prefix = archive_prefix.rstrip("/") + "/"
    result = {
        "schema_version": "cloudtrail_evidence.v1",
        "collected_at": now.isoformat(), "source_sha": source_sha,
        "window": {"start": since.isoformat(), "end": now.isoformat()},
        "status": "unobserved", "sample": None,
        "configuration": {"selectors": None, "lifecycle": None,
                          "selectors_match": False, "lifecycle_matches": False},
        "coverage": {"full_cycle_verified": False, "current_day_partial": True,
                     "truncated": False, "regions": [], "partitions": [],
                     "scope": "delivered region prefixes only; sample search, not a complete audit"},
        "limits": {"requests": max_requests, "bytes": max_bytes, "seconds": max_seconds},
        "requests": 0, "compressed_bytes": 0, "decoded_bytes": 0,
        "errors": [],
    }

    def call(fn, **kwargs):
        if result["requests"] >= max_requests or time.monotonic() - started >= max_seconds:
            raise LimitReached()
        result["requests"] += 1
        return fn(**kwargs)

    def pages(**kwargs):
        token = None
        seen = set()
        while True:
            page = call(s3.list_objects_v2, Bucket=archive_bucket, MaxKeys=100,
                        **kwargs, **({"ContinuationToken": token} if token else {}))
            yield page
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")
            if not token or token in seen:
                raise ValueError("invalid pagination token")
            seen.add(token)

    def sample_record(record, key):
        if record.get("eventSource") != "s3.amazonaws.com" or record.get("eventName") not in {
            "PutObject", "DeleteObject", "DeleteObjects", "CompleteMultipartUpload", "CopyObject"
        } or record.get("errorCode") or record.get("errorMessage"):
            return None
        params = record.get("requestParameters") or {}
        if params.get("bucketName") != research_bucket:
            return None
        # A DeleteObjects envelope alone is not proof of a particular object write.
        if not params.get("key") or not record.get("eventID"):
            return None
        event_time = timestamp(record["eventTime"])
        if not since <= event_time <= now:
            return None
        identity = record.get("userIdentity") or {}
        principal = (identity.get("sessionContext") or {}).get("sessionIssuer", {}).get("arn") or identity.get("arn")
        if not principal:
            return None
        return {"archive_bucket": archive_bucket, "archive_key": key,
                "event_id": record["eventID"], "event_time": event_time.isoformat(),
                "operation": record["eventName"], "principal": principal,
                "bucket": research_bucket, "key": params["key"]}

    stage = "configuration"
    try:
        selectors = call(cloudtrail.get_event_selectors, TrailName=trail)
        lifecycle = call(s3.get_bucket_lifecycle_configuration, Bucket=archive_bucket)
        # AWS response metadata is transport detail, not configuration evidence.
        selectors.pop("ResponseMetadata", None)
        lifecycle.pop("ResponseMetadata", None)
        result["configuration"] = {"selectors": selectors, "lifecycle": lifecycle,
                                  "selectors_match": selectors_match(selectors, research_bucket),
                                  "lifecycle_matches": lifecycle_matches(lifecycle)}
        stage = "region_discovery"
        regions = []
        for page in pages(Prefix=prefix, Delimiter="/"):
            for entry in page.get("CommonPrefixes", []):
                region_prefix = entry["Prefix"]
                if not region_prefix.startswith(prefix) or "/" in region_prefix[len(prefix):].strip("/"):
                    raise ValueError("invalid region prefix")
                regions.append(region_prefix)
        result["coverage"]["regions"] = sorted(set(regions))
        stage = "archive_scan"
        # Recent deliveries first: bounded sampling should not spend its whole
        # budget on old partitions. Exhaustion stays explicit, never zero writes.
        day = now.date()
        while day >= since.date() and result["sample"] is None:
            for region_prefix in sorted(set(regions)):
                partition = region_prefix + day.strftime("%Y/%m/%d/")
                observed = {"prefix": partition, "objects_read": 0, "listing_complete": False}
                result["coverage"]["partitions"].append(observed)
                for page in pages(Prefix=partition):
                    for item in page.get("Contents", []):
                        key = item["Key"]
                        if item.get("LastModified") and item["LastModified"] < since:
                            continue
                        if not key.startswith(partition) or not key.endswith(".json.gz"):
                            raise ValueError("unexpected archive object")
                        if item.get("StorageClass") in {"GLACIER", "DEEP_ARCHIVE"}:
                            raise ValueError("archive restore prohibited")
                        body = call(s3.get_object, Bucket=archive_bucket, Key=key)["Body"]
                        try:
                            compressed = body.read(max_bytes - result["compressed_bytes"] + 1)
                        finally:
                            body.close()
                        result["compressed_bytes"] += len(compressed)
                        if result["compressed_bytes"] > max_bytes:
                            raise LimitReached()
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
                            decoded = stream.read(max_bytes - result["decoded_bytes"] + 1)
                        result["decoded_bytes"] += len(decoded)
                        if result["decoded_bytes"] > max_bytes:
                            raise LimitReached()
                        records = json.loads(decoded)["Records"]
                        if not isinstance(records, list):
                            raise ValueError("Records must be a list")
                        observed["objects_read"] += 1
                        for record in records:
                            if time.monotonic() - started >= max_seconds:
                                raise LimitReached()
                            candidate = sample_record(record, key)
                            if candidate:
                                result["sample"] = candidate
                                break
                        if result["sample"]:
                            break
                    if result["sample"]:
                        break
                    observed["listing_complete"] = not page.get("IsTruncated", False)
                if result["sample"]:
                    break
            day -= dt.timedelta(days=1)
    except LimitReached:
        # Bounded scan exhaustion preserves only observations; the explicit
        # truncation field forbids consumers from interpreting absence as zero.
        result["coverage"]["truncated"] = True
    except Exception as exc:
        # Diagnostic failures are published privately and cause a nonzero CLI
        # exit. No exception text (possibly sensitive AWS payload) reaches logs.
        result["errors"].append({"stage": stage, "type": type(exc).__name__})
    config = result["configuration"]
    if result["errors"]:
        result["status"] = "error"
    elif not config["selectors_match"] or not config["lifecycle_matches"]:
        result["status"] = "configuration_mismatch" if config["selectors"] is not None else "unobserved"
    elif result["sample"]:
        result["status"] = "verified_sample"
    validate(result)
    return result


def main(argv=None):
    import boto3
    from botocore.config import Config
    from data_gate.producers.executor_profile import DEFAULT_ARCHIVE_BUCKET, DEFAULT_ARCHIVE_PREFIX, DEFAULT_BUCKET
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True)
    parser.add_argument("--trail", required=True)
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)
    sha = os.environ["GITHUB_SHA"]
    config = Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=10)
    s3 = boto3.client("s3", region_name="us-east-1", config=config)
    evidence = collect(s3, boto3.client("cloudtrail", region_name="us-east-1", config=config),
                       archive_bucket=DEFAULT_ARCHIVE_BUCKET, archive_prefix=DEFAULT_ARCHIVE_PREFIX,
                       research_bucket=DEFAULT_BUCKET, trail=args.trail, since=timestamp(args.since),
                       now=dt.datetime.now(UTC), source_sha=sha)
    if not args.no_write:
        # Each run is immutable-by-name; latest is updated even on failure so
        # stale successful proof cannot masquerade as the current observation.
        run = os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
        payload = json.dumps(evidence, sort_keys=True).encode()
        for key in (f"ops/checks/cloudtrail-evidence/runs/{run}.json",
                    "ops/checks/cloudtrail-evidence/latest.json"):
            s3.put_object(Bucket=DEFAULT_BUCKET, Key=key, Body=payload, ContentType="application/json")
    print("cloudtrail evidence status=" + evidence["status"])
    return 0 if evidence["status"] == "verified_sample" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("cloudtrail evidence failed: " + type(exc).__name__)
        raise SystemExit(1) from None

#!/usr/bin/env python3
"""Publish the alert-class DELIVERY TIER registry that `krepis.alerts` routes on.

alpha-engine-config-I6751 Phase 1 (subsumes I6293).

WHY AN ARTIFACT AND NOT A COPY IN krepis
----------------------------------------
`krepis` is a PyPI library with no dependency on this repo, and the emitters it
serves run on Lambdas, spot boxes and the laptop — none of which check this
repository out. The alternative to publishing is a hand-kept tier table inside
krepis, which is a recorded fleet bug class: `alpha-engine-config-I10121`, where
`iam-drift-check` needed a hand-written allowlist twin of a source it could have
read and reddened `main` four times in three days before the twin was deleted.
So the registry stays a single file in this repo and is PUBLISHED; krepis reads
the published object and never carries a second copy.

The shape is deliberately the one `sync-llm-model-registry.yml` and
`sync-artifact-registry.yml` already use (shared-code-policy, third adoption of
the pattern: a validated registry in the repo, published to the bucket the
consumer reads, with a scheduled arm that asserts repo == S3 so "the publish
never fired" and "the publish fired and worked" are different shapes).

WHY `alpha-engine-research`
---------------------------
`krepis.alerts` ALREADY reads and writes that bucket on every deduped publish
(`DEFAULT_DEDUP_BUCKET`, `_alerts/_dedup/`). Every identity that can emit a
fleet alert can therefore already read this object: no new bucket, no new IAM
grant, no new failure mode on the alerting path. A new bucket would have added a
grant to ~20 execution roles for one small JSON file.

WHY IT VERIFIES ITS OWN WRITE
-----------------------------
`principles.md` §2.3 — detect, act, VERIFY, close. An `aws s3 cp` that exits 0
is an event; the read-back is the effect. A failed verification is a non-zero
exit, so a publish that did not land reddens the workflow rather than being
inferred from an exit code that only proves a request was accepted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import jsonschema
import yaml

logger = logging.getLogger("publish_alert_tier_registry")

HERE = Path(__file__).resolve().parent
PLAYBOOKS = HERE / "playbooks.yaml"
SCHEMA = HERE / "playbooks.schema.json"

#: Must match `krepis.alert_tiers.REGISTRY_BUCKET` / `REGISTRY_OBJECT`. The
#: producer and the consumer resolve ONE string pair, or the emitters read an
#: object nothing publishes to and every source resolves `page` — loud, but
#: wrong, and it would restore exactly the 37-emails-a-day baseline this closes.
REGISTRY_BUCKET = "alpha-engine-research"
REGISTRY_OBJECT = "overseer/alert_tier_registry.json"

#: Bumped only on a breaking shape change. `krepis.alert_tiers` refuses a
#: version it does not know and falls back to PAGE — loudly, never silently.
SCHEMA_VERSION = 1


def build_document(playbooks_path: Path = PLAYBOOKS) -> dict:
    """Distil `alert_classes` into the routing document krepis consumes.

    Raises on a malformed registry rather than publishing a partial one: this
    is a PRODUCER, and a half-written routing table silently downgrades real
    pages (`~/Development/CLAUDE.md`, *Fail loud and fast*).
    """
    raw = playbooks_path.read_text()
    doc = yaml.safe_load(raw)
    jsonschema.validate(doc, json.loads(SCHEMA.read_text()))

    entries = []
    for row in doc["alert_classes"]:
        entry = {
            "class": row["class"],
            "source": row["source"],
            "tier": row["tier"],
            "severities": row["severities"],
        }
        if "page_after_consecutive" in row:
            entry["page_after_consecutive"] = row["page_after_consecutive"]
        if "episode_overrides" in row:
            entry["episode_overrides"] = row["episode_overrides"]
        entries.append(entry)

    sources = [e["source"] for e in entries]
    dupes = sorted({s for s in sources if sources.count(s) > 1})
    # A duplicated source is not fatal — `metron` is knowingly shared by two
    # classes (alpha-engine-config-I8995, awaiting a ruling) — but the resolver
    # must not pick between them silently, so the collision is carried in the
    # document and the consumer takes the STRICTEST tier of the colliding rows.
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_digest": "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
        "colliding_sources": dupes,
        "entries": entries,
    }


def publish(document: dict, *, bucket: str, obj: str, dry_run: bool) -> int:
    body = json.dumps(document, indent=2, sort_keys=True).encode()
    if dry_run:
        sys.stdout.write(body.decode() + "\n")
        return 0
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=obj, Body=body,
                  ContentType="application/json")
    # Read-back verification. Compare the SOURCE DIGEST, not the bytes: the
    # document carries `generated_at`, so a byte compare would be a tautology
    # on the write we just made and would fail against any concurrent run.
    got = json.loads(s3.get_object(Bucket=bucket, Key=obj)["Body"].read())
    if got.get("source_digest") != document["source_digest"]:
        logger.error(
            "alert-tier registry read-back MISMATCH at s3://%s/%s: wrote "
            "source_digest=%s, read %s. Every fleet emitter routes on this "
            "object; a stale one silently downgrades pages.",
            bucket, obj, document["source_digest"], got.get("source_digest"),
        )
        return 1
    logger.info(
        "published %d alert-class tiers to s3://%s/%s (digest %s), read-back OK",
        len(document["entries"]), bucket, obj, document["source_digest"],
    )
    return 0


def assert_in_sync(*, bucket: str, obj: str) -> int:
    """Scheduled arm: assert the published object still matches the repo."""
    document = build_document()
    s3 = boto3.client("s3")
    got = json.loads(s3.get_object(Bucket=bucket, Key=obj)["Body"].read())
    if got.get("source_digest") != document["source_digest"]:
        logger.error(
            "alert-tier registry DRIFT: s3://%s/%s carries source_digest=%s, "
            "the repo says %s. The publish arm did not fire, or something else "
            "wrote that object.", bucket, obj, got.get("source_digest"),
            document["source_digest"],
        )
        return 1
    logger.info("alert-tier registry in sync (%s)", document["source_digest"])
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", default=REGISTRY_BUCKET)
    ap.add_argument("--object", dest="obj", default=REGISTRY_OBJECT)
    ap.add_argument("--dry-run", action="store_true",
                    help="print the document, write nothing")
    ap.add_argument("--assert-in-sync", action="store_true",
                    help="verify the published object matches the repo; publish nothing")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.assert_in_sync:
        return assert_in_sync(bucket=args.bucket, obj=args.obj)
    return publish(build_document(), bucket=args.bucket, obj=args.obj,
                   dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())

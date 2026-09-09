"""Every alert class declares a delivery tier — alpha-engine-config-I6751.

These are the tests that make the tier field load-bearing rather than
decorative. The schema already REQUIRES `tier` on every row; these assert the
things a schema cannot:

* the published document is the shape `krepis.alert_tiers` reads,
* the producer/consumer string pair is one string pair,
* no row silently declares a tier that would route a genuinely-broken class
  into silence.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

OVERSEER = Path(__file__).resolve().parents[1] / "infrastructure" / "overseer"
PLAYBOOKS = OVERSEER / "playbooks.yaml"

TIERS = {"page", "notify-silent", "tracked-only", "dynamic"}


@pytest.fixture(scope="module")
def rows() -> list[dict]:
    return yaml.safe_load(PLAYBOOKS.read_text())["alert_classes"]


def test_every_alert_class_declares_a_tier(rows):
    missing = [r["class"] for r in rows if "tier" not in r]
    assert missing == [], (
        "alert_classes rows with no `tier`: "
        f"{missing}. A row with no tier cannot be routed, so krepis.alert_tiers "
        "pages for it and records registry drift — loud, but not the intent."
    )
    bad = [(r["class"], r["tier"]) for r in rows if r["tier"] not in TIERS]
    assert bad == []


def test_dynamic_tier_is_only_used_where_severity_is_a_runtime_fact(rows):
    """`dynamic` says the seriousness varies with the observed facts.

    observability-policy.md 7.1 — derived where derivable. A row whose
    severities are a fixed single value has a derivable tier and must declare
    it, or `dynamic` becomes the shrug that puts severity back in charge.
    """
    offenders = [
        r["class"] for r in rows
        if r["tier"] == "dynamic" and len(set(r["severities"])) == 1
        and r["severities"][0] != "dynamic"
    ]
    assert offenders == [], (
        f"rows declaring tier: dynamic on a single fixed severity: {offenders}"
    )


def test_no_critical_only_class_is_routed_to_tracked_only(rows):
    """A class that can ONLY fire at critical/alarm is not hygiene.

    This is the assertion that stops the tier field from becoming a mute
    switch. A genuinely-broken condition may be held by
    `page_after_consecutive`, never by a tier declaration.
    """
    offenders = [
        r["class"] for r in rows
        if r["tier"] == "tracked-only"
        and set(r["severities"]) <= {"critical", "alarm"}
    ]
    assert offenders == [], (
        f"critical-only classes declared tracked-only: {offenders}"
    )


def test_page_after_consecutive_is_a_positive_integer(rows):
    for r in rows:
        if "page_after_consecutive" in r:
            assert isinstance(r["page_after_consecutive"], int)
            assert r["page_after_consecutive"] >= 1
            assert r["tier"] in ("page", "dynamic"), (
                f"{r['class']}: a consecutive-detection gate on a non-paging "
                f"tier does nothing"
            )


def test_published_document_matches_the_consumer_contract():
    """The producer and `krepis.alert_tiers` resolve ONE bucket/object pair."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "publish_alert_tier_registry", OVERSEER / "publish_alert_tier_registry.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    doc = mod.build_document()
    assert doc["schema_version"] == 1
    assert doc["source_digest"].startswith("sha256:")
    assert len(doc["entries"]) == len(
        yaml.safe_load(PLAYBOOKS.read_text())["alert_classes"]
    )
    for entry in doc["entries"]:
        assert set(entry) <= {
            "class", "source", "tier", "severities", "page_after_consecutive",
            "episode_overrides",
        }
        assert entry["tier"] in TIERS

    # Serialisable exactly as published — a document that cannot round-trip
    # through JSON reaches the consumer as a registry-unavailable fail-upward.
    json.loads(json.dumps(doc))

    # The consumer contract, asserted as LITERALS rather than by importing
    # krepis. The installed krepis on any given runner may predate the
    # consumer (this repo does not pin it in lockstep), and an
    # `importorskip` that skips is a contract test that never ran — which is
    # how a producer and its consumer drift apart while CI is green.
    assert mod.REGISTRY_BUCKET == "alpha-engine-research"
    assert mod.REGISTRY_OBJECT == "overseer/alert_tier_registry.json"
    assert mod.SCHEMA_VERSION == 1

    krepis_tiers = pytest.importorskip("krepis.alert_tiers")
    if hasattr(krepis_tiers, "REGISTRY_BUCKET"):
        assert mod.REGISTRY_BUCKET == krepis_tiers.REGISTRY_BUCKET
        assert mod.REGISTRY_OBJECT == krepis_tiers.REGISTRY_OBJECT
        assert mod.SCHEMA_VERSION == krepis_tiers.SUPPORTED_SCHEMA_VERSION


def test_a_tracked_only_row_still_declares_an_intake_or_a_reason(rows):
    """Suppression is a delivery decision, never a recording one (7.2a).

    A tracked-only row whose intake is `none` AND which carries no
    operator_reason / migration_issue would be suppressed on the channel and
    invisible to the response plane — the exact trade the policy forbids.
    """
    offenders = [
        r["class"] for r in rows
        if r["tier"] == "tracked-only" and r["intake"] == "none"
        and not (r.get("operator_reason") or r.get("migration_issue"))
    ]
    assert offenders == [], (
        f"tracked-only rows that are also drain-blind with no declaration: "
        f"{offenders}"
    )

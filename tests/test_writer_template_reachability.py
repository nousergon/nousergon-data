"""`data_gate.writer_template_check` — the other direction of the writer bijection.

`alpha-engine-config-I10895`. `data_gate/inventory.py`'s bijection proves every
write CALL SITE has a descriptor and every descriptor's `code_path` has a call
site; it never checks that the descriptor's declared `writes:` KEY is one the
call site can actually produce. D03 (`predictor/price_cache/*.parquet`, a
retired tree) and D46 (`data/insider_transactions/{date}.parquet`, a shape
never produced) drifted silently under exactly that gap.

This module is code-derived (no AWS) and deliberately conservative — see its
own docstring for what it can and cannot resolve. `test_no_new_unit_goes_
stale_without_review` pins the CURRENT fleet-wide reading as a reviewed
baseline: a fix landing here should shrink `_KNOWN_FLAGGED`, never grow it
silently.
"""

from __future__ import annotations

from data_gate.descriptors import load_units
from data_gate.writer_template_check import run_all

#: Units this detector flags today (measured 2026-09-15, alpha-engine-config-I10895),
#: neither of which this PR fixes:
#:  - D01 (`collectors/constituents.py`): 4 S3 `put_object` call sites (dual
#:    `data/`+`reference/price_cache/` writes of sector_map.json,
#:    sub_industry_map.json, sub_sector_etf_map.json) against 2 declared
#:    `writes[]` entries — a real, pre-existing under-declaration, not caused
#:    by this PR.
#:  - D39 (`data/derived/inst_ownership.py`): 4 call sites against 2 declared
#:    entries — `data/inst_ownership/{quarter}/latest.parquet`
#:    (`write_inst_ownership_parquet`, `.github/workflows/inst-ownership-weekly.yml:193`)
#:    is undeclared. `registry.d/units/D39-inst-ownership.yaml` is being
#:    edited live by a sibling PR (fix/d39-crucible-consumer-I10873); not
#:    touched here.
#: Both are reported, not fixed, per this issue's deliverable 3.
_KNOWN_FLAGGED = frozenset({"D01", "D39"})


def test_d03_prices_writes_template_is_reachable():
    """The class bug this issue fixes: D03 declared the retired
    `predictor/price_cache/*.parquet` tree. Corrected to
    `reference/price_cache/{ticker}.parquet` — `PRICE_CACHE_NEW_PREFIX`,
    reached one hop through `price_cache_write_prefixes()` from
    `collectors/prices.py`'s write call."""
    finding = next(f for f in run_all() if f.unit_id == "D03")
    assert finding.status == "ok", finding.detail


def test_d46_insider_transactions_writes_templates_are_reachable():
    """D46 declared `data/insider_transactions/{date}.parquet`, produced by
    nothing. Corrected to the two keys `write_form4_parquet` actually
    writes: the run-stamped artifact and the `latest.json` sidecar — this
    also satisfies the call-site-count signal (2 calls, 2 declared entries),
    which the old single-entry descriptor failed."""
    finding = next(f for f in run_all() if f.unit_id == "D46")
    assert finding.status == "ok", finding.detail


def test_no_new_unit_goes_stale_without_review():
    """The class detector, run fleet-wide. A unit newly appearing here is a
    real finding — investigate and either fix its descriptor/writer or add it
    to `_KNOWN_FLAGGED` with the same evidence trail the two above carry. A
    unit DISAPPEARING from `_KNOWN_FLAGGED` (fixed) should shrink the
    constant in the same PR."""
    flagged = {f.unit_id for f in run_all() if f.status == "flagged"}
    assert flagged == _KNOWN_FLAGGED, (
        f"writer-template detector flagged {sorted(flagged)}, expected exactly "
        f"{sorted(_KNOWN_FLAGGED)}. A new name here is an undeclared write target "
        "(or an under-declared writes[] count) nobody has triaged yet; a name "
        "missing here is a fix that should shrink _KNOWN_FLAGGED."
    )


def test_every_in_service_unit_is_graded_ok_flagged_or_unverifiable():
    """No unit silently falls through the detector with no reading at all."""
    findings = run_all()
    assert {f.status for f in findings} <= {"ok", "flagged", "unverifiable"}
    assert len(findings) == len(load_units())

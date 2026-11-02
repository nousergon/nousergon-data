"""validators/d02_reference_diff_probe.py — weekday, READ-ONLY rehearsal of
D02's reference attestation (alpha-engine-config-I13045; prevention for
nous-ergon-ops-I2043).

WHY. D02 (``collectors.historical_constituents.collect``) fails closed on any
S&P 500 roster change the reference changes table cannot explain, and that is
correct. But it only looks on Saturday. Two weeklies in a row went down on a
change that was observable days earlier:

* 2026-10-03, VYLR spin-off addition (fixed by a declaration in
  ``collectors/data/sp500_declared_spinoff_additions.json``);
* 2026-10-10, PSKY -> SKYD reticker effective Tue 2026-10-06 (fixed by a
  declaration in ``collectors/data/sp500_known_retickers.json``).

Each fix was a committed declaration a person could only write after the
weekly had failed. This probe makes the same comparison on a weekday, against
TODAY's holdings file standing in for the snapshot Saturday will write, and
names each disagreement nothing explains together with the declaration file
that would explain it, so the declaration PR lands before Saturday. D02 itself
is unchanged and stays fail-closed.

WHAT IT READS, and what it does not do:

* the stored dated roster snapshots (``market_data/weekly/*/constituents.json``),
  the previous D02 build's ``rename_checks`` and ``reference/price_cache/`` —
  S3 GETs only, through the same loaders D02 uses;
* today's SPY/MDY holdings files (``collectors.constituents._fetch_ssga_membership``
  — NOT ``_fetch_constituents``, which falls back to a local cache: a cached
  roster is an older day's list, and diffing it would hide exactly the change
  this probe exists to see);
* the reference changes table (Wikipedia), as D02 fetches it.

It WRITES nothing to any data store, and it never asks Polygon. Saturday's
D02 also asks Polygon about swaps nothing else settles, and may accept one
Polygon-confirmed spin-off PROVISIONALLY; neither saved the 10-03 or 10-10
runs (Polygon reports neither BK -> BNY nor PSKY -> SKYD as a ticker change),
so a swap the probe cannot settle is reported, and the right response is
still a committed declaration. A finding the probe reports and Saturday
resolves is a conservative false alarm, never a missed failure.

Its only output is a report (stdout JSON, or ``--out`` local file) and, unless
``--no-alert``, one ``nousergon_lib.alerts.publish`` when something is
undeclared — the same alert path ``constituents_drift_check`` uses.

Usage::

    NE_RESEARCH_BUCKET=<bucket> python -m validators.d02_reference_diff_probe
    python -m validators.d02_reference_diff_probe --bucket <bucket> --no-alert

Exit 0 when every disagreement is explained (``clear``), 1 when at least one is
not (``undeclared``), 2 when an input could not be read (``error``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from collectors import historical_constituents as hc
from collectors.historical_constituents import ADDED, REMOVED, ConstituentChange

logger = logging.getLogger(__name__)

TRACKER = "alpha-engine-config-I13045"
RETICKERS_FILE = "collectors/data/sp500_known_retickers.json"
SPINOFFS_FILE = "collectors/data/sp500_declared_spinoff_additions.json"

#: How long a suggested spin-off declaration stays valid. VYLR's committed
#: entry ran 30 days past its effective date.
SUGGESTED_SPINOFF_VALIDITY_DAYS = 30

#: The alert's severity is fixed at ``warning`` and declared as such in
#: infrastructure/overseer/playbooks.yaml (class
#: ``data_d02_reference_diff_probe``): a finding is days ahead of the run it
#: would fail, so it is never a page.

_FINDING_RE = re.compile(
    r"^(?P<side>observed|reference) (?P<verb>added|removed) (?P<ticker>\S+) "
    r"(?:not in reference|not observed)$"
)


def resolve_renames_offline(
    universe: dict[str, list[str]],
    *,
    reference_removed: set[str] | None,
    prior: dict | None,
    closes: Callable[[str], object] | None,
    known_retickers: list[dict] | None = None,
) -> hc.RenameResolution:
    """D02's rename resolution with the Polygon leg removed.

    Same order as :func:`collectors.historical_constituents.resolve_renames`:
    committed retickers, then the previous build's settled verdicts, then the
    reference's own removals (excluded from inference), then price identity
    (:func:`collectors.historical_constituents.infer_price_identity_renames`).
    Candidates left over are what Saturday would ask Polygon about; they are
    returned in ``deferred`` rather than queried.
    """
    out = hc.RenameResolution()
    swaps = hc.same_date_swaps(universe)
    candidates = sorted({t for tickers in swaps.values() for t in tickers})
    if known_retickers is None:
        known_retickers = json.loads(hc._KNOWN_RETICKERS_PATH.read_text())["retickers"]
    out.renames = {r["old"]: r["new"] for r in known_retickers if r["old"] in candidates}
    candidates = [t for t in candidates if t not in out.renames]
    if prior:
        prior_renamed = dict(prior.get("renamed") or {})
        prior_not = set(prior.get("not_renamed") or [])
        out.settled_by_prior_run = [
            t for t in candidates if t in prior_renamed or t in prior_not
        ]
        out.renames.update({t: prior_renamed[t] for t in candidates if t in prior_renamed})
        out.not_renamed = [t for t in candidates if t in prior_not and t not in prior_renamed]
        candidates = [t for t in candidates if t not in out.settled_by_prior_run]
    if reference_removed is not None:
        out.confirmed_by_reference = [t for t in candidates if t in reference_removed]
        candidates = [t for t in candidates if t not in reference_removed]
    out.deferred = list(candidates)
    if closes is not None:
        hc.infer_price_identity_renames(out, hc.same_date_swap_pairs(universe), closes)
    return out


def _parse_finding(text: str) -> tuple[str, str, str] | None:
    m = _FINDING_RE.match(text)
    return (m["side"], m["verb"], m["ticker"]) if m else None


def classify(
    unexplained: list[str],
    observed: list[ConstituentChange],
    *,
    newest_stored: str | None,
) -> list[dict]:
    """One finding per unexplained disagreement (a reticker pair is ONE).

    Each finding names the declaration file that would explain it and a
    suggested entry with the fields a person must still fill in marked
    ``<fill in: ...>`` — the evidence is a human's claim, never the probe's.
    ``new_this_week`` is True when the observed change is dated after the
    newest stored snapshot, i.e. it is visible only in today's holdings file.
    """
    dates: dict[tuple[str, str], str] = {}
    for c in observed:
        dates.setdefault((c.ticker, c.action), c.date)

    parsed = [(text, _parse_finding(text)) for text in unexplained]
    removed_on: dict[str, list[str]] = {}
    added_on: dict[str, list[str]] = {}
    for _text, p in parsed:
        if p and p[0] == "observed":
            day = dates.get((p[2], p[1]))
            (removed_on if p[1] == REMOVED else added_on).setdefault(day, []).append(p[2])

    def _new(day: str | None) -> bool:
        return bool(day) and (newest_stored is None or day > newest_stored)

    findings: list[dict] = []
    paired: set[str] = set()
    for day in sorted(d for d in removed_on if d):
        olds, news = removed_on[day], added_on.get(day, [])
        if len(olds) == 1 and len(news) == 1:
            old, new = olds[0], news[0]
            paired.update({old, new})
            findings.append({
                "kind": "reticker_candidate",
                "disagreements": [
                    f"observed {REMOVED} {old} not in reference",
                    f"observed {ADDED} {new} not in reference",
                ],
                "tickers": [old, new],
                "observed_on": day,
                "new_this_week": _new(day),
                "declaration_file": RETICKERS_FILE,
                "suggested_entry": {
                    "old": old, "new": new,
                    "effective": f"<fill in: first session as {new}, on or before {day}>",
                    "evidence": (
                        "<fill in: the issuer's or exchange's symbol-change notice; "
                        "a price-identity check of the two price_cache parquets>"
                    ),
                },
                "next_step": (
                    f"If {old} -> {new} is a symbol change, add the entry to "
                    f"{RETICKERS_FILE} before Saturday. If it is a real index "
                    f"swap, the reference table should list it; re-check it."
                ),
            })

    for text, p in parsed:
        if p is None:
            findings.append({
                "kind": "unparsed", "disagreements": [text], "tickers": [],
                "observed_on": None, "new_this_week": False,
                "declaration_file": None, "suggested_entry": None,
                "next_step": "D02 emitted a disagreement this probe cannot read; inspect it.",
            })
            continue
        side, action, ticker = p
        if side == "observed" and ticker in paired:
            continue
        day = dates.get((ticker, action)) if side == "observed" else None
        if side == "observed" and action == ADDED:
            valid = (
                (datetime.strptime(day, "%Y-%m-%d")
                 + timedelta(days=SUGGESTED_SPINOFF_VALIDITY_DAYS)).strftime("%Y-%m-%d")
                if day else "<fill in>"
            )
            findings.append({
                "kind": "spinoff_addition_candidate", "disagreements": [text],
                "tickers": [ticker], "observed_on": day, "new_this_week": _new(day),
                "declaration_file": SPINOFFS_FILE,
                "suggested_entry": {
                    "ticker": ticker, "parent": "<fill in: parent ticker>",
                    "effective": f"<fill in: distribution date, within "
                                 f"{hc.SPINOFF_MATCH_WINDOW_DAYS}d of {day}>",
                    "valid_through": valid,
                    "evidence": "<fill in: S&P Dow Jones Indices announcement and URL>",
                },
                "next_step": (
                    f"If {ticker} joined as a spin-off, add the entry to "
                    f"{SPINOFFS_FILE} before Saturday. If it is a reticker whose "
                    f"old symbol D02 already counts as removed, it belongs in "
                    f"{RETICKERS_FILE} instead."
                ),
            })
        elif side == "observed":
            findings.append({
                "kind": "unexplained_removal", "disagreements": [text],
                "tickers": [ticker], "observed_on": day, "new_this_week": _new(day),
                "declaration_file": RETICKERS_FILE,
                "suggested_entry": None,
                "next_step": (
                    f"{ticker} left the SPY holdings with no reference removal and "
                    f"no same-day arrival to pair it with. If it was renamed, add "
                    f"it to {RETICKERS_FILE}; otherwise the reference table lags "
                    f"and nothing in this repo explains it yet."
                ),
            })
        else:
            findings.append({
                "kind": "reference_not_observed", "disagreements": [text],
                "tickers": [ticker], "observed_on": None, "new_this_week": False,
                "declaration_file": None, "suggested_entry": None,
                "next_step": (
                    f"The reference lists {ticker} {action} but no roster snapshot "
                    f"shows it. No declaration file explains this: either the "
                    f"holdings file has not caught up or a snapshot is missing."
                ),
            })
    return findings


def probe(
    *,
    stored_sp500: dict[str, list[str]],
    stored_universe: dict[str, list[str]],
    live_sp500: list[str],
    live_universe: list[str],
    live_date: str,
    reference: list[ConstituentChange],
    prior: dict | None = None,
    closes: Callable[[str], object] | None = None,
    known_retickers: list[dict] | None = None,
    declarations: list[dict] | None = None,
) -> dict:
    """Pure core: D02's attestation over stored snapshots + today's roster.

    ``live_date`` stands in for the snapshot Saturday will write; a stored
    snapshot on the same date is replaced by the live roster.
    """
    newest_stored = max(stored_sp500) if stored_sp500 else None
    snapshots = {**stored_sp500, live_date: list(live_sp500)}
    universe = {**stored_universe, live_date: list(live_universe)}

    reference_removed = {
        c.ticker for c in reference
        if c.action == REMOVED and c.date >= hc.SNAPSHOT_CUTOVER
    }
    resolution = resolve_renames_offline(
        universe, reference_removed=reference_removed, prior=prior,
        closes=closes, known_retickers=known_retickers,
    )
    observed, _unresolved = hc.changes_from_snapshots(snapshots, resolution.renames)
    observed, _flickers = hc.suppress_flickers(observed)
    found = hc.divergences(observed, reference, since=hc.SNAPSHOT_CUTOVER, until=live_date)
    unexplained, declared = hc.declared_spinoff_exceptions(
        found, observed, as_of=live_date, declarations=declarations,
    )
    findings = classify(unexplained, observed, newest_stored=newest_stored)
    return {
        "status": "undeclared" if findings else "clear",
        "live_date": live_date,
        "newest_stored_snapshot": newest_stored,
        "n_unexplained": len(unexplained),
        "unexplained": unexplained,
        "declared_exceptions": declared,
        "findings": findings,
        # A price-identity reticker D02 would infer on its own: not a failure,
        # but the collector asks for each to be committed (see
        # infer_price_identity_renames), and committing it before Saturday
        # removes the dependency on the price cache holding the new symbol.
        "inferred_retickers": resolution.inferred,
        # Swaps Saturday would ask Polygon about; never queried here.
        "rename_checks_not_asked": resolution.deferred,
        "pending_reference_changes": hc.pending_reference_changes(reference, until=live_date),
    }


def alert_message(report: dict) -> str:
    """The alert text: each finding, its file, and the Auto-fix/Resolution lines."""
    lines = [
        f"D02 weekday reference-diff probe: {len(report['findings'])} roster change(s) "
        f"Saturday's historical_constituents would fail on, as of {report['live_date']} "
        f"(stored snapshots through {report['newest_stored_snapshot']}).",
    ]
    for f in report["findings"]:
        where = f["declaration_file"] or "no declaration file"
        lines.append(f"- {'; '.join(f['disagreements'])} -> {where}. {f['next_step']}")
    lines.append(
        "Auto-fix: none. A declaration is a human's evidenced claim; the probe "
        "only names the file and a draft entry (run with --no-alert to print them)."
    )
    lines.append(
        "Resolution: commit the declaration to nousergon-data before Saturday's "
        "weekly; this alert clears on the next weekday run once D02's comparison "
        f"agrees. Tracker: {TRACKER}."
    )
    return "\n".join(lines)


def _publish(report: dict) -> None:
    try:
        from nousergon_lib import alerts  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        logger.warning("alerts publish skipped — nousergon_lib.alerts unavailable: %s", exc)
        return
    digest = hashlib.sha256(
        json.dumps(sorted(report["unexplained"])).encode()
    ).hexdigest()[:12]
    try:
        alerts.publish(
            alert_message(report),
            # Literals, not the constants: the alert-class registry scanner reads
            # them from source text (infrastructure/overseer/alert_class_registry_drift.py).
            severity="warning",
            source="nousergon-data/validators/d02_reference_diff_probe.py",
            dedup_key=f"d02_reference_diff_{digest}",
            dedup_window_min=1440,  # one per distinct finding set per day
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("D02 reference-diff alert publish failed: %s", exc)


def run(*, bucket: str, s3=None) -> dict:
    """Gather every input read-only, then :func:`probe`. Never raises."""
    import boto3  # noqa: PLC0415

    from collectors.constituents import _fetch_ssga_membership  # noqa: PLC0415

    s3 = s3 or boto3.client("s3")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    stage = "roster_snapshots"
    try:
        rosters = hc.load_roster_snapshots(bucket, s3=s3)
        stage = "live_holdings"
        tickers, n500, _n400, weights = _fetch_ssga_membership()
        members = weights.members_by_index or {}
        live_sp500 = list(members.get("S&P 500") or tickers[:n500])
        live_universe = list(dict.fromkeys(tickers))
        stage = "reference_table"
        reference = hc.parse_changes_table(hc._fetch_changes_table()[0])
    except Exception as exc:  # noqa: BLE001 — reported as the result
        logger.exception("D02 reference-diff probe could not read %s", stage)
        return {"status": "error", "stage": stage, "error": f"{type(exc).__name__}: {exc}"}
    prior, prior_error = hc.load_prior_rename_checks(bucket, s3=s3)
    report = probe(
        stored_sp500=rosters.snapshots,
        stored_universe=rosters.universe,
        live_sp500=live_sp500,
        live_universe=live_universe,
        live_date=today,
        reference=reference,
        prior=prior,
        closes=hc.price_cache_closes(bucket, s3=s3),
    )
    report["prior_rename_checks_error"] = prior_error
    report["skipped_snapshots"] = dict(sorted(rosters.skipped.items()))
    return report


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="D02 weekday reference-diff probe (read-only)")
    parser.add_argument("--bucket", default=os.environ.get("NE_RESEARCH_BUCKET"),
                        help="research bucket (default: $NE_RESEARCH_BUCKET)")
    parser.add_argument("--no-alert", action="store_true",
                        help="report only; publish no alert")
    parser.add_argument("--out", default=None, help="also write the JSON report to this local path")
    args = parser.parse_args(argv)
    if not args.bucket:
        parser.error("--bucket or NE_RESEARCH_BUCKET is required")

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    report = run(bucket=args.bucket)
    text = json.dumps(report, indent=1, sort_keys=True, default=str)
    print(text)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text + "\n")
    if report["status"] == "error":
        return 2
    if report["status"] == "undeclared":
        if not args.no_alert:
            _publish(report)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

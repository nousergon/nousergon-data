"""
emailer.py — Completion email for data pipeline steps.

Builds subject/body/HTML and delegates the SMTP+SES dispatch to
``nousergon_lib.email_sender.send_email`` (the L4356 chokepoint).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from nousergon_lib.email_sender import send_email

log = logging.getLogger(__name__)


#: The env var carrying the Step Functions execution name down to the box.
#: ``krepis.ssm_log_capture`` exports it to the launcher, and the spot
#: launchers forward it into the workload they run over SSM.
RUN_TOKEN_ENV = "RUN_TOKEN"

#: The weekly-SF rehearsal marker: the rehearsal door names every run
#: ``rehearsal-<date>-<n>`` (alpha-engine-config-I12351), the same prefix the
#: SF's own rehearsal isolation and crucible-evaluator key on.
REHEARSAL_PREFIX = "rehearsal-"


def rehearsal_execution(env: dict | None = None) -> str | None:
    """The rehearsal's execution name when this process runs under one, else ``None``."""
    token = ((os.environ if env is None else env).get(RUN_TOKEN_ENV) or "").strip()
    return token if token.startswith(REHEARSAL_PREFIX) else None


def send_step_email(step_name: str, results: dict, date_str: str) -> bool:
    """Send a completion email for a pipeline step. Never raises.

    Under a weekly-SF rehearsal the email is NOT sent. A rehearsal publishes
    its SNS alerts to the muted topic, but this email goes straight to the
    operator over SMTP/SES, so nothing the SF's ``sns_topic_arn`` says ever
    reached it: rehearsal-2026-10-16-2 put a "RAG Ingestion | DEGRADED" email
    in the operator's inbox. The would-be email is written to the run log
    instead (which ``ssm_log_capture`` ships to S3), and the call returns
    ``True`` because nothing failed.
    """
    try:
        subject, html_body, plain_body = _build_email(step_name, results, date_str)
    except Exception as exc:
        log.warning("Failed to build step email: %s", exc)
        return False
    rehearsal = rehearsal_execution()
    if rehearsal:
        log.warning(
            "Step email suppressed: %s is a weekly-SF rehearsal, and a rehearsal "
            "does not reach the operator's inbox. Subject: %r\n%s",
            rehearsal, subject, plain_body,
        )
        return True
    return send_email(subject, plain_body, html=html_body)


def _build_email(step_name: str, results: dict, date_str: str) -> tuple[str, str, str]:
    """Build subject, HTML body, and plain text body from results dict."""
    status = results.get("status", "unknown").upper()
    phase = results.get("phase", "")
    collectors = results.get("collectors", {})

    # Duration
    duration_str = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        secs = (end - start).total_seconds()
        if secs >= 60:
            duration_str = f"{secs / 60:.1f} min"
        else:
            duration_str = f"{secs:.0f}s"
    except Exception:
        # CARVE-OUT (alpha-engine-config-I10226): (a) failure mode swallowed —
        # `results["started_at"]`/`["completed_at"]` missing or unparseable.
        # (c) no recording surface — this only formats a cosmetic duration
        # substring for the notification email body/subject; it writes no
        # data and feeds no downstream consumer, so a blank `duration_str` is
        # the correct degraded rendering, not a corruption. See
        # `.debug-swallow-allowlist.yaml`.
        pass

    # DEGRADED (alpha-engine-config-I11472): the run completed but a source
    # returned nothing — distinct from both OK and FAILED.
    status_icon = status if status in ("OK", "PARTIAL", "DEGRADED") else "FAILED"
    subject = f"Alpha Engine {step_name} | {date_str} | {status_icon}"

    # Build collector rows
    collector_rows_html = ""
    collector_rows_plain = ""
    for name, info in collectors.items():
        c_status = info.get("status", "unknown")
        c_color = "#2e7d32" if c_status in ("ok", "ok_dry_run", "expected_empty") else "#c62828"
        error = info.get("error", "")

        # Extract useful metrics from collector results
        details = _extract_details(name, info)

        collector_rows_html += (
            f'<tr>'
            f'<td style="padding:4px 10px; border-bottom:1px solid #eee; font-weight:bold;">{name}</td>'
            f'<td style="padding:4px 10px; border-bottom:1px solid #eee; color:{c_color}; font-weight:bold;">{c_status}</td>'
            f'<td style="padding:4px 10px; border-bottom:1px solid #eee; font-size:12px;">{details}</td>'
            f'</tr>'
        )
        if error:
            collector_rows_html += (
                f'<tr><td colspan="3" style="padding:2px 10px 6px 20px; color:#c62828; font-size:11px;">'
                f'Error: {error}</td></tr>'
            )

        collector_rows_plain += f"  {name:<20} {c_status:<10} {details}\n"
        if error:
            collector_rows_plain += f"    Error: {error}\n"

    html_body = (
        f'<html><body style="font-family: -apple-system, Arial, sans-serif; max-width:600px; margin:0 auto;">'
        f'<h2 style="margin-bottom:4px;">Alpha Engine {step_name} — {date_str}</h2>'
        f'<p style="margin-top:0; font-size:13px; color:#666;">'
        f'Status: <b style="color:{"#2e7d32" if status == "OK" else "#c62828"}">{status}</b>'
        f'{f" | Duration: <b>{duration_str}</b>" if duration_str else ""}'
        f'</p>'
        f'<table style="border-collapse:collapse; width:100%; font-size:13px;">'
        f'<tr style="background:#f5f5f5;">'
        f'<th style="padding:6px 10px; text-align:left;">Collector</th>'
        f'<th style="padding:6px 10px; text-align:left;">Status</th>'
        f'<th style="padding:6px 10px; text-align:left;">Details</th>'
        f'</tr>'
        f'{collector_rows_html}'
        f'</table>'
        f'</body></html>'
    )

    plain_body = (
        f"Alpha Engine {step_name} — {date_str}\n"
        f"Status: {status}"
        f"{f'  Duration: {duration_str}' if duration_str else ''}\n\n"
        f"{'Collector':<20} {'Status':<10} Details\n"
        f"{'-' * 60}\n"
        f"{collector_rows_plain}"
    )

    return subject, html_body, plain_body


def _extract_details(name: str, info: dict) -> str:
    """Extract human-readable details from a collector result."""
    parts = []

    if "tickers_refreshed" in info:
        parts.append(f"{info['tickers_refreshed']} refreshed")
    elif "refreshed" in info:
        parts.append(f"{info['refreshed']} refreshed")
    if "stale" in info:
        parts.append(f"{info['stale']} stale")
    if "tickers_skipped" in info:
        parts.append(f"{info['tickers_skipped']} skipped")
    if "tickers_failed" in info:
        n = info["tickers_failed"]
        if n > 0:
            parts.append(f"{n} failed")
    elif "failed" in info:
        n = info["failed"]
        if n > 0:
            parts.append(f"{n} failed")
    if "total_tickers" in info:
        parts.append(f"{info['total_tickers']} total")
    elif "total" in info:
        parts.append(f"{info['total']} total")
    if "n_tickers" in info:
        parts.append(f"{info['n_tickers']} tickers")
    if "written" in info:
        parts.append(f"{info['written']} written")
    if "sp500_count" in info:
        parts.append(f"S&P500: {info['sp500_count']}")
    if "sp400_count" in info:
        parts.append(f"S&P400: {info['sp400_count']}")
    if "series_count" in info:
        parts.append(f"{info['series_count']} series")
    if "n_dates" in info:
        parts.append(f"{info['n_dates']} dates")
    if "elapsed_s" in info:
        parts.append(f"{info['elapsed_s']:.0f}s")

    # Validation results (from price/volume/gap checks)
    val = info.get("validation", {})
    if val:
        n_anom = val.get("anomalies", 0)
        n_total = val.get("total_validated", 0)
        if n_anom > 0:
            parts.append(f"⚠ {n_anom}/{n_total} anomalies")
        else:
            parts.append(f"✓ {n_total} validated")

    return " | ".join(parts) if parts else ""

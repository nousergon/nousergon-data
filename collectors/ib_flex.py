"""IB Flex statement collector for the performance tracker (alpha-engine-config-I12806).

The perf tracker has no machine-readable IB statement to check its NAV against, and the
ledger has no source for external flows (deposits, withdrawals, in-kind transfers), so every
tracker cell carries ``external_flows_not_journaled``. IB's Flex Web Service is the
authoritative read-only source for both. This module fetches one Activity Flex query and
writes two objects:

  * ``trades/ib_statements/raw/{to_date}.xml`` — the statement exactly as IB returned it,
    written FIRST, so a parse defect never loses the source;
  * ``trades/ib_statements/dated/{to_date}.json`` — the parsed sections the consumers read:
    NAV per report date (``EquitySummaryInBase``), ``ChangeInNAV``, ``CashTransactions``
    (DETAIL rows only, so a SUMMARY row never double-counts a flow), ``Transfers`` and
    ``CashReport``.

Design: the 2026-10-30 comment on alpha-engine-config-I12806. The query is the trading
account only, XML, period ``LastBusinessDay``. Credentials resolve at run time from SSM
(``TOKEN_PARAM`` / ``QUERY_ID_PARAM``); nothing here holds a value, and this repo is public,
so logs carry counts and dates only — never an amount, an account id, the token or a URL
that embeds it.

This slice is code only. No workflow calls it yet: the scheduled job lands with the IAM
grant that lets its role read the two parameters (red line 1, a separate ops PR).

Pure stdlib for the HTTP and XML (``urllib`` + ``xml.etree``); boto3 only for S3/SSM.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

logger = logging.getLogger("ib_flex")

DEFAULT_BUCKET = "alpha-engine-research"
RAW_PREFIX = "trades/ib_statements/raw/"
DATED_PREFIX = "trades/ib_statements/dated/"
SCHEMA_VERSION = 1

TOKEN_PARAM = "/alpha-engine/ib_flex/token"
QUERY_ID_PARAM = "/alpha-engine/ib_flex/trading_query_id"

# Flex Web Service v3. GetStatement's URL comes back in SendRequest's response.
SEND_REQUEST_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest"
FLEX_VERSION = "3"
# IB answers 1019 while a statement is still being generated: poll, do not fail.
GENERATING_CODE = "1019"

_HTTP_TIMEOUT_S = 30
_TRANSPORT_ATTEMPTS = 3
_TRANSPORT_BACKOFF_S = (2.0, 6.0)
_POLL_ATTEMPTS = 10
_POLL_DELAY_S = 5.0
# A generic product UA. Never a person's name or address (fleet rule).
_UA = {"User-Agent": "nousergon-ib-flex/1.0"}

# CashTransaction types that are external flows into or out of the account.
FLOW_TYPES = frozenset({"Deposits/Withdrawals"})

# Numeric attributes kept per section. Anything else is carried verbatim as a string only
# where a consumer needs it (dates, types, currency), so the artifact stays small and typed.
_NAV_ATTRS = {
    "cash": "cash",
    "stock": "stock",
    "options": "options",
    "funds": "funds",
    "interest_accruals": "interestAccruals",
    "dividend_accruals": "dividendAccruals",
    "total": "total",
}
_CHANGE_IN_NAV_ATTRS = {
    "starting_value": "startingValue",
    "mtm": "mtm",
    "realized": "realized",
    "deposits_withdrawals": "depositsWithdrawals",
    "asset_transfers": "assetTransfers",
    "dividends": "dividends",
    "withholding_tax": "withholdingTax",
    "interest": "interest",
    "commissions": "commissions",
    "other_fees": "otherFees",
    "other": "other",
    "ending_value": "endingValue",
}


class IbFlexError(RuntimeError):
    """Any Flex Web Service failure. The message never carries the token or a URL."""


# ── parsing ─────────────────────────────────────────────────────────────────────────


def _tag(e: ET.Element) -> str:
    return e.tag.split("}")[-1]


def _num(value: str | None) -> float | None:
    """A Flex numeric attribute → float. Blank or absent → None (never 0.0: a missing
    amount and a zero amount are different facts for a flow ledger)."""
    if value is None or value.strip() == "":
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def _iso(value: str | None) -> str | None:
    """A Flex date (``YYYYMMDD``, ``YYYYMMDD;HHMMSS``, ``YYYY-MM-DD``…) → ``YYYY-MM-DD``."""
    if not value:
        return None
    head = value.strip().split(";")[0].split(" ")[0].replace("-", "")
    if len(head) >= 8 and head[:8].isdigit():
        try:
            return date(int(head[:4]), int(head[4:6]), int(head[6:8])).isoformat()
        except ValueError:
            return None
    return None


def parse_flex_response(xml_text: str) -> tuple[str, str, str, str]:
    """Parse a SendRequest/GetStatement status envelope → ``(status, error_code, reference, url)``.

    Absent elements read as ``""``.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        raise IbFlexError("Flex status envelope is not XML") from None
    return tuple(  # type: ignore[return-value]
        (root.findtext(name) or "").strip() for name in ("Status", "ErrorCode", "ReferenceCode", "Url")
    )


def parse_statement(xml_text: str) -> dict[str, Any]:
    """Parse an Activity Flex statement into the ``dated`` artifact body.

    Raises :class:`IbFlexError` when the document is not a statement (an error envelope,
    or a response with no ``FlexStatement``), so a bad fetch can never publish an empty
    artifact that reads as "no flows".
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise IbFlexError(f"statement is not well-formed XML ({e.__class__.__name__})") from None
    if _tag(root) != "FlexQueryResponse":
        code = (root.findtext("ErrorCode") or "").strip()
        raise IbFlexError(f"not a FlexQueryResponse (root={_tag(root)}, error_code={code or 'none'})")
    statements = [e for e in root.iter() if _tag(e) == "FlexStatement"]
    if not statements:
        raise IbFlexError("FlexQueryResponse carries no FlexStatement")

    accounts: list[dict[str, Any]] = []
    for st in statements:
        acct = st.get("accountId", "") or ""
        nav: list[dict[str, Any]] = []
        change: list[dict[str, Any]] = []
        cash_tx: list[dict[str, Any]] = []
        transfers: list[dict[str, Any]] = []
        cash_report: list[dict[str, Any]] = []
        for e in st.iter():
            t = _tag(e)
            if t == "EquitySummaryByReportDateInBase":
                row: dict[str, Any] = {"report_date": _iso(e.get("reportDate"))}
                for key, attr in _NAV_ATTRS.items():
                    row[key] = _num(e.get(attr))
                nav.append(row)
            elif t == "ChangeInNAV":
                row = {"from_date": _iso(e.get("fromDate")), "to_date": _iso(e.get("toDate"))}
                for key, attr in _CHANGE_IN_NAV_ATTRS.items():
                    row[key] = _num(e.get(attr))
                change.append(row)
            elif t == "CashTransaction":
                # SUMMARY rows restate the DETAIL rows per currency; keep DETAIL only.
                if (e.get("levelOfDetail") or "DETAIL").upper() != "DETAIL":
                    continue
                amount = _num(e.get("amount"))
                fx = _num(e.get("fxRateToBase"))
                cash_tx.append({
                    "type": e.get("type", "") or "",
                    "report_date": _iso(e.get("reportDate")),
                    "settle_date": _iso(e.get("settleDate")),
                    "date_time": _iso(e.get("dateTime")),
                    "currency": e.get("currency", "") or "",
                    "amount": amount,
                    "fx_rate_to_base": fx,
                    "amount_base": None if amount is None else amount * (fx if fx is not None else 1.0),
                    "transaction_id": e.get("transactionID", "") or "",
                    "is_external_flow": (e.get("type") or "") in FLOW_TYPES,
                })
            elif t == "Transfer":
                transfers.append({
                    "type": e.get("type", "") or "",
                    "direction": e.get("direction", "") or "",
                    "date": _iso(e.get("date") or e.get("reportDate")),
                    "currency": e.get("currency", "") or "",
                    "symbol": e.get("symbol", "") or "",
                    "quantity": _num(e.get("quantity")),
                    "position_amount_in_base": _num(e.get("positionAmountInBase")),
                    "cash_transfer": _num(e.get("cashTransfer")),
                    "transaction_id": e.get("transactionID", "") or "",
                })
            elif t == "CashReportCurrency":
                cash_report.append({
                    "currency": e.get("currency", "") or "",
                    "from_date": _iso(e.get("fromDate")),
                    "to_date": _iso(e.get("toDate")),
                    "starting_cash": _num(e.get("startingCash")),
                    "deposits": _num(e.get("deposits")),
                    "withdrawals": _num(e.get("withdrawals")),
                    "deposit_withdrawals": _num(e.get("depositWithdrawals")),
                    "ending_cash": _num(e.get("endingCash")),
                })
        nav.sort(key=lambda r: r["report_date"] or "")
        accounts.append({
            "account_id": acct,
            "from_date": _iso(st.get("fromDate")),
            "to_date": _iso(st.get("toDate")),
            "period": st.get("period", "") or "",
            "when_generated": st.get("whenGenerated", "") or "",
            "nav": nav,
            "change_in_nav": change,
            "cash_transactions": cash_tx,
            "transfers": transfers,
            "cash_report": cash_report,
        })

    to_dates = [a["to_date"] for a in accounts if a["to_date"]]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "ib_flex_web_service_v3",
        "to_date": max(to_dates) if to_dates else None,
        "accounts": accounts,
    }


def summarize(body: dict[str, Any]) -> dict[str, Any]:
    """Counts and dates only — the one shape this public repo may log."""
    accts = body.get("accounts", [])
    return {
        "n_accounts": len(accts),
        "to_date": body.get("to_date"),
        "n_nav_rows": sum(len(a["nav"]) for a in accts),
        "n_cash_transactions": sum(len(a["cash_transactions"]) for a in accts),
        "n_external_flows": sum(1 for a in accts for c in a["cash_transactions"] if c["is_external_flow"]),
        "n_transfers": sum(len(a["transfers"]) for a in accts),
        "first_nav_date": min((r["report_date"] for a in accts for r in a["nav"] if r["report_date"]), default=None),
        "last_nav_date": max((r["report_date"] for a in accts for r in a["nav"] if r["report_date"]), default=None),
    }


# ── Flex Web Service ────────────────────────────────────────────────────────────────


def _http_get(url: str, sleep: Callable[[float], None] = time.sleep) -> str:
    """GET with a short transport retry ladder. Errors never echo ``url`` (it holds the token)."""
    last: str = ""
    for attempt in range(_TRANSPORT_ATTEMPTS):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_S) as resp:  # noqa: S310 - fixed https host
                return resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e.__class__.__name__
        if attempt < len(_TRANSPORT_BACKOFF_S):
            sleep(_TRANSPORT_BACKOFF_S[attempt])
    raise IbFlexError(f"Flex transport failed after {_TRANSPORT_ATTEMPTS} attempts ({last})")


def send_request(
    token: str,
    query_id: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    http_get: Callable[[str], str] = _http_get,
) -> tuple[str, str]:
    """SendRequest → ``(reference_code, get_statement_url)``.

    ``from_date``/``to_date`` (``YYYYMMDD``) override the query's saved period via the
    ``fd``/``td`` parameters. IB caps one Activity request at 365 days. The override is
    used only for the one-off inception backfill; verify it on that first live run, and if
    IB rejects it, use a second saved query whose period is ``Last365CalendarDays``.
    """
    params = {"t": token, "q": query_id, "v": FLEX_VERSION}
    if from_date:
        params["fd"] = from_date
    if to_date:
        params["td"] = to_date
    body = http_get(SEND_REQUEST_URL + "?" + urllib.parse.urlencode(params))
    status, code, ref, url = parse_flex_response(body)
    if status != "Success" or not ref:
        raise IbFlexError(f"SendRequest refused (status={status or 'none'}, error_code={code or 'none'})")
    if not url.startswith("https://"):
        raise IbFlexError("SendRequest returned no https GetStatement URL")
    return ref, url


def get_statement(
    token: str,
    reference_code: str,
    url: str,
    *,
    http_get: Callable[[str], str] = _http_get,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """GetStatement, polling while IB reports 1019 (statement generating)."""
    params = urllib.parse.urlencode({"q": reference_code, "t": token, "v": FLEX_VERSION})
    for _ in range(_POLL_ATTEMPTS):
        body = http_get(f"{url}?{params}")
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            raise IbFlexError("GetStatement returned non-XML") from None
        if _tag(root) == "FlexQueryResponse":
            return body
        code = (root.findtext("ErrorCode") or "").strip()
        if code != GENERATING_CODE:
            raise IbFlexError(f"GetStatement refused (error_code={code or 'none'})")
        sleep(_POLL_DELAY_S)
    raise IbFlexError(f"statement still generating after {_POLL_ATTEMPTS} polls")


def fetch_statement(token: str, query_id: str, **kwargs: Any) -> str:
    http_get = kwargs.pop("http_get", _http_get)
    sleep = kwargs.pop("sleep", time.sleep)
    ref, url = send_request(token, query_id, http_get=http_get, **kwargs)
    return get_statement(token, ref, url, http_get=http_get, sleep=sleep)


# ── credentials and S3 ──────────────────────────────────────────────────────────────


def _ssm_credentials(ssm_client: Any) -> tuple[str, str]:
    token = ssm_client.get_parameter(Name=TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
    query_id = ssm_client.get_parameter(Name=QUERY_ID_PARAM)["Parameter"]["Value"]
    if not token or not query_id:
        raise IbFlexError("Flex token or query id parameter is empty")
    return token, query_id


def collect(
    *,
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    credentials: Callable[[], tuple[str, str]] | None = None,
    fetch: Callable[..., str] = fetch_statement,
    from_date: str | None = None,
    to_date: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Fetch, write raw, parse, write dated. Returns a counts-only result dict."""
    now = now or datetime.now(UTC)
    if credentials is None:
        import boto3

        ssm = boto3.client("ssm", region_name="us-east-1")
        credentials = lambda: _ssm_credentials(ssm)  # noqa: E731
    token, query_id = credentials()
    xml_text = fetch(token, query_id, from_date=from_date, to_date=to_date)

    # Name the raw object from the statement itself when it parses, else from the run day,
    # so an unparseable statement is still kept for forensics.
    try:
        body = parse_statement(xml_text)
        parse_error: IbFlexError | None = None
    except IbFlexError as e:
        body, parse_error = {}, e
    key_date = (body.get("to_date") if body else None) or now.date().isoformat()
    raw_key = f"{RAW_PREFIX}{key_date}.xml"
    dated_key = f"{DATED_PREFIX}{key_date}.json"

    if dry_run:
        if parse_error:
            raise parse_error
        return {"status": "dry-run", **summarize(body)}

    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=raw_key, Body=xml_text.encode("utf-8"), ContentType="application/xml")
    if parse_error:
        logger.error("[ib_flex] raw kept at s3://%s/%s; parse failed: %s", bucket, raw_key, parse_error)
        raise parse_error

    body["fetched_utc"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    body["raw_key"] = raw_key
    s3_client.put_object(
        Bucket=bucket,
        Key=dated_key,
        Body=json.dumps(body, indent=1, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    summary = summarize(body)
    logger.info("[ib_flex] wrote s3://%s/%s (%s)", bucket, dated_key, json.dumps(summary, sort_keys=True))
    return {"status": "ok", "dated_key": dated_key, **summary}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="python -m collectors.ib_flex", description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--from-date", help="YYYYMMDD; inception backfill only (IB caps a request at 365 days)")
    parser.add_argument("--to-date", help="YYYYMMDD; with --from-date")
    parser.add_argument("--dry-run", action="store_true", help="fetch and parse, write nothing")
    args = parser.parse_args(argv)
    try:
        result = collect(bucket=args.bucket, from_date=args.from_date, to_date=args.to_date, dry_run=args.dry_run)
    except IbFlexError as e:
        logger.error("[ib_flex] %s", e)
        return 1
    logger.info("[ib_flex] %s", json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

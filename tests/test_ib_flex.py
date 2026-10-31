"""IB Flex statement collector (alpha-engine-config-I12806).

Synthetic XML only: this repo is public, and no real statement, account id or amount may
appear in it. Covers the parse (NAV, ChangeInNAV, DETAIL-only cash transactions, flows,
transfers), the refusal paths (error envelopes, no statement, non-XML), the 1019 poll, the
raw-before-dated write order, the keep-raw-on-parse-failure path, and that no log line or
error carries the token.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from collectors import ib_flex as fx

_TOKEN = "SYNTHETIC-TOKEN-0000"
_QUERY = "999999"
_NOW = datetime(2026, 11, 3, 10, 30, tzinfo=UTC)

_STATEMENT = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="synthetic" type="AF">
 <FlexStatements count="1">
  <FlexStatement accountId="U0000000" fromDate="20261029" toDate="20261030" period="LastBusinessDay" whenGenerated="20261031;083000">
   <EquitySummaryInBase>
    <EquitySummaryByReportDateInBase accountId="U0000000" currency="USD" reportDate="20261030" cash="100.5" stock="900" options="" funds="0" interestAccruals="0.25" dividendAccruals="1" total="1001.75"/>
    <EquitySummaryByReportDateInBase accountId="U0000000" currency="USD" reportDate="20261029" cash="100" stock="880" total="980"/>
   </EquitySummaryInBase>
   <ChangeInNAV accountId="U0000000" fromDate="20261029" toDate="20261030" startingValue="980" mtm="20" realized="0" depositsWithdrawals="0" assetTransfers="" dividends="1" withholdingTax="-0.15" interest="0.5" commissions="-0.35" otherFees="-0.25" other="0" endingValue="1001.75"/>
   <CashTransactions>
    <CashTransaction accountId="U0000000" currency="USD" fxRateToBase="1" type="Deposits/Withdrawals" amount="500" dateTime="20261030" reportDate="20261030" settleDate="20261030" transactionID="T1" levelOfDetail="DETAIL"/>
    <CashTransaction accountId="U0000000" currency="EUR" fxRateToBase="1.1" type="Broker Interest Received" amount="2" dateTime="20261030;120000" reportDate="20261030" settleDate="20261030" transactionID="T2" levelOfDetail="DETAIL"/>
    <CashTransaction accountId="U0000000" currency="USD" fxRateToBase="1" type="Deposits/Withdrawals" amount="500" reportDate="20261030" levelOfDetail="SUMMARY"/>
   </CashTransactions>
   <Transfers>
    <Transfer accountId="U0000000" type="ACATS" direction="IN" date="20261030" currency="USD" symbol="SYN" quantity="10" positionAmountInBase="1000" cashTransfer="" transactionID="X1"/>
   </Transfers>
   <CashReport>
    <CashReportCurrency accountId="U0000000" currency="BASE_SUMMARY" fromDate="20261029" toDate="20261030" startingCash="100" deposits="500" withdrawals="0" depositWithdrawals="500" endingCash="600.5"/>
   </CashReport>
  </FlexStatement>
 </FlexStatements>
</FlexQueryResponse>
"""

_SEND_OK = """<FlexStatementResponse timestamp="31 October, 2026 08:30 AM EDT">
<Status>Success</Status><ReferenceCode>1234567890</ReferenceCode>
<Url>https://gdcdyn.example.invalid/GetStatement</Url></FlexStatementResponse>"""
_GENERATING = """<FlexStatementResponse><Status>Warn</Status><ErrorCode>1019</ErrorCode>
<ErrorMessage>Statement generation in progress.</ErrorMessage></FlexStatementResponse>"""
_BAD_TOKEN = """<FlexStatementResponse><Status>Fail</Status><ErrorCode>1015</ErrorCode>
<ErrorMessage>Token is invalid.</ErrorMessage></FlexStatementResponse>"""


# ── parse ───────────────────────────────────────────────────────────────────────────


def test_parse_nav_is_sorted_by_report_date_and_typed():
    body = fx.parse_statement(_STATEMENT)
    (acct,) = body["accounts"]
    assert body["to_date"] == "2026-10-30"
    assert [r["report_date"] for r in acct["nav"]] == ["2026-10-29", "2026-10-30"]
    last = acct["nav"][-1]
    assert last["total"] == pytest.approx(1001.75)
    assert last["interest_accruals"] == pytest.approx(0.25)
    assert last["options"] is None  # blank is missing, not zero


def test_parse_change_in_nav_keeps_flow_interest_and_fee_fields():
    (row,) = fx.parse_statement(_STATEMENT)["accounts"][0]["change_in_nav"]
    assert row["from_date"] == "2026-10-29" and row["to_date"] == "2026-10-30"
    assert row["deposits_withdrawals"] == 0.0
    assert row["interest"] == pytest.approx(0.5)
    assert row["commissions"] == pytest.approx(-0.35)
    assert row["other_fees"] == pytest.approx(-0.25)
    assert row["asset_transfers"] is None


def test_parse_cash_transactions_keep_detail_rows_only_and_mark_flows():
    rows = fx.parse_statement(_STATEMENT)["accounts"][0]["cash_transactions"]
    assert [r["transaction_id"] for r in rows] == ["T1", "T2"]  # SUMMARY dropped
    flow, interest = rows
    assert flow["is_external_flow"] is True and flow["amount_base"] == pytest.approx(500)
    assert interest["is_external_flow"] is False
    assert interest["amount_base"] == pytest.approx(2.2)
    assert interest["date_time"] == "2026-10-30"


def test_parse_transfers_and_cash_report():
    acct = fx.parse_statement(_STATEMENT)["accounts"][0]
    (tr,) = acct["transfers"]
    assert (tr["type"], tr["direction"], tr["date"]) == ("ACATS", "IN", "2026-10-30")
    assert tr["position_amount_in_base"] == pytest.approx(1000) and tr["cash_transfer"] is None
    (cr,) = acct["cash_report"]
    assert cr["currency"] == "BASE_SUMMARY" and cr["deposit_withdrawals"] == pytest.approx(500)


def test_summary_is_counts_and_dates_only():
    s = fx.summarize(fx.parse_statement(_STATEMENT))
    assert s == {
        "n_accounts": 1,
        "to_date": "2026-10-30",
        "n_nav_rows": 2,
        "n_cash_transactions": 2,
        "n_external_flows": 1,
        "n_transfers": 1,
        "first_nav_date": "2026-10-29",
        "last_nav_date": "2026-10-30",
    }
    assert "U0000000" not in json.dumps(s)


@pytest.mark.parametrize(
    "xml_text",
    [_BAD_TOKEN, "<FlexQueryResponse><FlexStatements count='0'/></FlexQueryResponse>", "not xml"],
)
def test_parse_refuses_anything_that_is_not_a_statement(xml_text):
    with pytest.raises(fx.IbFlexError):
        fx.parse_statement(xml_text)


@pytest.mark.parametrize("raw,iso", [("20261030", "2026-10-30"), ("20261030;083000", "2026-10-30"),
                                     ("2026-10-30", "2026-10-30"), ("", None), ("2026", None), ("20261340", None)])
def test_iso_dates(raw, iso):
    assert fx._iso(raw) == iso


# ── web service ─────────────────────────────────────────────────────────────────────


def test_fetch_polls_while_generating_then_returns_the_statement():
    calls: list[str] = []
    replies = iter([_SEND_OK, _GENERATING, _GENERATING, _STATEMENT])

    def http_get(url):
        calls.append(url)
        return next(replies)

    sleeps: list[float] = []
    out = fx.fetch_statement(_TOKEN, _QUERY, http_get=http_get, sleep=sleeps.append)
    assert out == _STATEMENT
    assert calls[0].startswith(fx.SEND_REQUEST_URL) and "q=999999" in calls[0] and "v=3" in calls[0]
    assert all(c.startswith("https://gdcdyn.example.invalid/GetStatement?") for c in calls[1:])
    assert "q=1234567890" in calls[1]
    assert sleeps == [fx._POLL_DELAY_S, fx._POLL_DELAY_S]


def test_send_request_passes_the_period_override_only_when_given():
    seen: list[str] = []
    fx.send_request(_TOKEN, _QUERY, from_date="20251101", to_date="20261030",
                    http_get=lambda u: seen.append(u) or _SEND_OK)
    fx.send_request(_TOKEN, _QUERY, http_get=lambda u: seen.append(u) or _SEND_OK)
    assert "fd=20251101" in seen[0] and "td=20261030" in seen[0]
    assert "fd=" not in seen[1] and "td=" not in seen[1]


def test_refusals_never_echo_the_token():
    with pytest.raises(fx.IbFlexError) as e:
        fx.send_request(_TOKEN, _QUERY, http_get=lambda u: _BAD_TOKEN)
    assert "1015" in str(e.value) and _TOKEN not in str(e.value)
    with pytest.raises(fx.IbFlexError) as e:
        fx.get_statement(_TOKEN, "1", "https://x.invalid/g", http_get=lambda u: _BAD_TOKEN, sleep=lambda s: None)
    assert _TOKEN not in str(e.value)


def test_poll_gives_up_after_the_attempt_budget():
    with pytest.raises(fx.IbFlexError, match="still generating"):
        fx.get_statement(_TOKEN, "1", "https://x.invalid/g", http_get=lambda u: _GENERATING, sleep=lambda s: None)


def test_transport_failure_is_retried_then_raised_without_the_url(monkeypatch):
    import urllib.error

    attempts = []

    def boom(req, timeout):
        attempts.append(req.full_url)
        raise urllib.error.URLError("down")

    monkeypatch.setattr(fx.urllib.request, "urlopen", boom)
    with pytest.raises(fx.IbFlexError) as e:
        fx._http_get(f"{fx.SEND_REQUEST_URL}?t={_TOKEN}", sleep=lambda s: None)
    assert len(attempts) == fx._TRANSPORT_ATTEMPTS
    assert _TOKEN not in str(e.value) and "interactivebrokers" not in str(e.value)


# ── collect ─────────────────────────────────────────────────────────────────────────


def _collect(s3, xml_text=_STATEMENT, **kw):
    return fx.collect(
        s3_client=s3,
        credentials=lambda: (_TOKEN, _QUERY),
        fetch=lambda t, q, **_: xml_text,
        now=_NOW,
        **kw,
    )


def test_collect_writes_raw_first_then_dated(caplog):
    s3 = MagicMock()
    with caplog.at_level(logging.INFO, logger="ib_flex"):
        result = _collect(s3)
    keys = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
    assert keys == ["trades/ib_statements/raw/2026-10-30.xml", "trades/ib_statements/dated/2026-10-30.json"]
    dated = json.loads(s3.put_object.call_args_list[1].kwargs["Body"])
    assert dated["raw_key"] == keys[0] and dated["fetched_utc"] == "2026-11-03T10:30:00Z"
    assert result["status"] == "ok" and result["n_external_flows"] == 1
    # public repo: no account id, amount or token in any log line
    assert "U0000000" not in caplog.text and _TOKEN not in caplog.text and "1001.75" not in caplog.text


def test_collect_keeps_raw_and_raises_when_the_statement_does_not_parse():
    s3 = MagicMock()
    with pytest.raises(fx.IbFlexError):
        _collect(s3, xml_text=_BAD_TOKEN)
    (call,) = s3.put_object.call_args_list
    assert call.kwargs["Key"] == "trades/ib_statements/raw/2026-11-03.xml"  # run day: no to_date


def test_collect_dry_run_writes_nothing():
    s3 = MagicMock()
    result = _collect(s3, dry_run=True)
    assert result["status"] == "dry-run" and result["n_nav_rows"] == 2
    s3.put_object.assert_not_called()


def test_ssm_credentials_read_the_declared_parameters_and_refuse_empty():
    ssm = MagicMock()
    ssm.get_parameter.side_effect = lambda Name, **kw: {"Parameter": {"Value": {fx.TOKEN_PARAM: _TOKEN, fx.QUERY_ID_PARAM: _QUERY}[Name]}}
    assert fx._ssm_credentials(ssm) == (_TOKEN, _QUERY)
    names = [c.kwargs["Name"] for c in ssm.get_parameter.call_args_list]
    assert names == [fx.TOKEN_PARAM, fx.QUERY_ID_PARAM]
    assert ssm.get_parameter.call_args_list[0].kwargs["WithDecryption"] is True
    ssm.get_parameter.side_effect = lambda Name, **kw: {"Parameter": {"Value": ""}}
    with pytest.raises(fx.IbFlexError):
        fx._ssm_credentials(ssm)

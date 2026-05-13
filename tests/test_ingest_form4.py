"""Tests for the SEC Form 4 (insider transactions) ingest pipeline.

Wave 1 PR B of the institutional data-revamp arc.

Covers:
  - XML parsing (single transaction, multi-transaction, derivative table)
  - Robustness against malformed XML (returns [] not crash)
  - Missing optional fields → None in the structured row
  - Transaction value computed (shares × price)
  - Form4Transaction dataclass shape
  - Parquet round-trip via in-memory S3 mock
  - Orchestrator: discovery → download → parse → write
  - Failure isolation (one bad download doesn't crash batch)
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rag.pipelines.ingest_form4 import (
    DEFAULT_S3_PREFIX,
    SCHEMA_VERSION,
    Form4Transaction,
    _get_cik,
    _search_form4_filings,
    ingest_for_tickers,
    parse_form4_xml,
    s3_key_for_filed_date,
    transactions_to_dataframe,
    write_form4_parquet,
)


# ── In-memory S3 mock (reused pattern from PR A.2 tests) ───────────────


class _InMemoryS3:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise RuntimeError("NoSuchKey")
        return {"Body": BytesIO(self.store[(Bucket, Key)])}


# ── XML fixtures ───────────────────────────────────────────────────────


_FORM4_SINGLE_SALE = """<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>1234567</rptOwnerCik>
      <rptOwnerName>Cook Timothy D</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>true</isDirector>
      <isOfficer>true</isOfficer>
      <isTenPercentOwner>false</isTenPercentOwner>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-05-13</value></transactionDate>
      <transactionCoding>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10000</value></transactionShares>
        <transactionPricePerShare><value>185.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>3300000</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


_FORM4_MULTI_TRANSACTION = """<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0000789019</issuerCik>
    <issuerName>Microsoft Corp</issuerName>
    <issuerTradingSymbol>MSFT</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>1111111</rptOwnerCik>
      <rptOwnerName>Nadella Satya</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>true</isDirector>
      <isOfficer>true</isOfficer>
      <isTenPercentOwner>false</isTenPercentOwner>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-05-10</value></transactionDate>
      <transactionCoding>
        <transactionCode>P</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5000</value></transactionShares>
        <transactionPricePerShare><value>420.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-05-10</value></transactionDate>
      <transactionCoding>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>2500</value></transactionShares>
        <transactionPricePerShare><value>421.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Stock Option (right to buy)</value></securityTitle>
      <transactionDate><value>2026-05-10</value></transactionDate>
      <transactionCoding>
        <transactionCode>M</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>250.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>
"""


_FORM4_MISSING_OPTIONAL = """<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0001234567</issuerCik>
    <issuerName>SomeCo Inc</issuerName>
    <issuerTradingSymbol>SMC</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Doe Jane</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>false</isDirector>
      <isOfficer>false</isOfficer>
      <isTenPercentOwner>true</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionCoding>
        <transactionCode>G</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


# ── XML parser tests ───────────────────────────────────────────────────


class TestParseForm4Xml:
    def test_single_sale_parsed(self):
        txs = parse_form4_xml(
            _FORM4_SINGLE_SALE,
            accession_number="0000320193-26-000001",
            filed_date=date(2026, 5, 13),
        )
        assert len(txs) == 1
        tx = txs[0]
        assert tx.ticker == "AAPL"
        assert tx.issuer_cik == "0000320193"
        assert tx.reporting_owner_name == "Cook Timothy D"
        assert tx.reporting_owner_cik == "1234567"
        assert tx.is_director is True
        assert tx.is_officer is True
        assert tx.is_ten_percent_owner is False
        assert tx.officer_title == "CEO"
        assert tx.transaction_code == "S"
        assert tx.transaction_shares == 10000.0
        assert tx.transaction_price_per_share == 185.50
        assert tx.acquired_disposed_code == "D"
        assert tx.transaction_value_usd == 1_855_000.00
        assert tx.shares_owned_after == 3_300_000.0
        assert tx.direct_or_indirect == "D"
        assert tx.is_derivative is False
        assert tx.schema_version == SCHEMA_VERSION
        assert tx.transaction_date == date(2026, 5, 13)

    def test_multi_transaction_yields_multiple_rows(self):
        txs = parse_form4_xml(
            _FORM4_MULTI_TRANSACTION,
            accession_number="0000789019-26-000002",
            filed_date=date(2026, 5, 11),
        )
        # 2 non-derivative + 1 derivative = 3 rows
        assert len(txs) == 3
        non_deriv = [t for t in txs if not t.is_derivative]
        deriv = [t for t in txs if t.is_derivative]
        assert len(non_deriv) == 2
        assert len(deriv) == 1
        # Multi-transaction filings share issuer + reporting owner
        assert {t.ticker for t in txs} == {"MSFT"}
        assert {t.reporting_owner_name for t in txs} == {"Nadella Satya"}
        # The derivative row is flagged
        assert deriv[0].security_title == "Stock Option (right to buy)"
        assert deriv[0].transaction_code == "M"

    def test_missing_optional_fields_set_to_none_or_default(self):
        txs = parse_form4_xml(
            _FORM4_MISSING_OPTIONAL,
            accession_number="0001234567-26-000001",
            filed_date=date(2026, 5, 13),
        )
        assert len(txs) == 1
        tx = txs[0]
        # No transaction_date in the XML → None
        assert tx.transaction_date is None
        # No price → None + transaction_value_usd also None
        assert tx.transaction_price_per_share is None
        assert tx.transaction_value_usd is None
        # No shares_owned_after → None
        assert tx.shares_owned_after is None
        # 10%-owner gift, indirect ownership
        assert tx.is_ten_percent_owner is True
        assert tx.is_director is False
        assert tx.is_officer is False
        assert tx.officer_title == ""
        assert tx.direct_or_indirect == "I"
        assert tx.transaction_code == "G"
        # reporting_owner_cik missing → None
        assert tx.reporting_owner_cik is None

    def test_malformed_xml_returns_empty_list(self):
        txs = parse_form4_xml(
            "<not-valid-xml",
            accession_number="x",
            filed_date=date(2026, 5, 13),
        )
        assert txs == []

    def test_empty_filing_returns_empty_list(self):
        xml = """<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0000123</issuerCik>
    <issuerName>X</issuerName>
    <issuerTradingSymbol>X</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Jane Doe</rptOwnerName>
    </reportingOwnerId>
  </reportingOwner>
</ownershipDocument>
"""
        txs = parse_form4_xml(
            xml, accession_number="x", filed_date=date(2026, 5, 13),
        )
        assert txs == []


# ── DataFrame conversion ───────────────────────────────────────────────


class TestDataFrameConversion:
    def test_empty_list_returns_empty_df_with_schema(self):
        df = transactions_to_dataframe([])
        assert len(df) == 0
        for col in Form4Transaction.__dataclass_fields__:
            assert col in df.columns

    def test_round_trip_preserves_fields(self):
        txs = parse_form4_xml(
            _FORM4_SINGLE_SALE,
            accession_number="abc-001",
            filed_date=date(2026, 5, 13),
        )
        df = transactions_to_dataframe(txs)
        assert len(df) == 1
        row = df.iloc[0]
        assert row["ticker"] == "AAPL"
        assert row["transaction_value_usd"] == 1_855_000.00
        assert row["schema_version"] == SCHEMA_VERSION


# ── S3 parquet writer ─────────────────────────────────────────────────


class TestS3ParquetWriter:
    def test_s3_key_format(self):
        assert (
            s3_key_for_filed_date(date(2026, 5, 13))
            == "data/insider_transactions/2026-05-13.parquet"
        )

    def test_write_and_read_back(self):
        s3 = _InMemoryS3()
        txs = parse_form4_xml(
            _FORM4_SINGLE_SALE,
            accession_number="abc-001",
            filed_date=date(2026, 5, 13),
        )
        key = write_form4_parquet(
            txs, filed_date=date(2026, 5, 13), s3_client=s3,
        )
        assert key == "data/insider_transactions/2026-05-13.parquet"

        body = s3.store[("alpha-engine-research", key)]
        df = pd.read_parquet(BytesIO(body), engine="pyarrow")
        assert len(df) == 1
        assert df.iloc[0]["reporting_owner_name"] == "Cook Timothy D"

    def test_empty_transactions_still_writes_schema_parquet(self):
        s3 = _InMemoryS3()
        key = write_form4_parquet(
            [], filed_date=date(2026, 5, 13), s3_client=s3,
        )
        body = s3.store[("alpha-engine-research", key)]
        df = pd.read_parquet(BytesIO(body), engine="pyarrow")
        assert len(df) == 0
        # Empty parquet still has canonical schema columns
        for col in Form4Transaction.__dataclass_fields__:
            assert col in df.columns


# ── Orchestrator (discovery → download → parse → write) ───────────────


class TestIngestForTickers:
    def _make_http_mock(self, *, form4_list, xml_by_url):
        """Build a MagicMock http object that responds to:
          - GET /files/company_tickers.json → returns ticker→CIK map
          - GET /submissions/CIK*.json → returns form4_list
          - GET Archives URL → returns xml_by_url[url]
        """
        http = MagicMock()

        def get(url, headers=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 200
            if "company_tickers.json" in url:
                resp.json.return_value = {
                    "0": {"ticker": "AAPL", "cik_str": 320193},
                }
                return resp
            if "submissions/CIK" in url:
                # Build the recent-filings shape EDGAR returns
                resp.json.return_value = {
                    "filings": {
                        "recent": {
                            "form": [f["form_type"] for f in form4_list],
                            "accessionNumber": [
                                f["accession_number"] for f in form4_list
                            ],
                            "filingDate": [
                                f["filed_date"].isoformat() for f in form4_list
                            ],
                            "primaryDocument": [
                                f["primary"] for f in form4_list
                            ],
                        },
                    },
                }
                return resp
            # Archives URL — match by URL substring
            for key, xml in xml_by_url.items():
                if key in url:
                    resp.text = xml
                    return resp
            resp.status_code = 404
            return resp

        http.get.side_effect = get
        return http

    def test_end_to_end_writes_parquet_per_filed_date(self, monkeypatch):
        # Clear CIK cache so http mock is consulted
        from rag.pipelines import ingest_form4
        monkeypatch.setattr(ingest_form4, "_CIK_CACHE", {})
        # Disable rate-limit sleeps
        monkeypatch.setattr(
            ingest_form4, "_INTER_REQUEST_SLEEP_SECONDS", 0,
        )

        s3 = _InMemoryS3()
        form4_list = [
            {
                "form_type": "4",
                "accession_number": "0000320193-26-000001",
                "filed_date": date(2026, 5, 13),
                "primary": "form4.xml",
            },
        ]
        http = self._make_http_mock(
            form4_list=form4_list,
            xml_by_url={"form4.xml": _FORM4_SINGLE_SALE},
        )
        stats = ingest_for_tickers(
            ["AAPL"],
            lookback_days=90,
            s3_client=s3,
            http=http,
        )
        assert stats["n_filings_discovered"] == 1
        assert stats["n_filings_downloaded"] == 1
        assert stats["n_transactions_parsed"] == 1
        assert stats["n_parquet_writes"] == 1
        assert stats["n_failures"] == 0
        assert (
            "alpha-engine-research",
            "data/insider_transactions/2026-05-13.parquet",
        ) in s3.store

    def test_non_form4_filings_skipped_in_discovery(self, monkeypatch):
        from rag.pipelines import ingest_form4
        monkeypatch.setattr(ingest_form4, "_CIK_CACHE", {})
        monkeypatch.setattr(
            ingest_form4, "_INTER_REQUEST_SLEEP_SECONDS", 0,
        )

        # Discovery payload has a mix of forms — only the "4" should be kept
        form4_list = [
            {
                "form_type": "10-K",  # not form 4
                "accession_number": "0000320193-26-100001",
                "filed_date": date(2026, 5, 1),
                "primary": "10k.htm",
            },
            {
                "form_type": "4",
                "accession_number": "0000320193-26-100002",
                "filed_date": date(2026, 5, 13),
                "primary": "form4.xml",
            },
        ]
        http = self._make_http_mock(
            form4_list=form4_list,
            xml_by_url={"form4.xml": _FORM4_SINGLE_SALE},
        )
        s3 = _InMemoryS3()
        stats = ingest_for_tickers(
            ["AAPL"], lookback_days=90, s3_client=s3, http=http,
        )
        assert stats["n_filings_discovered"] == 1  # only the form 4
        assert stats["n_transactions_parsed"] == 1

    def test_dry_run_skips_s3_writes(self, monkeypatch):
        from rag.pipelines import ingest_form4
        monkeypatch.setattr(ingest_form4, "_CIK_CACHE", {})
        monkeypatch.setattr(
            ingest_form4, "_INTER_REQUEST_SLEEP_SECONDS", 0,
        )

        s3 = _InMemoryS3()
        form4_list = [{
            "form_type": "4",
            "accession_number": "abc",
            "filed_date": date(2026, 5, 13),
            "primary": "form4.xml",
        }]
        http = self._make_http_mock(
            form4_list=form4_list,
            xml_by_url={"form4.xml": _FORM4_SINGLE_SALE},
        )
        stats = ingest_for_tickers(
            ["AAPL"], lookback_days=90,
            s3_client=s3, http=http, dry_run=True,
        )
        # Stats still report what would have been written
        assert stats["n_parquet_writes"] == 1
        # But nothing in S3
        assert len(s3.store) == 0

    def test_download_failure_isolated_per_filing(self, monkeypatch):
        from rag.pipelines import ingest_form4
        monkeypatch.setattr(ingest_form4, "_CIK_CACHE", {})
        monkeypatch.setattr(
            ingest_form4, "_INTER_REQUEST_SLEEP_SECONDS", 0,
        )

        # 2 filings; one's primary doc is missing from xml_by_url → 404
        form4_list = [
            {
                "form_type": "4",
                "accession_number": "a",
                "filed_date": date(2026, 5, 13),
                "primary": "form4.xml",
            },
            {
                "form_type": "4",
                "accession_number": "b",
                "filed_date": date(2026, 5, 13),
                "primary": "missing.xml",  # 404
            },
        ]
        http = self._make_http_mock(
            form4_list=form4_list,
            xml_by_url={"form4.xml": _FORM4_SINGLE_SALE},  # missing.xml absent
        )
        s3 = _InMemoryS3()
        stats = ingest_for_tickers(
            ["AAPL"], lookback_days=90, s3_client=s3, http=http,
        )
        assert stats["n_filings_discovered"] == 2
        assert stats["n_filings_downloaded"] == 1
        assert stats["n_failures"] == 1
        # Successful filing still got parsed + written
        assert stats["n_transactions_parsed"] == 1
        assert stats["n_parquet_writes"] == 1


# ── Schema version ────────────────────────────────────────────────────


def test_schema_version_pinned_to_one():
    assert SCHEMA_VERSION == 1


def test_schema_version_on_every_row():
    txs = parse_form4_xml(
        _FORM4_MULTI_TRANSACTION,
        accession_number="x", filed_date=date(2026, 5, 13),
    )
    assert all(t.schema_version == SCHEMA_VERSION for t in txs)

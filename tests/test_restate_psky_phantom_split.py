"""scripts/restate_psky_phantom_split.py (alpha-engine-config-I12359).

Builds a versioned moto bucket shaped like the live staging/daily_closes
history (yfinance first write -> polygon writes -> the 10-05 x0.5/x2 halving
-> more halved rewrites) and checks that the restatement:

* picks the last pre-halving version as its source and touches only PSKY;
* refuses before writing anything on any state it was not written for;
* refuses --apply while #2192 is unmerged;
* verifies its own write, records provenance, and is a no-op when re-run.
"""
from __future__ import annotations

import io
import json

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from scripts import restate_psky_phantom_split as r

DATE = "2026-10-02"
KEY = f"{r.PREFIX}{DATE}.parquet"


def _frame(psky: dict, aapl_close: float = 200.0, rev: int = 1) -> pd.DataFrame:
    base = {"date": DATE, "Adj_Close": None, "VWAP": None, "source": "polygon", "revision": rev,
            "xsource_status": "single_source_provisional", "xsource_flagged": True,
            "xsource_agreement_bps": float("nan"), "xsource_provenance": "x"}
    rows = {
        "AAPL": {**base, "Open": 199.0, "High": 201.0, "Low": 198.0, "Close": aapl_close,
                 "Adj_Close": aapl_close, "Volume": 1_000_000, "VWAP": 200.1},
        "PSKY": {**base, **psky, "Adj_Close": psky["Close"]},
    }
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "ticker"
    df["Volume"] = df["Volume"].astype("int64")
    df["revision"] = df["revision"].astype("int64")
    df["VWAP"] = df["VWAP"].astype("float64")
    df["Adj_Close"] = df["Adj_Close"].astype("float64")
    return df


TRUE = {"Open": 9.35, "High": 9.67, "Low": 8.97, "Close": 9.5, "Volume": 37388663}
YF = {"Open": 9.37, "High": 9.67, "Low": 8.97, "Close": 9.5, "Volume": 37359669, "source": "yfinance"}
HALF = {"Open": 4.675, "High": 4.835, "Low": 4.485, "Close": 4.75, "Volume": 74777326}


def _put(s3, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    s3.put_object(Bucket=r.BUCKET, Key=KEY, Body=buf.getvalue())


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setattr(r, "TARGETS", {DATE: 4.75})
    with mock_aws():
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket=r.BUCKET)
        c.put_bucket_versioning(Bucket=r.BUCKET, VersioningConfiguration={"Status": "Enabled"})
        yield c


def _history(s3, *, final_halved=HALF, aapl_last=201.5):
    _put(s3, _frame({**YF}, rev=1))
    _put(s3, _frame({**TRUE, "source": "polygon"}, rev=2))
    _put(s3, _frame({**HALF, "source": "polygon"}, rev=3))
    _put(s3, _frame({**final_halved, "source": "polygon"}, aapl_close=aapl_last, rev=4))


def _current(s3) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(s3.get_object(Bucket=r.BUCKET, Key=KEY)["Body"].read()))


def _n_versions(s3) -> int:
    return len(s3.list_object_versions(Bucket=r.BUCKET, Prefix=KEY)["Versions"])


# ── pure ───────────────────────────────────────────────────────────────────

def test_halving_detector_is_exact():
    assert r.is_halved(HALF, TRUE)
    assert not r.is_halved(TRUE, HALF)
    assert not r.is_halved({**HALF, "Volume": TRUE["Volume"]}, TRUE)  # price halved, volume not
    assert not r.is_halved({**HALF, "Close": 4.9}, TRUE)


def test_restore_source_is_the_version_before_the_single_transition():
    hist = [YF, TRUE, HALF, HALF]
    assert r.find_restore_source(hist) == (1, 2, False)
    assert r.find_restore_source([*hist, TRUE]) == (1, 2, True)  # already restated
    with pytest.raises(r.Refused):
        r.find_restore_source([YF, TRUE])                         # no transition
    with pytest.raises(r.Refused):
        r.find_restore_source([YF, TRUE, HALF, {**HALF, "Close": 4.8}])  # unexplained later change


# ── plan / apply against a versioned bucket ────────────────────────────────

def test_dry_run_writes_nothing_and_plans_only_psky(s3, monkeypatch):
    _history(s3)
    monkeypatch.setattr(r, "fix_merged", lambda: (True, "test"))
    before = _n_versions(s3)
    assert r.main([]) == 0
    assert _n_versions(s3) == before


def test_apply_restates_psky_only_and_verifies(s3, monkeypatch, tmp_path):
    _history(s3)
    monkeypatch.setattr(r, "fix_merged", lambda: (True, "test"))
    monkeypatch.setenv("HOME", str(tmp_path))
    pre = _current(s3)
    assert r.main(["--apply", "--script-pr", "https://example/pr"]) == 0
    post = _current(s3)
    ps = post.loc["PSKY"]
    assert (ps["Close"], ps["Open"], ps["Volume"]) == (9.5, 9.35, 37388663)
    assert ps["revision"] == pre.loc["PSKY", "revision"] + 1
    assert "RESTATED" in ps["xsource_provenance"]
    pd.testing.assert_frame_equal(post.drop(index="PSKY"), pre.drop(index="PSKY"))
    assert list(post.dtypes) == list(pre.dtypes)
    head = s3.head_object(Bucket=r.BUCKET, Key=KEY)
    assert head["Metadata"]["restated-ticker"] == "PSKY"
    # provenance record: S3 + local copy
    recs = s3.list_objects_v2(Bucket=r.BUCKET, Prefix=r.RECORD_PREFIX)["Contents"]
    assert len(recs) == 1
    rec = json.loads(s3.get_object(Bucket=r.BUCKET, Key=recs[0]["Key"])["Body"].read())
    assert rec["status"] == "complete" and rec["fix_pr"] == r.FIX_PR
    row = rec["rows"][0]
    assert row["status"] == "RESTATE" and row["verified"] is True
    assert row["before"]["Close"] == 4.75 and row["after"]["Close"] == 9.5
    assert row["current_version_id"] and row["new_version_id"] and row["restore_source_version_id"]
    assert list((tmp_path / ".ne-restatements").glob("*.json"))


def test_rerun_after_apply_is_a_no_op(s3, monkeypatch, tmp_path):
    _history(s3)
    monkeypatch.setattr(r, "fix_merged", lambda: (True, "test"))
    monkeypatch.setenv("HOME", str(tmp_path))
    assert r.main(["--apply"]) == 0
    n = _n_versions(s3)
    assert r.main(["--apply"]) == 0
    assert _n_versions(s3) == n
    plan = r.plan_file(s3, DATE, 4.75, restatement_id="t", run_date="2026-10-18")
    assert plan.status == "ALREADY-CORRECT"


def test_apply_refused_while_fix_unmerged(s3, monkeypatch):
    _history(s3)
    monkeypatch.setattr(r, "fix_merged", lambda: (False, "not merged"))
    n = _n_versions(s3)
    assert r.main(["--apply"]) == 1
    assert r.main([]) == 3  # dry run says the gate is closed
    assert _n_versions(s3) == n


def test_unmeasured_current_value_is_refused(s3, monkeypatch):
    _history(s3)
    monkeypatch.setattr(r, "fix_merged", lambda: (True, "test"))
    monkeypatch.setattr(r, "TARGETS", {DATE: 4.76})  # store differs from the 10-18 measurement
    n = _n_versions(s3)
    assert r.main(["--apply"]) == 1
    assert _n_versions(s3) == n


def test_aged_out_file_is_moot(s3, monkeypatch):
    _history(s3)
    s3.delete_object(Bucket=r.BUCKET, Key=KEY)  # lifecycle expiry = a delete marker
    monkeypatch.setattr(r, "fix_merged", lambda: (True, "test"))
    assert r.main(["--apply"]) == 0
    assert r.plan_file(s3, DATE, 4.75, restatement_id="t", run_date="d").status == "AGED-OUT"


def test_expiry_rounds_up_to_utc_midnight():
    import datetime as dt
    assert r._expiry(dt.datetime(2026, 10, 12, 11, 33, tzinfo=dt.UTC)) == "2026-10-20T00:00Z"
    assert r._expiry(dt.datetime(2026, 10, 12, 0, 0, tzinfo=dt.UTC)) == "2026-10-19T00:00Z"


def test_targets_are_the_five_measured_rows():
    import importlib
    fresh = importlib.reload(r)
    assert fresh.TARGETS == {"2026-09-28": 5.14, "2026-09-29": 4.995, "2026-09-30": 5.165,
                             "2026-10-01": 4.67, "2026-10-02": 4.75}

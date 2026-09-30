import datetime as dt
import gzip
import io
import json
import pytest
from data_gate.producers import cloudtrail_evidence as m

NOW = dt.datetime(2026, 9, 28, 22, tzinfo=dt.timezone.utc)
SINCE = dt.datetime(2026, 9, 28, 19, 18, 11, tzinfo=dt.timezone.utc)
EVENT = {"eventID": "event-1", "eventTime": "2026-09-28T21:00:00Z", "eventSource": "s3.amazonaws.com", "eventName": "PutObject", "requestParameters": {"bucketName": "research", "key": "trades/a"}, "userIdentity": {"arn": "arn:session", "sessionContext": {"sessionIssuer": {"arn": "arn:role"}}}}
SELECTORS = {"EventSelectors": [{"ReadWriteType": "All", "IncludeManagementEvents": True}, {"ReadWriteType": "WriteOnly", "IncludeManagementEvents": False, "DataResources": [{"Type": "AWS::S3::Object", "Values": ["arn:aws:s3:::research/"]}]}]}
LIFECYCLE = {"Rules": [{"Status": "Enabled", "Transitions": [{"Days": 90, "StorageClass": "GLACIER"}]}, {"Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 365}}, {"Status": "Enabled", "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}]}

class Trail:
    def get_event_selectors(self, **kw):
        return SELECTORS

class S3:
    def __init__(self, event=None, payload=None):
        self.payload = payload if payload is not None else gzip.compress(json.dumps({"Records": [event or EVENT]}).encode())
        self.puts = []
        self.calls = 0
    def get_bucket_lifecycle_configuration(self, **kw):
        return LIFECYCLE
    def list_objects_v2(self, **kw):
        self.calls += 1
        if kw.get("Delimiter"):
            return {"CommonPrefixes": [{"Prefix": "logs/us-east-1/"}], "IsTruncated": False}
        return {"Contents": [{"Key": kw["Prefix"] + "a.json.gz"}], "IsTruncated": False}
    def get_object(self, **kw):
        return {"Body": io.BytesIO(self.payload)}
    def put_object(self, **kw):
        self.puts.append(kw)

def collect(s3=None, **kw):
    return m.collect(s3 or S3(), Trail(), archive_bucket="archive", archive_prefix="logs/", research_bucket="research", trail="trail", since=SINCE, now=NOW, source_sha="a"*40, **kw)

def test_actual_event_and_private_provenance():
    result = collect()
    assert result["status"] == "verified_sample"
    assert result["sample"]["event_id"] == "event-1"
    assert result["sample"]["principal"] == "arn:role"
    assert result["sample"]["archive_key"].endswith("a.json.gz")
    assert result["coverage"]["full_cycle_verified"] is False
    assert result["coverage"]["current_day_partial"] is True
    m.validate(result)

@pytest.mark.parametrize("change", [{"errorCode":"AccessDenied"}, {"eventTime":"2026-09-28T18:00:00Z"}, {"eventTime":"2026-09-29T00:00:00Z"}, {"eventName":"GetObject"}, {"requestParameters":{"bucketName":"other","key":"x"}}])
def test_non_evidence_is_not_success(change):
    assert collect(S3(EVENT | change))["status"] == "unobserved"

def test_malformed_archive_is_error_not_quiet():
    result = collect(S3(payload=b"bad gzip"))
    assert result["status"] == "error"
    assert result["errors"]
    assert result["sample"] is None

def test_denied_metadata_is_error():
    class Denied(Trail):
        def get_event_selectors(self, **kw):
            raise PermissionError("denied")
    result = m.collect(S3(), Denied(), archive_bucket="archive", archive_prefix="logs/", research_bucket="research", trail="trail", since=SINCE, now=NOW, source_sha="a"*40)
    assert result["status"] == "error"

def test_request_limit_is_visible():
    result = collect(max_requests=2)
    assert result["status"] == "unobserved"
    assert result["coverage"]["truncated"] is True
    assert result["requests"] <= 2

def test_decompression_limit_is_visible():
    result = collect(max_bytes=20)
    assert result["coverage"]["truncated"] is True
    assert result["sample"] is None

def test_lifecycle_scoped_rule_cannot_prove_bucket_retention():
    assert not m.lifecycle_matches({"Rules":[r | {"Filter":{"Prefix":"only/"}} for r in LIFECYCLE["Rules"]]})

def test_expiration_or_different_transition_is_drift():
    assert not m.lifecycle_matches({"Rules": LIFECYCLE["Rules"] + [{"Status":"Enabled", "Expiration":{"Days":30}}]})
    assert not m.lifecycle_matches({"Rules": [{"Status":"Enabled", "Transitions":[{"Days":30,"StorageClass":"GLACIER"}]}]})

def test_selector_readonly_or_other_bucket_is_drift():
    assert not m.selectors_match({"EventSelectors":[{"ReadWriteType":"ReadOnly","IncludeManagementEvents":True}]}, "research")
    assert not m.selectors_match(SELECTORS, "wrong")

def test_schema_rejects_false_success():
    result = collect()
    result["sample"] = None
    with pytest.raises(Exception):
        m.validate(result)

def test_pagination_finds_second_page_and_does_not_loop():
    class Pages(S3):
        def list_objects_v2(self, **kw):
            if kw.get("Delimiter"):
                return super().list_objects_v2(**kw)
            if not kw.get("ContinuationToken"):
                return {"IsTruncated":True,"NextContinuationToken":"next","Contents":[]}
            return super().list_objects_v2(**kw)
    assert collect(Pages())["sample"]["event_id"] == "event-1"

def test_missing_token_is_error():
    class Broken(S3):
        def list_objects_v2(self, **kw):
            return {"IsTruncated":True}
    assert collect(Broken())["status"] == "error"

def test_cli_publishes_failure_without_leaking_metadata(monkeypatch, capsys):
    import boto3
    s3 = S3()
    monkeypatch.setattr(boto3, "client", lambda name, **kw: s3 if name == "s3" else Trail())
    report = collect(S3(payload=b"bad"))
    monkeypatch.setattr(m, "collect", lambda *a, **kw: report)
    for name, value in {"GITHUB_SHA":"a"*40,"GITHUB_RUN_ID":"42","GITHUB_RUN_ATTEMPT":"1"}.items():
        monkeypatch.setenv(name,value)
    assert m.main(["--since", SINCE.isoformat(), "--trail","trail"]) == 1
    assert [p["Key"] for p in s3.puts] == ["ops/checks/cloudtrail-evidence/runs/42-1.json","ops/checks/cloudtrail-evidence/latest.json"]
    assert json.loads(s3.puts[-1]["Body"])["status"] == "error"
    assert capsys.readouterr().out == "cloudtrail evidence status=error\n"

def test_workflow_is_independent_and_failure_notified():
    from pathlib import Path
    import yaml
    workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/phase-exit-metrics.yml").read_text())
    job = workflow["jobs"]["cloudtrail_evidence"]
    assert "needs" not in job
    assert "cloudtrail_evidence" in workflow["jobs"]["notify-main-failure"]["needs"]
    invoke = next(s for s in job["steps"] if "data_gate.producers.cloudtrail_evidence" in s.get("run",""))
    assert "2026-09-28T19:18:11Z" in invoke["run"]
    assert not invoke.get("continue-on-error")
    assert invoke["timeout-minutes"] == 3

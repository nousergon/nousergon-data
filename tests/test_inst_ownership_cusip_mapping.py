"""Tests for the CUSIP→ticker mapping path (alpha-engine-config-I10529).

Replaces the retired yfinance-based ``build_cusip_to_ticker`` (which called
``Ticker(t).info`` per universe ticker — the wrong mapping direction, and
one that returned 0 mappings from a GitHub-hosted runner) with an OpenFIGI
mapping adapter behind the ``IdentifierMapper`` protocol, an S3-cache
fallback (``CachedMapper``), and a token-bucket rate limiter. Covers:

1. ``OpenFigiMapper`` batching + rate-limit behaviour against a fake HTTP
   client (no network).
2. ``CachedMapper`` / ``build_cusip_to_ticker`` cache-merge behaviour.
3. The producer's fail-loud exit-code contract (``main`` exits non-zero
   when nothing was produced).
"""

from __future__ import annotations

import json
from io import BytesIO

import pytest

from data.derived.inst_ownership import (
    CachedMapper,
    OpenFigiMapper,
    OPENFIGI_KEYED_BATCH_SIZE,
    OPENFIGI_KEYED_RATE_PER_MIN,
    OPENFIGI_KEYLESS_BATCH_SIZE,
    OPENFIGI_KEYLESS_RATE_PER_MIN,
    OPENFIGI_MAPPING_URL,
    _TokenBucket,
    build_cusip_to_ticker,
)


class _InMemoryS3:
    """Mirrors test_inst_ownership_reader.py's stub."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise Exception(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}

    def seed_cache(self, bucket: str, mapping: dict[str, str], *, as_of: str = "2026-09-13") -> None:
        payload = {"as_of": as_of, "schema_version": 1, "mapping": mapping}
        self._store[(bucket, "data/crosswalks/cusip_to_ticker.json")] = (
            json.dumps(payload).encode("utf-8")
        )


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _FakePost:
    """Records every call and returns one canned OpenFIGI-shaped response
    per batch (``{"data": [{"ticker": ...}]}`` per job, in order, or
    ``{}`` for a cusip with no result — matches the real API's per-job
    result array, same length/order as the request)."""

    def __init__(self, ticker_for_cusip: dict[str, str]):
        self.ticker_for_cusip = ticker_for_cusip
        self.calls: list[dict] = []

    def __call__(self, url, *, json, headers, timeout):
        self.calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        results = []
        for job in json:
            cusip = job["idValue"]
            ticker = self.ticker_for_cusip.get(cusip)
            if ticker:
                results.append({"data": [{"ticker": ticker}]})
            else:
                results.append({"warning": "No identifier found."})
        return _FakeResponse(results)


class TestOpenFigiMapperBatching:
    def test_keyless_defaults_batch_of_10_rate_25(self):
        mapper = OpenFigiMapper()
        assert mapper.batch_size == OPENFIGI_KEYLESS_BATCH_SIZE == 10
        assert mapper._bucket._rate == OPENFIGI_KEYLESS_RATE_PER_MIN == 25

    def test_keyed_defaults_batch_of_100_rate_250(self):
        mapper = OpenFigiMapper(api_key="secret")
        assert mapper.batch_size == OPENFIGI_KEYED_BATCH_SIZE == 100
        assert mapper._bucket._rate == OPENFIGI_KEYED_RATE_PER_MIN == 250

    def test_splits_requests_into_batches(self):
        cusips = [f"{i:09d}" for i in range(25)]
        ticker_map = {c: f"T{i}" for i, c in enumerate(cusips)}
        fake_post = _FakePost(ticker_map)
        mapper = OpenFigiMapper(http_post=fake_post, batch_size=10)

        result = mapper.map_cusips(cusips)

        assert len(fake_post.calls) == 3  # 10 + 10 + 5
        assert [len(c["json"]) for c in fake_post.calls] == [10, 10, 5]
        assert result == ticker_map

    def test_sends_api_key_header_when_present(self):
        fake_post = _FakePost({"037833100": "AAPL"})
        mapper = OpenFigiMapper(api_key="my-key", http_post=fake_post)

        mapper.map_cusips(["037833100"])

        assert fake_post.calls[0]["headers"]["X-OPENFIGI-APIKEY"] == "my-key"
        assert fake_post.calls[0]["url"] == OPENFIGI_MAPPING_URL

    def test_omits_api_key_header_when_absent(self):
        fake_post = _FakePost({"037833100": "AAPL"})
        mapper = OpenFigiMapper(http_post=fake_post)

        mapper.map_cusips(["037833100"])

        assert "X-OPENFIGI-APIKEY" not in fake_post.calls[0]["headers"]

    def test_unresolved_cusip_omitted_not_errored(self):
        fake_post = _FakePost({"037833100": "AAPL"})
        mapper = OpenFigiMapper(http_post=fake_post)

        result = mapper.map_cusips(["037833100", "999999999"])

        assert result == {"037833100": "AAPL"}
        assert "999999999" not in result

    def test_request_failure_is_swallowed_and_logged_not_raised(self):
        def _raising_post(*a, **k):
            raise RuntimeError("network down")

        mapper = OpenFigiMapper(http_post=_raising_post)
        result = mapper.map_cusips(["037833100"])
        assert result == {}

    def test_dedupes_input_cusips(self):
        fake_post = _FakePost({"037833100": "AAPL"})
        mapper = OpenFigiMapper(http_post=fake_post, batch_size=10)

        mapper.map_cusips(["037833100", "037833100", "037833100"])

        assert len(fake_post.calls[0]["json"]) == 1


class TestTokenBucketRateLimit:
    def test_no_sleep_while_tokens_available(self):
        sleeps: list[float] = []
        clock = [0.0]
        bucket = _TokenBucket(
            rate=5, per_seconds=60.0,
            time_fn=lambda: clock[0], sleep_fn=sleeps.append,
        )
        for _ in range(5):
            bucket.acquire()
        assert sleeps == []

    def test_sleeps_once_bucket_exhausted(self):
        sleeps: list[float] = []
        clock = [0.0]
        bucket = _TokenBucket(
            rate=2, per_seconds=60.0,
            time_fn=lambda: clock[0], sleep_fn=sleeps.append,
        )
        bucket.acquire()
        bucket.acquire()
        bucket.acquire()  # 3rd call within the same instant must wait
        assert len(sleeps) == 1
        assert sleeps[0] > 0

    def test_tokens_replenish_over_elapsed_time(self):
        sleeps: list[float] = []
        clock = [0.0]
        bucket = _TokenBucket(
            rate=1, per_seconds=60.0,
            time_fn=lambda: clock[0], sleep_fn=sleeps.append,
        )
        bucket.acquire()  # consumes the only token
        clock[0] = 60.0  # a full period elapses
        bucket.acquire()  # should not need to sleep — token replenished
        assert sleeps == []


class TestCachedMapper:
    def test_returns_only_cached_entries(self):
        mapper = CachedMapper({"037833100": "AAPL", "594918104": "MSFT"})
        result = mapper.map_cusips(["037833100", "999999999"])
        assert result == {"037833100": "AAPL"}


class TestBuildCusipToTickerCacheMerge:
    def test_uses_cache_without_calling_mapper_when_fully_cached(self):
        s3 = _InMemoryS3()
        s3.seed_cache("test-bucket", {"037833100": "AAPL"})

        calls = []

        class _Spy:
            def map_cusips(self, cusips):
                calls.append(list(cusips))
                return {}

        result = build_cusip_to_ticker(
            {"037833100"}, s3_client=s3, bucket="test-bucket", mapper=_Spy(),
        )
        assert result == {"037833100": "AAPL"}
        assert calls == []  # never asked the live mapper — fully served by cache

    def test_queries_mapper_only_for_uncached_cusips(self):
        s3 = _InMemoryS3()
        s3.seed_cache("test-bucket", {"037833100": "AAPL"})

        class _Spy:
            def __init__(self):
                self.seen = None

            def map_cusips(self, cusips):
                self.seen = list(cusips)
                return {"594918104": "MSFT"}

        spy = _Spy()
        result = build_cusip_to_ticker(
            {"037833100", "594918104"}, s3_client=s3, bucket="test-bucket", mapper=spy,
        )
        assert spy.seen == ["594918104"]
        assert result == {"037833100": "AAPL", "594918104": "MSFT"}

    def test_persists_merged_mapping_back_to_cache(self):
        s3 = _InMemoryS3()
        s3.seed_cache("test-bucket", {"037833100": "AAPL"})

        class _Mapper:
            def map_cusips(self, cusips):
                return {c: "NEWTICK" for c in cusips}

        build_cusip_to_ticker(
            {"037833100", "023135106"}, s3_client=s3, bucket="test-bucket", mapper=_Mapper(),
        )

        obj = s3.get_object(Bucket="test-bucket", Key="data/crosswalks/cusip_to_ticker.json")
        persisted = json.loads(obj["Body"].read())
        assert persisted["mapping"] == {"037833100": "AAPL", "023135106": "NEWTICK"}

    def test_force_rebuild_ignores_stale_cache_but_still_merges(self):
        s3 = _InMemoryS3()
        s3.seed_cache("test-bucket", {"037833100": "STALE"})

        class _Mapper:
            def map_cusips(self, cusips):
                return {c: "FRESH" for c in cusips}

        result = build_cusip_to_ticker(
            {"037833100"}, s3_client=s3, bucket="test-bucket",
            force_rebuild=True, mapper=_Mapper(),
        )
        assert result == {"037833100": "FRESH"}

    def test_empty_when_nothing_cached_and_mapper_resolves_nothing(self):
        s3 = _InMemoryS3()

        class _Mapper:
            def map_cusips(self, cusips):
                return {}

        result = build_cusip_to_ticker(
            {"999999999"}, s3_client=s3, bucket="test-bucket", mapper=_Mapper(),
        )
        assert result == {}


class _NullSink:
    """A ManifestSink that keeps the write off the network. Shared by the
    main()-invoking tests below and by tests/test_inst_ownership_reader.py."""

    bucket = "test-bucket"

    def write(self, key: str, payload: bytes):  # noqa: ARG002
        return None


class TestMainExitCodeContract:
    """Fail-loud producer contract: main() must exit non-zero when nothing
    was produced, never sys.exit(0) — a producer that writes nothing and
    reports success was the root defect this issue tracks."""

    @pytest.fixture(autouse=True)
    def _no_real_manifest_writes(self, monkeypatch):
        """`main()` now writes one D39 run manifest per execution
        (alpha-engine-config-I10810). Swap the SINK so these tests exercise the
        real path without a live S3 PUT; the manifest's own content is graded in
        tests/test_unit_manifests.py."""
        import run_units

        monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
        monkeypatch.setattr(
            run_units, "manifest_sink", lambda bucket, s3_client=None: _NullSink(),
        )

    def test_exits_1_when_compute_returns_none(self, monkeypatch, tmp_path):
        from data.derived.inst_ownership import main

        tickers_file = tmp_path / "tickers.txt"
        tickers_file.write_text("AAPL\n")

        monkeypatch.setattr(
            "sys.argv", ["inst_ownership", "--tickers-file", str(tickers_file)],
        )
        monkeypatch.setattr("boto3.client", lambda *a, **k: None)
        monkeypatch.setattr(
            "data.derived.inst_ownership.compute_and_write_inst_ownership",
            lambda *a, **k: None,
        )

        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_exits_1_when_compute_returns_empty_list(self, monkeypatch, tmp_path):
        from data.derived.inst_ownership import main

        tickers_file = tmp_path / "tickers.txt"
        tickers_file.write_text("AAPL\n")

        monkeypatch.setattr(
            "sys.argv", ["inst_ownership", "--tickers-file", str(tickers_file)],
        )
        monkeypatch.setattr("boto3.client", lambda *a, **k: None)
        monkeypatch.setattr(
            "data.derived.inst_ownership.compute_and_write_inst_ownership",
            lambda *a, **k: [],
        )

        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1


import data.derived.inst_ownership as inst  # noqa: E402


class TestOpenFigiQuotaExhaustion:
    """Five consecutive 429s end the run with a named fix, not a 35-minute
    burn to the job timeout (nousergon-data run 34773054198, 2026-09-13)."""

    class _Resp:
        def __init__(self, status_code):
            self.status_code = status_code
            self.ok = status_code < 400

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

        def json(self):
            return [{"data": [{"ticker": "AAPL"}]}] * 10

    def test_five_consecutive_429s_raise_quota_exhausted(self):
        calls = []

        def post(url, *, json, headers, timeout):
            calls.append(len(json))
            return self._Resp(429)

        mapper = inst.OpenFigiMapper(api_key=None, http_post=post)
        mapper._bucket.acquire = lambda: None
        with pytest.raises(inst.OpenFigiQuotaExhausted) as ei:
            mapper.map_cusips([f"{i:09d}" for i in range(100)])
        assert len(calls) == inst.OPENFIGI_MAX_CONSECUTIVE_429
        assert "/alpha-engine/OPENFIGI_API_KEY" in str(ei.value)

    def test_a_success_resets_the_429_counter(self):
        seq = iter([429, 429, 200, 429, 429, 429, 429, 200, 200, 200])

        def post(url, *, json, headers, timeout):
            return self._Resp(next(seq))

        mapper = inst.OpenFigiMapper(api_key=None, http_post=post)
        mapper._bucket.acquire = lambda: None
        out = mapper.map_cusips([f"{i:09d}" for i in range(100)])
        assert out  # never raised; mapped the 200 batches

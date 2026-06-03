"""
Tests for ``preflight.DataPreflight`` (consolidated 2026-04-30).

Preflight runs at the START of weekly_collector before any collector
burns spot-EC2 time. Each check raises ``RuntimeError`` (the
``BasePreflight`` family) with a specific named message; these tests
mock requests + boto3 + arcticdb to exercise each failure mode.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from preflight import DataPreflight


BUCKET = "test-bucket"


def _make(mode: str = "phase1") -> DataPreflight:
    return DataPreflight(bucket=BUCKET, mode=mode)


# ── Mode validation ──────────────────────────────────────────────────────────

class TestModeValidation:
    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="unknown mode"):
            DataPreflight(bucket=BUCKET, mode="bogus")

    def test_known_modes_accepted(self):
        for mode in ("phase1", "phase2", "daily", "morning_enrich"):
            DataPreflight(bucket=BUCKET, mode=mode)  # no exception


# ── Polygon reachability ─────────────────────────────────────────────────────

class TestPolygonReachable:
    def _setup(self):
        env_patch = patch.dict("os.environ", {"POLYGON_API_KEY": "fake_key"}, clear=False)
        env_patch.start()
        self._env_patch = env_patch
        return _make()

    def teardown_method(self):
        if hasattr(self, "_env_patch"):
            self._env_patch.stop()

    def test_200_passes(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf._check_polygon_reachable()

    def test_401_auth_error(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=401, text="unauthorized")
            with pytest.raises(RuntimeError, match="auth failed.*invalid or revoked"):
                pf._check_polygon_reachable()

    def test_403_auth_error(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=403, text="forbidden")
            with pytest.raises(RuntimeError, match="auth failed"):
                pf._check_polygon_reachable()

    def test_500_outage(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=503, text="unavailable")
            with pytest.raises(RuntimeError, match="upstream outage"):
                pf._check_polygon_reachable()

    def test_network_error(self):
        # L4494: a sustained ConnectionError now retries _REACHABILITY_MAX_ATTEMPTS
        # times (backoff sleep patched out) before failing loud as "unreachable".
        pf = self._setup()
        with patch("requests.get") as mock_get, patch("preflight.time.sleep"):
            mock_get.side_effect = requests.ConnectionError("DNS failure")
            with pytest.raises(RuntimeError, match="unreachable after 3 attempts"):
                pf._check_polygon_reachable()
        assert mock_get.call_count == 3

    def test_transient_then_success_recovers(self):
        # L4494: a single transient blip must NOT abort — it retries and the
        # next attempt's 200 passes the probe.
        pf = self._setup()
        with patch("requests.get") as mock_get, patch("preflight.time.sleep"):
            mock_get.side_effect = [
                requests.ReadTimeout("read timed out"),
                MagicMock(status_code=200, text='{"results": []}'),
            ]
            pf._check_polygon_reachable()
        assert mock_get.call_count == 2


# ── FRED reachability ────────────────────────────────────────────────────────

class TestFredReachable:
    def _setup(self):
        env_patch = patch.dict("os.environ", {"FRED_API_KEY": "fake_key"}, clear=False)
        env_patch.start()
        self._env_patch = env_patch
        return _make()

    def teardown_method(self):
        if hasattr(self, "_env_patch"):
            self._env_patch.stop()

    def test_200_passes(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, text='{"observations": []}')
            pf._check_fred_reachable()

    def test_400_invalid_api_key(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=400, text="Bad Request: api_key is invalid"
            )
            with pytest.raises(RuntimeError, match="auth failed.*invalid"):
                pf._check_fred_reachable()

    def test_500_outage(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=502, text="bad gateway")
            with pytest.raises(RuntimeError, match="upstream outage"):
                pf._check_fred_reachable()

    def test_network_error(self):
        # L4494: sustained Timeout retries then fails loud as "unreachable".
        pf = self._setup()
        with patch("requests.get") as mock_get, patch("preflight.time.sleep"):
            mock_get.side_effect = requests.Timeout("timed out")
            with pytest.raises(RuntimeError, match="unreachable after 3 attempts"):
                pf._check_fred_reachable()
        assert mock_get.call_count == 3


# ── S3 writeable sentinel ────────────────────────────────────────────────────

class TestS3WriteableSentinel:
    def test_put_and_delete_succeed(self):
        pf = _make()
        mock_s3 = MagicMock()
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        with patch("boto3.client", return_value=mock_s3):
            pf._check_s3_writeable_sentinel()
        assert mock_s3.put_object.called
        assert mock_s3.delete_object.called

    def test_put_failure_raises(self):
        pf = _make()
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("PutObject denied")
        with patch("boto3.client", return_value=mock_s3):
            with pytest.raises(RuntimeError, match="PUT.*failed"):
                pf._check_s3_writeable_sentinel()

    def test_delete_failure_is_warning_not_fatal(self):
        """DELETE failure logs WARNING but does not raise — sentinel
        accumulation is benign and PUT succeeded."""
        pf = _make()
        mock_s3 = MagicMock()
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.side_effect = Exception("DeleteObject denied")
        with patch("boto3.client", return_value=mock_s3):
            pf._check_s3_writeable_sentinel()  # no exception


# ── ArcticDB libraries present ───────────────────────────────────────────────

class TestArcticDbLibrariesPresent:
    def test_all_expected_libraries_present(self):
        pf = _make()
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro", "extra"]
        with patch("arcticdb.Arctic", return_value=mock_arctic):
            pf._check_arcticdb_libraries_present(("universe", "macro"))

    def test_connection_failure(self):
        pf = _make()
        with patch("arcticdb.Arctic", side_effect=Exception("S3 timeout")):
            with pytest.raises(RuntimeError, match="connection failed"):
                pf._check_arcticdb_libraries_present(("universe", "macro"))

    def test_missing_universe_library(self):
        pf = _make()
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["macro"]
        with patch("arcticdb.Arctic", return_value=mock_arctic):
            with pytest.raises(
                RuntimeError, match=r"missing expected libraries.*universe"
            ):
                pf._check_arcticdb_libraries_present(("universe", "macro"))

    def test_missing_macro_library(self):
        pf = _make()
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe"]
        with patch("arcticdb.Arctic", return_value=mock_arctic):
            with pytest.raises(
                RuntimeError, match=r"missing expected libraries.*macro"
            ):
                pf._check_arcticdb_libraries_present(("universe", "macro"))


# ── End-to-end run() per mode ────────────────────────────────────────────────

class TestRunEndToEnd:
    def test_phase1_all_pass(self):
        pf = _make("phase1")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro"]

        _secrets = {"POLYGON_API_KEY": "k1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("boto3.client", return_value=mock_s3), patch(
            "arcticdb.Arctic", return_value=mock_arctic
        ), patch("requests.get") as mock_http:
            mock_http.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf.run()

    def test_phase1_missing_polygon_secret_short_circuits(self):
        """Missing API-key SECRET raises before HTTP/S3/ArcticDB are touched.

        Post #241/#242 the keys resolve via get_secret() (SSM), not
        os.environ — but the fail-fast short-circuit before the
        reachability probes must still hold. Regression for the
        2026-05-16 Saturday SF DataPhase1 phase1-preflight failure.
        """
        pf = _make("phase1")
        # FRED present, POLYGON absent — at the SSM layer, not env.
        _secrets = {"AWS_REGION": "us-east-1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=True
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("requests.get") as mock_http, patch(
            "boto3.client"
        ) as mock_boto, patch("arcticdb.Arctic") as mock_arctic:
            with pytest.raises(RuntimeError, match="secrets missing.*POLYGON_API_KEY"):
                pf.run()
            mock_http.assert_not_called()
            mock_boto.assert_not_called()
            mock_arctic.assert_not_called()

    def test_daily_requires_arctic_freshness(self):
        """Daily mode skips polygon/FRED/sentinel + requires macro.SPY freshness."""
        pf = _make("daily")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_arctic_obj = MagicMock()
        mock_arctic_obj.list_libraries.return_value = ["universe", "macro"]
        # check_arcticdb_fresh reads symbol; provide a fresh DataFrame
        import pandas as pd
        from datetime import datetime, timezone
        fresh_df = pd.DataFrame(
            {"close": [1.0]},
            index=pd.DatetimeIndex([pd.Timestamp(datetime.now(timezone.utc).date())]),
        )
        mock_lib = MagicMock()
        mock_lib.read.return_value = MagicMock(data=fresh_df)
        mock_arctic_obj.get_library.return_value = mock_lib

        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch("boto3.client", return_value=mock_s3), patch(
            "arcticdb.Arctic", return_value=mock_arctic_obj
        ):
            pf.run()

    def test_phase2_requires_fmp_stable(self):
        pf = _make("phase2")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}

        _secrets = {
            "FMP_API_KEY": "x",
            "FINNHUB_API_KEY": "y",
            "EDGAR_IDENTITY": "Tester test@example.com",
        }
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("boto3.client", return_value=mock_s3), patch("requests.get") as mock_http:
            mock_http.return_value = MagicMock(
                status_code=200, text='[{"symbol":"AAPL"}]', json=lambda: [{"symbol": "AAPL"}]
            )
            pf.run()


# ── morning_enrich mode (preflight-task-split 2026-05-16) ─────────────────────


class TestMorningEnrichMode:
    """`morning_enrich` is the dedicated entry preflight for the
    MorningEnrich SF state (split out of DataPhase1). Its checks are the
    UNION of what _run_morning_enrich needs: polygon + FRED secrets +
    reachability + S3 writeable + ArcticDB libraries present. It MUST NOT
    perform an ArcticDB-freshness check — morning-enrich is part of what
    makes ArcticDB fresh, so a freshness gate at its own entry would be
    circular. Previously --morning-enrich mapped to mode "daily", which
    only probed ArcticDB freshness and never validated polygon/FRED
    reachability — so a drifted key failed ~28min into the spot run."""

    def test_morning_enrich_all_pass(self):
        pf = _make("morning_enrich")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro"]

        _secrets = {"POLYGON_API_KEY": "k1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("boto3.client", return_value=mock_s3), patch(
            "arcticdb.Arctic", return_value=mock_arctic
        ), patch("requests.get") as mock_http:
            mock_http.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf.run()
        # Probed polygon + FRED (2 reachability HTTP calls minimum).
        assert mock_http.call_count >= 2

    def test_morning_enrich_probes_polygon_and_fred(self):
        """The two reachability probes must actually fire (regression for
        the old 'daily' mapping that skipped them entirely)."""
        pf = _make("morning_enrich")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro"]

        _secrets = {"POLYGON_API_KEY": "k1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("boto3.client", return_value=mock_s3), patch(
            "arcticdb.Arctic", return_value=mock_arctic
        ), patch("requests.get") as mock_http:
            mock_http.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf.run()
        urls = [c.args[0] for c in mock_http.call_args_list]
        assert any("polygon.io" in u for u in urls), urls
        assert any("stlouisfed.org" in u for u in urls), urls

    def test_morning_enrich_no_arcticdb_freshness_check(self):
        """morning_enrich must NOT call check_arcticdb_fresh (it is part
        of what makes ArcticDB fresh — a freshness gate here is circular).
        Patch the freshness primitive and assert it's never invoked."""
        pf = _make("morning_enrich")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro"]

        _secrets = {"POLYGON_API_KEY": "k1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("boto3.client", return_value=mock_s3), patch(
            "arcticdb.Arctic", return_value=mock_arctic
        ), patch("requests.get") as mock_http, patch.object(
            DataPreflight, "check_arcticdb_fresh"
        ) as mock_fresh:
            mock_http.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf.run()
            mock_fresh.assert_not_called()

    def test_morning_enrich_missing_polygon_secret_short_circuits(self):
        """Missing API-key SECRET fails fast in <1s, before HTTP / S3 /
        ArcticDB are touched — same fail-fast contract as phase1."""
        pf = _make("morning_enrich")
        _secrets = {"AWS_REGION": "us-east-1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=True
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("requests.get") as mock_http, patch(
            "boto3.client"
        ) as mock_boto, patch("arcticdb.Arctic") as mock_arctic:
            with pytest.raises(
                RuntimeError, match="secrets missing.*POLYGON_API_KEY"
            ):
                pf.run()
            mock_http.assert_not_called()
            mock_boto.assert_not_called()
            mock_arctic.assert_not_called()

    def test_morning_enrich_requires_arcticdb_libraries_present(self):
        """Libraries-present gate (not freshness) must still fire — a
        path_prefix typo must fail in 100ms not 28min."""
        pf = _make("morning_enrich")
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_s3.put_object.return_value = {}
        mock_s3.delete_object.return_value = {}
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["macro"]  # universe missing

        _secrets = {"POLYGON_API_KEY": "k1", "FRED_API_KEY": "k2"}
        with patch.dict(
            "os.environ", {"AWS_REGION": "us-east-1"}, clear=False
        ), patch(
            "preflight.get_secret",
            side_effect=lambda n, **kw: _secrets.get(n, kw.get("default", "")),
        ), patch("boto3.client", return_value=mock_s3), patch(
            "arcticdb.Arctic", return_value=mock_arctic
        ), patch("requests.get") as mock_http:
            mock_http.return_value = MagicMock(status_code=200, text='{"ok": true}')
            with pytest.raises(
                RuntimeError, match=r"missing expected libraries.*universe"
            ):
                pf.run()

"""Regression tests for two production fixes in ``collectors/alternative.py``.

1. EDGAR local data dir → /tmp (Lambda read-only $HOME)
   --------------------------------------------------------
   edgartools (``edgar`` package, used by ``_fetch_institutional`` for 13F
   data) writes its local data + httpx response cache under ``~/.edgar`` /
   ``~/.edgar/_tcache``. In the DataPhase2 Lambda sandbox ``$HOME`` is a
   read-only filesystem (only ``/tmp`` is writable), so on 2026-05-17 every
   edgar call raised ``[Errno 30] Read-only file system`` → institutional
   source 0/33 populated → per-source populated-ratio gate (``institutional``
   floor 0.20) breached → DataPhase2 returned ``{"status": "ERROR"}``.

   The module sets ``EDGAR_LOCAL_DATA_DIR`` to a writable ``/tmp`` path at
   import time *only if unset* (an operator-provided value must win).

2. API-key leak in alt-data exception logs
   ----------------------------------------
   FMP-backed warnings ("EPS estimate failed for AFL: 402 ... ?apikey=<KEY>")
   and Finnhub-backed warnings (``token=<KEY>``) embed the live credential in
   the request URL inside ``HTTPError.str()``. ``_scrub_url_creds`` masks
   ``apikey=``/``api_key=``/``token=`` querystring secrets before logging.
"""

from __future__ import annotations

import importlib
import os

import pytest


# ── _scrub_url_creds ───────────────────────────────────────────────────────


def test_scrub_masks_fmp_apikey_in_402_url():
    from collectors import alternative

    msg = (
        "402 Client Error: Payment Required for url: "
        "https://financialmodelingprep.com/stable/analyst-estimates"
        "?apikey=4509846484a78c3ee667a118d5179de7&symbol=AFL&period=annual"
    )
    scrubbed = alternative._scrub_url_creds(msg)
    assert "4509846484a78c3ee667a118d5179de7" not in scrubbed
    assert "apikey=***" in scrubbed
    # querystring after the key must survive (regex stops at ``&``).
    assert "symbol=AFL" in scrubbed and "period=annual" in scrubbed


def test_scrub_masks_api_key_underscore_variant():
    from collectors import alternative

    msg = "url: https://x/?api_key=SECRETVALUEXYZ&file_type=json"
    scrubbed = alternative._scrub_url_creds(msg)
    assert "SECRETVALUEXYZ" not in scrubbed
    assert "api_key=***" in scrubbed
    assert scrubbed == "url: https://x/?api_key=***&file_type=json"


def test_scrub_masks_token_variant():
    from collectors import alternative

    msg = "https://finnhub.io/api/v1/stock/recommendation?token=LIVEFINNHUB&symbol=AFL"
    scrubbed = alternative._scrub_url_creds(msg)
    assert "LIVEFINNHUB" not in scrubbed
    assert "token=***" in scrubbed


def test_scrub_accepts_exception_object_directly():
    """The helper is invoked at ``logger.warning("... %s", _scrub(e))``
    sites — it must accept an exception object, not just a str."""
    import requests

    try:
        resp = requests.Response()
        resp.status_code = 402
        resp.url = (
            "https://financialmodelingprep.com/stable/analyst-estimates"
            "?apikey=EXC_OBJ_SECRET&symbol=AFL"
        )
        resp.reason = "Payment Required"
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        from collectors import alternative

        scrubbed = alternative._scrub_url_creds(e)

    assert "EXC_OBJ_SECRET" not in scrubbed
    assert "apikey=***" in scrubbed


def test_scrub_is_noop_on_clean_string():
    from collectors import alternative

    msg = "Finnhub recommendation failed for AFL: connection timed out"
    assert alternative._scrub_url_creds(msg) == msg


def test_scrub_is_idempotent():
    from collectors import alternative

    msg = "url: https://fmp/x?apikey=SECRET&symbol=AFL"
    once = alternative._scrub_url_creds(msg)
    twice = alternative._scrub_url_creds(once)
    assert once == twice
    assert "SECRET" not in twice


def test_scrub_case_insensitive():
    from collectors import alternative

    msg = "https://x/?APIKEY=MIXEDCASE&a=1"
    scrubbed = alternative._scrub_url_creds(msg)
    assert "MIXEDCASE" not in scrubbed
    assert "APIKEY=***" in scrubbed


# ── EDGAR_LOCAL_DATA_DIR redirect ──────────────────────────────────────────


def test_edgar_local_data_dir_set_to_tmp_when_unset(monkeypatch):
    """Simulate the Lambda env (no preset var): after the institutional
    module is (re)imported, ``EDGAR_LOCAL_DATA_DIR`` points at a writable
    ``/tmp`` path and the directory exists."""
    monkeypatch.delenv("EDGAR_LOCAL_DATA_DIR", raising=False)

    from collectors import alternative

    importlib.reload(alternative)

    val = os.environ.get("EDGAR_LOCAL_DATA_DIR")
    assert val is not None
    assert val.startswith("/tmp/"), f"expected /tmp path, got {val!r}"
    assert os.path.isdir(val)


def test_edgar_local_data_dir_respects_operator_preset(monkeypatch, tmp_path):
    """An operator-provided ``EDGAR_LOCAL_DATA_DIR`` must NOT be overridden
    (e.g. a future EFS mount or a tuned /tmp subdir)."""
    preset = str(tmp_path / "operator-edgar")
    monkeypatch.setenv("EDGAR_LOCAL_DATA_DIR", preset)

    from collectors import alternative

    importlib.reload(alternative)

    assert os.environ.get("EDGAR_LOCAL_DATA_DIR") == preset


def test_edgar_redirect_is_effective_for_installed_edgartools():
    """End-to-end: with the env var set by the module, the installed
    edgartools resolves BOTH its data dir and its httpx ``_tcache`` cache
    (the path that raised the read-only-FS error) under the redirected
    root — proving the env var alone is sufficient (no $HOME override)."""
    edgar = pytest.importorskip("edgar")

    from collectors import alternative  # noqa: F401  (sets the env var)

    root = os.environ["EDGAR_LOCAL_DATA_DIR"]
    resolved_root = os.path.realpath(root)

    from edgar.core import get_edgar_data_directory
    from edgar.httpclient import get_cache_directory

    data_dir = os.path.realpath(str(get_edgar_data_directory()))
    http_cache = os.path.realpath(str(get_cache_directory()))

    assert data_dir.startswith(resolved_root), (data_dir, resolved_root)
    assert http_cache.startswith(resolved_root), (http_cache, resolved_root)

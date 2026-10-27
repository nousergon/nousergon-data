"""corporate_actions never logs an exception's message text (alpha-engine-config-I12704).

CodeQL ``py/clear-text-logging-sensitive-data`` flagged ``corporate_actions``:
its degrade paths logged polygon exception text, which embeds the request URL
and therefore the ``apiKey`` querystring. The text went through a regex mask
(``polygon_client._scrub_api_key``), and the module's own ``_scrub`` fallback
returned the RAW ``str(exc)`` whenever that import failed. The fix logs a
fixed description instead (class name, HTTP status, AWS error code); these
tests pin that no part of the message reaches the log on any path.

The sentinel below is a made-up marker, not a credential.
"""
from __future__ import annotations

import ast
import logging
import os
from unittest.mock import MagicMock, patch

import pytest
import requests
from botocore.exceptions import ClientError

import corporate_actions as ca

SENTINEL = "SENTINEL-not-a-real-key-i12704"
URL = f"https://api.polygon.io/v3/reference/splits?ticker=X&apiKey={SENTINEL}"


def _leaky_http_error(status: int = 500) -> requests.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    resp.url = URL
    return requests.HTTPError(f"{status} Server Error for url: {URL}", response=resp)


def _leaky_plain_error() -> RuntimeError:
    # Not an HTTPError: the message is the only thing carrying the URL.
    return RuntimeError(f"connection reset while fetching {URL}")


def _logged(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records)


# ── _describe_error ───────────────────────────────────────────────────────


def test_describe_error_http_error_is_class_and_status_only():
    out = ca._describe_error(_leaky_http_error(503))
    assert out == "HTTPError (HTTP 503)"
    assert SENTINEL not in out


def test_describe_error_plain_error_is_class_name_only():
    out = ca._describe_error(_leaky_plain_error())
    assert out == "RuntimeError"


def test_describe_error_client_error_carries_the_aws_code_not_the_message():
    exc = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": f"missing {SENTINEL}"}},
        "GetObject",
    )
    out = ca._describe_error(exc)
    assert out == "ClientError (NoSuchKey)"
    assert SENTINEL not in out


def test_describe_error_ignores_a_code_that_is_not_a_plain_token():
    exc = ClientError({"Error": {"Code": f"x {SENTINEL}"}}, "GetObject")
    assert ca._describe_error(exc) == "ClientError"


# ── every polygon degrade path ────────────────────────────────────────────

_FETCH_PATHS = [
    ("get_splits", "get_splits", lambda f: f("AAPL", client=_client())),
    ("get_dividends", "get_dividends", lambda f: f("AAPL", client=_client())),
    ("detect_splits", "get_recent_splits",
     lambda f: f("2026-10-01", "2026-10-02", client=_client())),
    ("detect_dividends", "get_recent_dividends",
     lambda f: f("2026-10-01", "2026-10-02", client=_client())),
    ("detect_renames", "get_ticker_events", lambda f: f(["OLD"], client=_client())),
]

_CURRENT: dict = {}


def _client():
    return _CURRENT["client"]


@pytest.mark.parametrize("make_exc", [_leaky_http_error, _leaky_plain_error])
@pytest.mark.parametrize("func_name,method,call", _FETCH_PATHS)
def test_fetch_failure_log_carries_no_message_text(caplog, func_name, method, call, make_exc):
    client = MagicMock()
    getattr(client, method).side_effect = make_exc()
    _CURRENT["client"] = client
    with caplog.at_level(logging.DEBUG, logger="corporate_actions"):
        call(getattr(ca, func_name))
    logged = _logged(caplog)
    assert caplog.records, f"{func_name} logged nothing on a fetch failure"
    assert SENTINEL not in logged
    assert "apiKey" not in logged
    assert type(make_exc()).__name__ in logged


_CONSTRUCT_PATHS = [
    ("get_splits", lambda f: f("AAPL")),
    ("get_dividends", lambda f: f("AAPL")),
    ("detect_splits", lambda f: f("2026-10-01", "2026-10-02")),
    ("detect_dividends", lambda f: f("2026-10-01", "2026-10-02")),
    ("detect_renames", lambda f: f(["OLD"])),
]


@pytest.mark.parametrize("func_name,call", _CONSTRUCT_PATHS)
def test_client_construction_failure_log_carries_no_message_text(caplog, func_name, call):
    with patch("polygon_client.polygon_client", side_effect=_leaky_plain_error()):
        with caplog.at_level(logging.DEBUG, logger="corporate_actions"):
            call(getattr(ca, func_name))
    logged = _logged(caplog)
    assert caplog.records
    assert SENTINEL not in logged
    assert "RuntimeError" in logged


def test_archive_parquet_read_failure_log_carries_no_message_text(caplog):
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"not parquet")}
    with patch.object(ca.pd, "read_parquet", side_effect=_leaky_plain_error()):
        with caplog.at_level(logging.DEBUG, logger="corporate_actions"):
            assert ca._read_archive_parquet(s3, "b", "p/", "2026-10-01") is None
    logged = _logged(caplog)
    assert caplog.records
    assert SENTINEL not in logged


# ── static: no log argument is built from the message ─────────────────────

_SRC = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "corporate_actions", "__init__.py",
)
_LOG_METHODS = {"debug", "info", "warning", "error", "exception", "critical"}


def _log_calls(tree):
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _LOG_METHODS
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "log"
        ):
            yield node


def test_no_log_call_formats_exception_text():
    """Every log argument that mentions an ``except ... as exc`` binding goes
    through ``_describe_error`` (or is the bare ``type(exc).__name__``); none is
    ``exc`` itself, ``str(exc)``,
    ``repr(exc)``, ``exc.args`` or a ``*_scrub_api_key`` mask."""
    with open(_SRC, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    offenders = []
    for call in _log_calls(tree):
        for arg in call.args[1:]:
            names = {n.id for n in ast.walk(arg) if isinstance(n, ast.Name)}
            if "exc" not in names:
                continue
            ok = (
                isinstance(arg, ast.Call)
                and isinstance(arg.func, ast.Name)
                and arg.func.id == "_describe_error"
            ) or ast.unparse(arg) == "type(exc).__name__"
            if not ok:
                offenders.append(f"line {call.lineno}: {ast.unparse(arg)}")
    assert not offenders, offenders


def test_module_no_longer_calls_a_regex_mask_or_formats_exc():
    with open(_SRC, encoding="utf-8") as fh:
        src = fh.read()
    tree = ast.parse(src)
    called = {
        n.func.id for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }
    assert "_scrub_api_key" not in called
    assert "_scrub" not in called
    fn = next(
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "_describe_error"
    )
    body = "\n".join(ast.unparse(stmt) for stmt in fn.body[1:])  # skip docstring
    for banned in ("str(exc)", "repr(exc)", "exc.args", "format(exc"):
        assert banned not in body, banned

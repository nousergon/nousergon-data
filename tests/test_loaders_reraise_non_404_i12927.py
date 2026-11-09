"""S3 loader absence is distinct from an unreadable or corrupt object (I12927)."""

from io import BytesIO
import json
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
import pytest

from collectors import constituents, nasdaq100, universe_returns


LOADERS = ("nasdaq100", "constituents_pointer", "constituents_document", "sector_map")


class NoSuchKey(ClientError):
    """Model the client's NoSuchKey exception, a ClientError subclass."""


def _load(loader: str, s3: Mock) -> object:
    if loader == "sector_map":
        return universe_returns._load_sector_map(s3, "bucket", "sector_map.json")
    module = nasdaq100 if loader == "nasdaq100" else constituents
    with patch.object(module.boto3, "client", return_value=s3):
        return module.load_from_s3("bucket")


def _s3(loader: str, result: object) -> Mock:
    s3 = Mock()
    responses: list[object] = [result]
    if loader == "constituents_document":
        responses.insert(0, {"Body": BytesIO(b'{"date": "2026-11-09"}')})
    s3.get_object.side_effect = responses
    return s3


@pytest.mark.parametrize("loader", LOADERS)
@pytest.mark.parametrize("code", ("NoSuchKey", "404", "NotFound"))
def test_not_found_returns_none(loader: str, code: str) -> None:
    error = ClientError({"Error": {"Code": code}}, "GetObject")
    assert _load(loader, _s3(loader, error)) is None


@pytest.mark.parametrize("loader", LOADERS)
def test_client_no_such_key_returns_none(loader: str) -> None:
    error = NoSuchKey({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    assert _load(loader, _s3(loader, error)) is None


@pytest.mark.parametrize("loader", LOADERS)
@pytest.mark.parametrize("code", ("Throttling", "AccessDenied", "500"))
def test_other_client_errors_propagate(loader: str, code: str) -> None:
    error = ClientError({"Error": {"Code": code}}, "GetObject")
    with pytest.raises(ClientError) as caught:
        _load(loader, _s3(loader, error))
    assert caught.value is error


@pytest.mark.parametrize("loader", LOADERS)
def test_invalid_json_propagates(loader: str) -> None:
    with pytest.raises(json.JSONDecodeError):
        _load(loader, _s3(loader, {"Body": BytesIO(b"not json")}))


@pytest.mark.parametrize("loader", LOADERS)
def test_other_exceptions_propagate(loader: str) -> None:
    error = RuntimeError("read failed")
    with pytest.raises(RuntimeError) as caught:
        _load(loader, _s3(loader, error))
    assert caught.value is error


@pytest.mark.parametrize("loader", ("nasdaq100", "constituents_document", "sector_map"))
def test_present_document_is_returned(loader: str) -> None:
    document = {"AAPL": "XLK"}
    s3 = _s3(loader, {"Body": BytesIO(json.dumps(document).encode())})
    assert _load(loader, s3) == document
    if loader == "constituents_document":
        assert s3.get_object.call_count == 2
        s3.get_object.assert_called_with(
            Bucket="bucket", Key="market_data/weekly/2026-11-09/constituents.json"
        )


def test_pointer_without_date_returns_none() -> None:
    s3 = _s3("constituents_pointer", {"Body": BytesIO(b"{}")})
    assert _load("constituents_pointer", s3) is None
    s3.get_object.assert_called_once()

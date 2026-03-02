import pytest

from app.services.ingestion import MAX_UPLOAD_BYTES, validate_upload_inputs


def test_validate_upload_inputs_rejects_missing_file_name():
    with pytest.raises(ValueError, match="File name is required"):
        validate_upload_inputs("", b"abc")


def test_validate_upload_inputs_rejects_unsupported_extension():
    with pytest.raises(ValueError, match="Unsupported file type"):
        validate_upload_inputs("data.json", b"{}")


def test_validate_upload_inputs_rejects_empty_file():
    with pytest.raises(ValueError, match="Uploaded file is empty"):
        validate_upload_inputs("data.csv", b"")


def test_validate_upload_inputs_rejects_oversized_file():
    too_large = b"a" * (MAX_UPLOAD_BYTES + 1)
    with pytest.raises(ValueError, match="File too large"):
        validate_upload_inputs("data.csv", too_large)


def test_validate_upload_inputs_accepts_csv():
    assert validate_upload_inputs("data.csv", b"id,name\n1,alpha") == ".csv"

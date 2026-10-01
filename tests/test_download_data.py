"""Tests for the download script. They never touch the network."""

import io

import pytest

from scripts import download_data


def test_trip_url_zero_pads_month():
    assert download_data.trip_url(2024, 3) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
    )


def test_existing_file_is_skipped_without_network(tmp_path, monkeypatch):
    dest = tmp_path / "yellow_tripdata_2024-01.parquet"
    dest.write_bytes(b"already here")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("should not hit the network for an existing file")

    monkeypatch.setattr(download_data.urllib.request, "urlopen", fail_if_called)

    assert download_data.download("https://example.com/x", dest) is False
    assert dest.read_bytes() == b"already here"


class _DroppedConnection(io.BytesIO):
    """Fake HTTP response whose body fails partway through."""

    headers = {"Content-Length": "100"}

    def read(self, *args):
        raise ConnectionError("connection dropped")


def test_interrupted_download_leaves_no_final_file(tmp_path, monkeypatch):
    dest = tmp_path / "yellow_tripdata_2024-01.parquet"
    monkeypatch.setattr(
        download_data.urllib.request, "urlopen", lambda *a, **k: _DroppedConnection()
    )

    with pytest.raises(ConnectionError):
        download_data.download("https://example.com/x", dest)

    # The next run must retry, so the real filename must not exist yet.
    assert not dest.exists()

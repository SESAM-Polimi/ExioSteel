"""Download contracts use small files; the full public source is checked separately."""
import copy
import hashlib
import io
import json

import pandas as pd
import pytest

from gcam_mario import download as dl


def entry(body, key="z.txt"):
    return {"key": key, "size": len(body),
            "checksum": "md5:" + hashlib.md5(body).hexdigest(),
            "links": {"self": f"https://zenodo.org/api/records/{dl.RECORD}/files/{key}/content"}}


class Response(io.BytesIO):
    def __init__(self, body, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {}


def no_network(*args, **kwargs):
    pytest.fail("Unexpected network request")


def test_resume_validates_range_and_full_checksum(tmp_path, monkeypatch):
    body = b"a complete source file"
    part = tmp_path / "z.txt.part"
    part.write_bytes(body[:5])

    def request(req, timeout):
        assert req.get_header("Range") == "bytes=5-"
        return Response(body[5:], 206, {"Content-Range": f"bytes 5-{len(body)-1}/{len(body)}"})

    monkeypatch.setattr(dl, "urlopen", request)
    path = dl._download_file(entry(body), tmp_path, progress=lambda _: None)
    assert path.read_bytes() == body
    assert not part.exists()
    monkeypatch.setattr(dl, "urlopen", no_network)
    assert dl._download_file(entry(body), tmp_path, progress=lambda _: None) == path


def test_ignored_range_restarts_instead_of_appending(tmp_path, monkeypatch):
    body = b"valid complete file"
    (tmp_path / "z.txt.part").write_bytes(b"partial")
    monkeypatch.setattr(dl, "urlopen", lambda *a, **k: Response(body))
    assert dl._download_file(entry(body), tmp_path, progress=lambda _: None).read_bytes() == body


def test_bad_range_never_commits_source(tmp_path, monkeypatch):
    (tmp_path / "z.txt.part").write_bytes(b"x")
    monkeypatch.setattr(dl, "urlopen", lambda *a, **k: Response(b"yz", 206, {"Content-Range": "bytes 0-1/2"}))
    with pytest.raises(ValueError, match="Invalid resume"):
        dl._download_file(entry(b"xyz"), tmp_path, progress=lambda _: None)
    assert not (tmp_path / "z.txt").exists()


def test_checksum_failure_never_commits_source(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "urlopen", lambda *a, **k: Response(b"wrong"))
    monkeypatch.setattr(dl.time, "sleep", lambda _: None)
    with pytest.raises(OSError, match="checksum mismatch"):
        dl._download_file(entry(b"right"), tmp_path, progress=lambda _: None)
    assert not (tmp_path / "z.txt").exists()


def test_truncated_transfer_resumes_on_retry(tmp_path, monkeypatch):
    body = b"resumable source"
    calls = []

    def request(req, **kwargs):
        calls.append(req)
        if len(calls) == 1:
            return Response(body[:4])
        assert req.get_header("Range") == "bytes=4-"
        return Response(body[4:], 206, {"Content-Range": f"bytes 4-{len(body)-1}/{len(body)}"})

    monkeypatch.setattr(dl, "urlopen", request)
    monkeypatch.setattr(dl.time, "sleep", lambda _: None)
    assert dl._download_file(entry(body), tmp_path, progress=lambda _: None).read_bytes() == body
    assert len(calls) == 2


def test_damaged_user_source_is_not_overwritten(tmp_path, monkeypatch):
    (tmp_path / "z.txt").write_bytes(b"damaged")
    monkeypatch.setattr(dl, "urlopen", no_network)
    with pytest.raises(ValueError, match="move the damaged file"):
        dl._download_file(entry(b"correct"), tmp_path)
    assert (tmp_path / "z.txt").read_bytes() == b"damaged"


@pytest.fixture
def tiny_source(tmp_path, monkeypatch):
    """Use real conversion, with enough rows to cross its chunk boundary."""
    monkeypatch.setattr(dl, "_CHUNK_ROWS", 128)
    index = pd.MultiIndex.from_tuples([("R", "Activity", f"a{i}") for i in range(130)])
    columns = pd.MultiIndex.from_tuples([("R", "Activity", "a0"), ("R", "Commodity", "c0")])
    frames = {}
    for stem in dl.REQUIRED:
        if stem == "units":
            frames[stem] = pd.DataFrame({"unit": ["None", "kg"]},
                                       index=pd.MultiIndex.from_tuples([("Activity", "a0"), ("Commodity", "c0")]))
        else:
            rows = index if stem in ("z", "Y") else pd.Index([f"row{i}" for i in range(130)])
            frames[stem] = pd.DataFrame([[0.12345678901234567, -0.00000000000023]] * 130,
                                       index=rows, columns=columns)
    bodies = {stem + ".txt": frame.to_csv().encode() for stem, frame in frames.items()}
    catalog = {"record_id": dl.RECORD, "doi": f"10.5281/zenodo.{dl.RECORD}",
               "files": [entry(body, key) for key, body in bodies.items()]}
    monkeypatch.setattr(dl, "_catalog", lambda: copy.deepcopy(catalog))

    def request(req, **kwargs):
        key = req.full_url.split("/files/")[1].split("/")[0]
        return Response(bodies[key])

    monkeypatch.setattr(dl, "urlopen", request)
    return frames


def test_complete_download_converts_and_reuses_verified_cache(tmp_path, monkeypatch, tiny_source):
    path = dl.download_database(tmp_path / "source", progress=lambda _: None)
    assert not (path / ".zenodo-download.json").exists()
    receipt = json.loads((path / "zenodo-source.json").read_text())
    assert receipt["record_id"] == dl.RECORD
    for stem, expected in tiny_source.items():
        actual = pd.read_parquet(path / f"{stem}.parquet")
        pd.testing.assert_frame_equal(actual, expected)
    monkeypatch.setattr(dl, "urlopen", no_network)
    assert dl.download_database(path, progress=lambda _: None) == path
    (path / "e.parquet").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum mismatch"):
        dl.download_database(path)


def test_unmanaged_parquets_are_never_overwritten(tmp_path, monkeypatch):
    (tmp_path / "z.parquet").write_bytes(b"my database")
    monkeypatch.setattr(dl, "urlopen", no_network)
    with pytest.raises(ValueError, match="unmanaged parquet"):
        dl.download_database(tmp_path)
    assert (tmp_path / "z.parquet").read_bytes() == b"my database"


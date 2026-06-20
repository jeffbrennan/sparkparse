from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sparkparse import storage
from sparkparse.storage import (
    copy_file,
    ensure_dir,
    get_path_name,
    get_path_stem,
    is_cloud_path,
    join_path,
    list_files,
    open_file,
    path_exists,
    read_text,
    remove_dir,
    write_text,
)


@pytest.mark.parametrize(
    "path",
    [
        "s3://bucket/key",
        "abfss://container@account.dfs.core.windows.net/path",
        "gs://bucket/key",
        "dbfs:/mnt/data/file",
    ],
)
def test_is_cloud_path_true_for_cloud_uris(path: str) -> None:
    assert is_cloud_path(path) is True


@pytest.mark.parametrize(
    "path",
    [
        "/local/path",
        "./relative/path",
        "data/logs/raw",
        "/dbfs/mnt/data/file",
        "/Volumes/catalog/schema/table",
        "file.txt",
    ],
)
def test_is_cloud_path_false_for_local_paths(path: str) -> None:
    assert is_cloud_path(path) is False


def test_is_cloud_path_accepts_pathlib() -> None:
    assert is_cloud_path(Path("/local/path")) is False


@pytest.fixture
def tmpdir_path(tmp_path: Path) -> Path:
    return tmp_path


def test_write_text_and_read_text_round_trip(tmpdir_path: Path) -> None:
    p = tmpdir_path / "file.txt"
    write_text(p, "hello cloud\n")
    assert read_text(p) == "hello cloud\n"


def test_write_text_creates_parent_dirs_not_needed(tmpdir_path: Path) -> None:
    p = tmpdir_path / "nested" / "dir" / "file.txt"
    ensure_dir(p.parent)
    write_text(p, "data")
    assert read_text(p) == "data"


def test_list_files_local(tmpdir_path: Path) -> None:
    (tmpdir_path / "a.txt").write_text("a")
    (tmpdir_path / "b.json").write_text("b")
    (tmpdir_path / "c.txt").write_text("c")
    results = list_files(tmpdir_path, "*.txt")
    names = sorted(Path(r).name for r in results)
    assert names == ["a.txt", "c.txt"]


def test_list_files_default_pattern(tmpdir_path: Path) -> None:
    (tmpdir_path / "x").write_text("x")
    results = list_files(tmpdir_path)
    assert len(results) == 1
    assert Path(results[0]).name == "x"


def test_copy_file_local(tmpdir_path: Path) -> None:
    src = tmpdir_path / "src.txt"
    dst = tmpdir_path / "dst.txt"
    write_text(src, "copy me")
    copy_file(src, dst)
    assert read_text(dst) == "copy me"


def test_remove_dir_local(tmpdir_path: Path) -> None:
    sub = tmpdir_path / "to_remove"
    sub.mkdir()
    (sub / "inner.txt").write_text("x")
    remove_dir(sub)
    assert not sub.exists()


def test_ensure_dir_local_idempotent(tmpdir_path: Path) -> None:
    new_dir = tmpdir_path / "new_dir"
    ensure_dir(new_dir)
    assert new_dir.exists() and new_dir.is_dir()
    ensure_dir(new_dir)
    assert new_dir.exists()


def test_path_exists_local_true(tmpdir_path: Path) -> None:
    p = tmpdir_path / "exists.txt"
    write_text(p, "yes")
    assert path_exists(p) is True


def test_path_exists_local_false(tmpdir_path: Path) -> None:
    assert path_exists(tmpdir_path / "missing") is False


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/local/dir/file.json", "file.json"),
        ("s3://bucket/prefix/file.json", "file.json"),
        ("s3://bucket/prefix/", "prefix"),
        ("gs://bucket/key", "key"),
        ("plain_name.csv", "plain_name.csv"),
    ],
)
def test_get_path_name(path: str, expected: str) -> None:
    assert get_path_name(path) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/local/dir/file.json", "file"),
        ("s3://bucket/prefix/file.json", "file"),
        ("s3://bucket/prefix/", "prefix"),
        ("no_extension", "no_extension"),
        ("plain_name.csv", "plain_name"),
    ],
)
def test_get_path_stem(path: str, expected: str) -> None:
    assert get_path_stem(path) == expected


@pytest.mark.parametrize(
    "base, parts, expected",
    [
        ("s3://bucket/prefix", ("file.txt",), "s3://bucket/prefix/file.txt"),
        ("s3://bucket/prefix/", ("file.txt",), "s3://bucket/prefix/file.txt"),
        ("/local/dir", ("sub", "f.csv"), "/local/dir/sub/f.csv"),
        ("/local/dir/", ("sub", "f.csv"), "/local/dir/sub/f.csv"),
        ("s3://bucket", (), "s3://bucket"),
    ],
)
def test_join_path(base: str, parts: tuple[str, ...], expected: str) -> None:
    assert join_path(base, *parts) == expected



@pytest.fixture
def mock_fsspec():
    fake = MagicMock(name="fsspec")
    file_handle = MagicMock(name="file_handle")
    file_handle.__enter__.return_value = file_handle
    file_handle.__exit__.return_value = None
    fake.open.return_value.open.return_value = file_handle
    fake.core.url_to_fs.return_value = (MagicMock(name="fs"), "stripped")
    fake.copy = MagicMock(name="copy")
    with patch.object(storage, "_import_fsspec", return_value=fake):
        yield fake


def test_open_file_cloud_uses_fsspec(mock_fsspec: MagicMock) -> None:
    open_file("s3://bucket/key", "r")
    mock_fsspec.open.assert_called_once_with("s3://bucket/key", "r")


def test_write_text_cloud_uses_fsspec(mock_fsspec: MagicMock) -> None:
    write_text("s3://bucket/key.txt", "content")
    mock_fsspec.open.assert_called_once_with("s3://bucket/key.txt", "w")


def test_read_text_cloud_uses_fsspec(mock_fsspec: MagicMock) -> None:
    file_handle = mock_fsspec.open.return_value.open.return_value
    file_handle.read.return_value = "cloud content"
    result = read_text("gs://bucket/key")
    assert result == "cloud content"
    mock_fsspec.open.assert_called_once_with("gs://bucket/key", "r")


def test_list_files_cloud_uses_fsspec_glob(mock_fsspec: MagicMock) -> None:
    fs = mock_fsspec.core.url_to_fs.return_value[0]
    fs.glob.return_value = ["s3://bucket/key1", "s3://bucket/key2"]
    fs.unstrip_protocol.side_effect = lambda p: p
    results = list_files("s3://bucket/path", "*")
    assert results == ["s3://bucket/key1", "s3://bucket/key2"]
    fs.glob.assert_called_once_with("s3://bucket/path/*")


def test_copy_file_cloud_uses_fsspec_copy(mock_fsspec: MagicMock) -> None:
    copy_file("s3://bucket/src", "s3://bucket/dst")
    mock_fsspec.copy.assert_called_once_with("s3://bucket/src", "s3://bucket/dst")


def test_remove_dir_cloud_uses_fsspec_rm(mock_fsspec: MagicMock) -> None:
    fs = mock_fsspec.core.url_to_fs.return_value[0]
    remove_dir("s3://bucket/dir")
    fs.rm.assert_called_once_with("s3://bucket/dir", recursive=True)


def test_ensure_dir_cloud_is_noop(mock_fsspec: MagicMock) -> None:
    ensure_dir("s3://bucket/dir")
    mock_fsspec.open.assert_not_called()
    mock_fsspec.core.url_to_fs.assert_not_called()


def test_path_exists_cloud_uses_fsspec(mock_fsspec: MagicMock) -> None:
    fs = mock_fsspec.core.url_to_fs.return_value[0]
    fs.exists.return_value = True
    assert path_exists("s3://bucket/key") is True
    fs.exists.assert_called_once_with("s3://bucket/key")


def test_mixed_copy_local_to_cloud_uses_fsspec(mock_fsspec: MagicMock) -> None:
    copy_file("/local/file", "s3://bucket/file")
    mock_fsspec.copy.assert_called_once_with("/local/file", "s3://bucket/file")

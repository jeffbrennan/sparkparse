"""Path-agnostic I/O utilities for local and cloud storage.

Cloud paths (``s3://``, ``abfss://``, ``gs://``, ``dbfs:/``) are routed through
``fsspec``; local paths use the stdlib. ``fsspec`` is imported lazily inside the
functions that need it, so local-only users do not have to install it or any
cloud backend.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import IO, Any

logger = logging.getLogger(__name__)

CLOUD_PREFIXES: tuple[str, ...] = ("s3://", "abfss://", "gs://", "dbfs:/")


def is_cloud_path(path: str | Path) -> bool:
    """Return True for cloud URIs, False for local paths.

    DBFS local-mount paths (``/dbfs/...``) and Unity Catalog volumes
    (``/Volumes/...``) are local filesystem paths on the Databricks driver and
    therefore return False.
    """
    path_str = str(path)
    return path_str.startswith(CLOUD_PREFIXES)


def _import_fsspec() -> Any:
    try:
        import fsspec

        return fsspec
    except ImportError as e:  # pragma: no cover - exercised only without fsspec
        raise ImportError(
            "fsspec is required for cloud path support. "
            "Install it with `uv add fsspec` or pull in the relevant cloud "
            "extra: sparkparse[s3|azure|gcs|cloud]."
        ) from e


def _fs_for(path: str) -> tuple[Any, str]:
    """Return (fsspec_filesystem, path_stripped_of_protocol) for a cloud path."""
    fsspec = _import_fsspec()
    fs, _, stripped = fsspec.core.get_fs_token_paths(path)
    return fs, stripped


def open_file(path: str | Path, mode: str = "r") -> IO[Any]:
    """Open a local or cloud file.

    Cloud paths require the relevant fsspec backend (``s3fs``, ``adlfs``,
    ``gcsfs``) and credentials to be configured in the environment.
    """
    path_str = str(path)
    if is_cloud_path(path_str):
        fsspec = _import_fsspec()
        return fsspec.open(path_str, mode).open()  # type: ignore[no-any-return]
    return open(path_str, mode)


def write_text(path: str | Path, content: str) -> None:
    """Write ``content`` to ``path`` as UTF-8 text, local or cloud."""
    with open_file(path, "w") as f:
        f.write(content)


def append_text(path: str | Path, content: str) -> None:
    """Append ``content`` to ``path`` as UTF-8 text, local or cloud.

    Creates the file if it does not exist. For cloud object stores there is
    no true append primitive, so each call rewrites the object; Delta is the
    recommended format for cloud history stores with frequent appends.
    """
    with open_file(path, "a") as f:
        f.write(content)


def read_text(path: str | Path) -> str:
    """Read and return the full text content of ``path``."""
    with open_file(path, "r") as f:
        return f.read()  # type: ignore[no-any-return]


def list_files(path: str | Path, pattern: str = "*") -> list[str]:
    """List files under ``path`` matching ``pattern``.

    Returns fully-qualified paths (including the cloud protocol prefix when
    applicable) so results can be passed back into the other functions in this
    module.
    """
    path_str = str(path)
    if is_cloud_path(path_str):
        fsspec = _import_fsspec()
        fs, _ = fsspec.core.url_to_fs(path_str)
        glob_pattern = f"{path_str.rstrip('/')}/{pattern}"
        return [fs.unstrip_protocol(p) for p in fs.glob(glob_pattern)]
    return [str(p) for p in Path(path_str).glob(pattern)]


def copy_file(src: str | Path, dst: str | Path) -> None:
    """Copy a single file between any combination of local/cloud paths."""
    src_str, dst_str = str(src), str(dst)
    if is_cloud_path(src_str) or is_cloud_path(dst_str):
        fsspec = _import_fsspec()
        fsspec.copy(src_str, dst_str)
    else:
        shutil.copy2(src_str, dst_str)


def remove_dir(path: str | Path) -> None:
    """Recursively remove a directory and its contents."""
    path_str = str(path)
    if is_cloud_path(path_str):
        fsspec = _import_fsspec()
        fs, _ = fsspec.core.url_to_fs(path_str)
        fs.rm(path_str, recursive=True)
    else:
        shutil.rmtree(path_str)


def ensure_dir(path: str | Path) -> None:
    """Create ``path`` if it does not exist. No-op for cloud paths.

    Cloud object stores have no real directory hierarchy, so directory creation
    is implicit when an object is written.
    """
    path_str = str(path)
    if is_cloud_path(path_str):
        return
    os.makedirs(path_str, exist_ok=True)


def path_exists(path: str | Path) -> bool:
    """Return True if ``path`` exists locally or in cloud storage."""
    path_str = str(path)
    if is_cloud_path(path_str):
        fsspec = _import_fsspec()
        fs, _ = fsspec.core.url_to_fs(path_str)
        return bool(fs.exists(path_str))
    return Path(path_str).exists()


def get_path_name(path: str | Path) -> str:
    """Return the final component of ``path`` (no trailing slash)."""
    path_str = str(path).rstrip("/")
    return path_str.rsplit("/", 1)[-1]


def get_path_stem(path: str | Path) -> str:
    """Return the final component of ``path`` without its file extension."""
    name = get_path_name(path)
    if "." in name:
        return name.rsplit(".", 1)[0]
    return name


def join_path(base: str | Path, *parts: str | Path) -> str:
    """Join ``base`` and ``parts`` with a single separating slash.

    Works for both local relative paths and cloud URIs (which Path cannot
    handle because of the ``://`` scheme separator).
    """
    base_str = str(base).rstrip("/")
    pieces = [str(p).strip("/") for p in parts if str(p).strip("/")]
    if not pieces:
        return base_str
    return base_str + "/" + "/".join(pieces)

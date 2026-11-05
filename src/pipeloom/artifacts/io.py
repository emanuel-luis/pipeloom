"""Atomic JSON files."""

from __future__ import annotations

import errno
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

REPLACE_RETRIES = 10
REPLACE_BACKOFF = 0.05
NO_LINK_ERRNOS = frozenset(
    {errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EXDEV, errno.ENOSYS}
)
"""What `os.link` raises on a file system without hard links."""


def json_bytes(data: Any) -> bytes:
    """Indented by 2, UTF-8, LF, with a trailing newline."""
    return (json.dumps(data, indent=2, allow_nan=False, ensure_ascii=False) + "\n").encode("utf-8")


def write_json_atomic(path: Path, data: Any) -> int:
    """Write `data` next to `path`, then replace it. Returns the bytes written."""
    content = json_bytes(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(content)
    try:
        for attempt in range(REPLACE_RETRIES):
            try:
                os.replace(tmp, path)
                break
            except PermissionError:  # Windows: a reader holds the file
                if sys.platform != "win32" or attempt == REPLACE_RETRIES - 1:
                    raise
                time.sleep(REPLACE_BACKOFF)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return len(content)


def write_json_once(path: Path, data: Any) -> bool:
    """Create `path` with `data` unless it exists; True when this call created it.

    The first terminal attempt file wins: the content goes to a temp file, which is then linked
    (POSIX) or renamed (Windows) to `path`. Both fail when `path` exists, and the temp file is
    removed either way. A file system that cannot hard-link (a FUSE volume) makes the link fail
    with one of `NO_LINK_ERRNOS`; the content is then written with `O_CREAT | O_EXCL`, which
    is exclusive too but not atomic: a reader may see a partial file for a moment.
    """
    content = json_bytes(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(content)
    try:
        if sys.platform == "win32":
            os.rename(tmp, path)
        else:
            os.link(tmp, path)
    except FileExistsError:
        return False
    except OSError as exc:
        if exc.errno not in NO_LINK_ERRNOS:
            raise
        return _create_exclusive(path, content)
    finally:
        tmp.unlink(missing_ok=True)
    return True


def _create_exclusive(path: Path, content: bytes) -> bool:
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
    except FileExistsError:
        return False
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return True

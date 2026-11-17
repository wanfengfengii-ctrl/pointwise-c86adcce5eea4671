import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from lake.objects.integrity import digest_bytes
from lake.objects.paths import object_path


@dataclass(frozen=True)
class StagedObject:
    kind: str
    digest: str
    size: int
    path: Path


def stage_bytes(paths, kind, data):
    if type(data) is not bytes:
        raise ValueError("object data must be bytes")
    digest = digest_bytes(data)
    object_path(paths, kind, digest)
    fd, name = tempfile.mkstemp(prefix="object-", dir=paths.staging)
    path = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        return StagedObject(kind, digest, len(data), path)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def discard_staged(staged):
    if staged is not None:
        staged.path.unlink(missing_ok=True)

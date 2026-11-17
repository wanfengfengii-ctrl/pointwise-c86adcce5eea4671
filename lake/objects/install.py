import os
from dataclasses import dataclass

from lake.objects.integrity import digest_bytes, read_verified_bytes
from lake.objects.paths import object_path
from lake.objects.staging import discard_staged


@dataclass(frozen=True)
class InstalledObject:
    kind: str
    digest: str
    size: int
    address: str


def install_object(paths, staged):
    destination = object_path(paths, staged.kind, staged.digest)
    try:
        data = staged.path.read_bytes()
        if len(data) != staged.size or digest_bytes(data) != staged.digest:
            raise OSError("staged object changed before installation")
        try:
            os.link(staged.path, destination)
        except FileExistsError:
            read_verified_bytes(paths, staged.kind, staged.digest, staged.size)
        else:
            read_verified_bytes(paths, staged.kind, staged.digest, staged.size)
        return InstalledObject(staged.kind, staged.digest,
                               staged.size, str(destination))
    finally:
        discard_staged(staged)

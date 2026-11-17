import re
from dataclasses import dataclass
from pathlib import Path

_DIGEST = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class StoragePaths:
    root: Path
    database: Path
    objects: Path
    staging: Path


def resolve_storage_paths(root):
    root = Path(root).absolute()
    return StoragePaths(root, root / "catalog.sqlite3",
                        root / "objects", root / "staging")


def ensure_storage_root(paths):
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.staging.mkdir(exist_ok=True)
    for kind in ("data", "index"):
        (paths.objects / kind).mkdir(parents=True, exist_ok=True)


def object_path(paths, kind, digest):
    if kind not in ("data", "index"):
        raise ValueError("unknown object kind")
    if type(digest) is not str or not _DIGEST.fullmatch(digest):
        raise ValueError("invalid object digest")
    return paths.objects / kind / digest

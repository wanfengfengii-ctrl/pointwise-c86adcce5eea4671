import hashlib
from dataclasses import asdict, dataclass

from lake.objects.paths import object_path


class CorruptObject(OSError):
    def __init__(self, address, code, reason):
        self.address = str(address)
        self.code = code
        self.reason = reason
        super().__init__(self.address + ": " + code + ": " + reason)


@dataclass(frozen=True)
class ObjectCheck:
    address: str
    kind: str
    digest: str
    ok: bool
    code: str
    reason: str

    def to_dict(self):
        return asdict(self)


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def read_verified_bytes(paths, kind, digest, size=None):
    path = object_path(paths, kind, digest)
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise CorruptObject(path, "missing", str(exc)) from exc
    except OSError as exc:
        raise CorruptObject(path, "unreadable", str(exc)) from exc
    if digest_bytes(data) != digest:
        raise CorruptObject(path, "digest_mismatch", "SHA-256 mismatch")
    if size is not None and len(data) != size:
        raise CorruptObject(path, "size_mismatch", "descriptor size mismatch")
    return data


def check_object(paths, kind, digest, size=None):
    address = str(object_path(paths, kind, digest))
    try:
        read_verified_bytes(paths, kind, digest, size)
    except CorruptObject as exc:
        return ObjectCheck(address, kind, digest, False, exc.code, exc.reason)
    return ObjectCheck(address, kind, digest, True, "ok", "")

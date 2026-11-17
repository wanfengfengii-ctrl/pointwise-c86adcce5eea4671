import uuid
from dataclasses import dataclass

from lake.admission.validation import validate_id
from lake.catalog.schema import FIXED_BUCKET_COUNT
from lake.segments.codec import encode_segment_meta, decode_segment_meta


@dataclass(frozen=True)
class Manifest:
    id: int
    commit: int
    segments: tuple
    bucket_count: int = FIXED_BUCKET_COUNT


@dataclass(frozen=True)
class SnapshotBinding:
    id: str
    name: str
    manifest_id: int


def get_manifest(catalog, manifest_id):
    row = catalog.connection.execute(
        "SELECT business_commit, bucket_count FROM manifests WHERE id=?",
        (manifest_id,),
    ).fetchone()
    if row is None:
        raise ValueError("unknown manifest")
    if row[1] not in (2, 3):
        raise ValueError("unsupported manifest layout")
    segments = tuple(
        decode_segment_meta(entry[0])
        for entry in catalog.connection.execute(
            "SELECT descriptor FROM manifest_segments "
            "WHERE manifest_id=? ORDER BY position", (manifest_id,)
        )
    )
    return Manifest(manifest_id, row[0], segments, row[1])


def get_head(catalog):
    row = catalog.connection.execute(
        "SELECT manifest_id FROM current_head WHERE singleton=1"
    ).fetchone()
    return get_manifest(catalog, row[0])


def create_manifest(catalog, commit, segments, bucket_count=FIXED_BUCKET_COUNT):
    if not catalog.connection.in_transaction:
        raise RuntimeError("manifest publication needs a transaction")
    if type(commit) is not int or commit < 0:
        raise ValueError("invalid manifest commit")
    if bucket_count not in (2, 3):
        raise ValueError("invalid manifest bucket count")
    descriptors = [encode_segment_meta(segment).decode("utf-8")
                   for segment in segments]
    cursor = catalog.connection.execute(
        "INSERT INTO manifests(business_commit, bucket_count) VALUES (?, ?)",
        (commit, bucket_count),
    )
    manifest_id = cursor.lastrowid
    catalog.connection.executemany(
        "INSERT INTO manifest_segments VALUES (?, ?, ?)",
        [(manifest_id, position, text)
         for position, text in enumerate(descriptors)],
    )
    return manifest_id


def freeze_manifest(catalog, name):
    validate_id(name)
    with catalog.write_transaction():
        head = get_head(catalog)
        row = catalog.connection.execute(
            "SELECT id, manifest_id FROM snapshots WHERE name=?", (name,)
        ).fetchone()
        if row:
            if row[1] != head.id:
                raise ValueError("snapshot name already bound to another manifest")
            return row[0]
        snapshot_id = uuid.uuid4().hex
        catalog.connection.execute(
            "INSERT INTO snapshots VALUES (?, ?, ?)",
            (snapshot_id, name, head.id),
        )
        return snapshot_id


def resolve_snapshot(catalog, snapshot):
    validate_id(snapshot)
    row = catalog.connection.execute(
        "SELECT id, name, manifest_id FROM snapshots WHERE id=?", (snapshot,)
    ).fetchone()
    if row is None:
        raise ValueError("unknown snapshot")
    return SnapshotBinding(*row)

from dataclasses import dataclass

from lake.catalog.manifests import get_head, create_manifest


@dataclass(frozen=True)
class CommitReceipt:
    request_id: str
    commit: int
    manifest_id: int
    operation_count: int

    def to_dict(self):
        return dict(vars(self))


class HeadChanged(Exception):
    pass


def find_receipt(catalog, request_id, fingerprint=None):
    row = catalog.connection.execute(
        "SELECT fingerprint, commit_id, manifest_id, operation_count "
        "FROM receipts WHERE request_id=?", (request_id,)
    ).fetchone()
    if row is None:
        return None
    if fingerprint is not None and fingerprint != row[0]:
        raise ValueError("request id already used with different content")
    return CommitReceipt(request_id, row[1], row[2], row[3])


def next_candidate_commit(catalog):
    with catalog.read_transaction():
        head = get_head(catalog)
        return head, head.commit + 1


def publish_batch(catalog, request_id, fingerprint, input_head,
                  candidate, segments, operation_count, emit):
    with catalog.write_transaction():
        previous = find_receipt(catalog, request_id, fingerprint)
        if previous is not None:
            return previous
        head = get_head(catalog)
        if head.id != input_head.id or candidate != head.commit + 1:
            raise HeadChanged()
        manifest_id = create_manifest(
            catalog, candidate, head.segments + tuple(segments)
        )
        emit("metadata_after_manifest", {"manifest_id": manifest_id})
        catalog.connection.execute(
            "INSERT INTO commits VALUES (?, ?, ?)",
            (candidate, manifest_id, request_id),
        )
        catalog.connection.execute(
            "INSERT INTO receipts VALUES (?, ?, ?, ?, ?)",
            (request_id, fingerprint, candidate, manifest_id, operation_count),
        )
        catalog.connection.execute(
            "UPDATE current_head SET manifest_id=? WHERE singleton=1",
            (manifest_id,),
        )
        return CommitReceipt(request_id, candidate, manifest_id, operation_count)

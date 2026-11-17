import hashlib

from lake.segments.codec import canonical_json_bytes


def canonical_batch_bytes(batch):
    operations = []
    for operation in batch.operations:
        entry = {"id": operation.id, "op": operation.kind}
        if operation.kind == "put":
            entry["payload"] = operation.payload
        operations.append(entry)
    return canonical_json_bytes({"version": 1, "operations": operations})


def batch_fingerprint(batch):
    return hashlib.sha256(canonical_batch_bytes(batch)).hexdigest()

import json
import zlib
from dataclasses import dataclass

from lake.admission.validation import (
    validate_operation, validate_unique_ids,
)
from lake.catalog.schema import FIXED_BUCKET_COUNT


@dataclass(frozen=True)
class Operation:
    id: str
    kind: str
    payload: object = None


@dataclass(frozen=True)
class NormalizedBatch:
    operations: tuple

    def __len__(self):
        return len(self.operations)


def put(id, payload):
    return Operation(id, "put", payload)


def delete(id):
    return Operation(id, "delete")


def bucket_for(id):
    return zlib.crc32(id.encode("utf-8")) % FIXED_BUCKET_COUNT


def normalize_batch(operations):
    try:
        source = tuple(operations)
    except TypeError as exc:
        raise ValueError("operations must be iterable") from exc
    if not source:
        raise ValueError("empty batch")
    normalized = []
    for operation in source:
        if type(operation) is not Operation:
            raise ValueError("expected put/delete operation")
        validate_operation(operation)
        payload = None
        if operation.kind == "put":
            payload = json.loads(json.dumps(
                operation.payload, ensure_ascii=False, sort_keys=True,
                allow_nan=False, separators=(",", ":"),
            ))
        normalized.append(Operation(operation.id, operation.kind, payload))
    validate_unique_ids(normalized)
    return NormalizedBatch(tuple(sorted(normalized, key=lambda op: op.id)))

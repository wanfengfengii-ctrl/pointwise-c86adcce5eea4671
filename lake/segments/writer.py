from lake.admission.batch import bucket_for
from lake.objects.staging import stage_bytes, discard_staged
from lake.objects.install import install_object
from lake.segments.codec import (
    SegmentMeta, SegmentRecord, canonical_json_bytes, encode_record, decode_record,
)
from lake.segments.index import build_index, encode_index, validate_index
from lake.segments.reader import open_segment


def write_segment(paths, bucket, records, emit, index_stride=8):
    if type(bucket) is not int or bucket not in (0, 1):
        raise ValueError("invalid fixed bucket")
    records = tuple(sorted(records, key=lambda row: (row.id, row.commit)))
    if not records:
        raise ValueError("empty segment")
    if any(bucket_for(record.id) != bucket for record in records):
        raise ValueError("record routed to wrong bucket")
    identities = [(row.id, row.commit) for row in records]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate logical record identity")
    lines = tuple(encode_record(row) for row in records)
    checked = tuple(decode_record(line) for line in lines)
    index = build_index(checked, lines, index_stride)
    validate_index(index, checked, lines)
    staged_data = None
    staged_index = None
    try:
        staged_data = stage_bytes(paths, "data", b"".join(lines))
        staged_index = stage_bytes(paths, "index", encode_index(index))
        for staged in (staged_data, staged_index):
            emit("before_object_install", {
                "bucket": bucket, "kind": staged.kind, "digest": staged.digest,
            })
            install_object(paths, staged)
            emit("object_installed", {
                "bucket": bucket, "kind": staged.kind, "digest": staged.digest,
            })
        meta = SegmentMeta(
            bucket, len(records), records[0].id, records[-1].id,
            staged_data.digest, staged_index.digest,
            staged_data.size, staged_index.size,
        )
        open_segment(paths, meta)
        emit("segment_installed", {"bucket": bucket, "meta": meta})
        return meta
    finally:
        discard_staged(staged_data)
        discard_staged(staged_index)


def write_batch_segments(paths, batch, commit, emit,
                         segment_rows=128, index_stride=8):
    if type(segment_rows) is not int or segment_rows <= 0:
        raise ValueError("segment_rows must be positive")
    grouped = {0: [], 1: []}
    for operation in batch.operations:
        value = (canonical_json_bytes(operation.payload).decode("utf-8")
                 if operation.kind == "put" else None)
        grouped[bucket_for(operation.id)].append(
            SegmentRecord(operation.id, commit, operation.kind, value)
        )
    segments = []
    for bucket in (0, 1):
        rows = grouped[bucket]
        for start in range(0, len(rows), segment_rows):
            segments.append(write_segment(
                paths, bucket, rows[start:start + segment_rows],
                emit, index_stride,
            ))
    return tuple(segments)

"""Durable state for the one-shot two-to-three bucket rebucket task.

The active layout is the bucket count new admissions route by. During a
rebucket the head manifest keeps its old-layout segments *and* receives new
target-layout segments; every segment descriptor therefore carries the bucket
count it was written for. Reads merge both layouts and count an identical
``(id, logical commit)`` replica once. The task row pins the finite source
sequence and the copy cursor so steps resume without replaying a copied prefix.
"""

import json
import uuid
from dataclasses import dataclass

from lake.admission.batch import bucket_for
from lake.admission.validation import validate_request_id
from lake.catalog.layout import (
    COPYING, FINISHED, IDLE, READY, get_layout, update_layout,
)
from lake.catalog.manifests import create_manifest, get_head
from lake.catalog.schema import FIXED_BUCKET_COUNT, TARGET_BUCKET_COUNT
from lake.objects.integrity import CorruptObject
from lake.segments.codec import (
    decode_segment_meta, encode_segment_meta, strict_json_loads,
)
from lake.segments.reader import open_segment
from lake.segments.writer import write_segment


@dataclass(frozen=True)
class RebucketTask:
    request_id: str
    task_id: str
    total: int
    source_commit: int
    source_segments: tuple


def _encode_sources(segments):
    return json.dumps(
        [encode_segment_meta(segment).decode("utf-8") for segment in segments],
        ensure_ascii=False, separators=(",", ":"),
    )


def _decode_sources(text):
    decoded = strict_json_loads(text)
    if type(decoded) is not list:
        raise RuntimeError("corrupt rebucket source descriptor")
    return tuple(
        decode_segment_meta(entry.encode("utf-8")) for entry in decoded
    )


def _load_task(catalog, task_id):
    row = catalog.connection.execute(
        "SELECT request_id, task_id, total, source_manifest_id, source_segments "
        "FROM layout_tasks WHERE task_id=?", (task_id,)
    ).fetchone()
    if row is None:
        raise RuntimeError("rebucket task is missing")
    commit = catalog.connection.execute(
        "SELECT business_commit FROM manifests WHERE id=?", (row[3],)
    ).fetchone()
    if commit is None:
        raise RuntimeError("rebucket source manifest is missing")
    return RebucketTask(row[0], row[1], row[2], commit[0],
                        _decode_sources(row[4]))


def begin_rebucket(catalog, request_id):
    validate_request_id(request_id)
    with catalog.write_transaction():
        layout = get_layout(catalog)
        if layout.task_id is not None:
            task = _load_task(catalog, layout.task_id)
            if task.request_id == request_id:
                return task.task_id
            raise ValueError("a rebucket task already exists")
        head = get_head(catalog)
        if head.bucket_count != FIXED_BUCKET_COUNT:
            raise ValueError("table is not in the two-bucket layout")
        sources = tuple(head.segments)
        if any(segment.bucket_count != FIXED_BUCKET_COUNT
               for segment in sources):
            raise RuntimeError("source segment layout does not match manifest")
        total = sum(segment.count for segment in sources)
        task_id = uuid.uuid4().hex
        catalog.connection.execute(
            "INSERT INTO layout_tasks VALUES (?, ?, ?, ?, ?, ?, ?)",
            (request_id, task_id, FIXED_BUCKET_COUNT, TARGET_BUCKET_COUNT,
             total, head.id, _encode_sources(sources)),
        )
        # Routing moves to the target layout immediately, while the pinned
        # source segments stay readable in the same head until cutover.
        manifest_id = create_manifest(
            catalog, head.commit, sources, TARGET_BUCKET_COUNT
        )
        catalog.connection.execute(
            "UPDATE current_head SET manifest_id=? WHERE singleton=1",
            (manifest_id,),
        )
        phase = READY if total == 0 else COPYING
        update_layout(catalog, TARGET_BUCKET_COUNT, phase, task_id, request_id,
                      total, 0, 0, 0)
        return task_id


def rebucket_status(catalog):
    layout = get_layout(catalog)
    return {
        "task_id": layout.task_id,
        "phase": layout.phase,
        "source_buckets": FIXED_BUCKET_COUNT,
        "target_buckets": TARGET_BUCKET_COUNT,
        "processed": layout.processed,
        "total": layout.total,
    }


def _valid_limit(limit):
    if type(limit) is not int or limit <= 0:
        raise ValueError("limit must be a positive integer")
    return limit


def _collect_slice(catalog, task, layout, limit):
    segments = task.source_segments
    position = layout.cursor_segment
    offset = layout.cursor_offset
    records = []
    remaining = limit
    while remaining > 0 and position < len(segments):
        segment = open_segment(catalog.paths, segments[position],
                               task.source_commit)
        rows = segment.records
        if offset > len(rows):
            raise RuntimeError("rebucket cursor is outside its source segment")
        take = rows[offset:offset + remaining]
        if not take:
            raise RuntimeError("rebucket source ends before its recorded total")
        records.extend(take)
        remaining -= len(take)
        offset += len(take)
        if offset == len(rows):
            position += 1
            offset = 0
    return tuple(records), position, offset


def _target_outputs(catalog, records, emit, segment_rows, index_stride):
    grouped = {bucket: [] for bucket in range(TARGET_BUCKET_COUNT)}
    seen = {}
    for record in records:
        bucket = bucket_for(record.id, TARGET_BUCKET_COUNT)
        key = (bucket, record.id, record.commit)
        previous = seen.get(key)
        if previous is not None:
            if previous != record:
                raise CorruptObject(
                    catalog.paths.database, "conflicting_revision",
                    "same logical identity has different content",
                )
            continue
        seen[key] = record
        grouped[bucket].append(record)
    outputs = []
    for bucket in range(TARGET_BUCKET_COUNT):
        rows = sorted(grouped[bucket], key=lambda row: (row.id, row.commit))
        for start in range(0, len(rows), segment_rows):
            outputs.append(write_segment(
                catalog.paths, bucket, rows[start:start + segment_rows],
                emit, index_stride, TARGET_BUCKET_COUNT,
            ))
    return tuple(outputs)


def _retired_segments_exact(head, task):
    retired = tuple(segment for segment in head.segments
                    if segment.bucket_count == FIXED_BUCKET_COUNT)
    return retired == task.source_segments


def step_rebucket(catalog, limit, emit, segment_rows=128, index_stride=8):
    _valid_limit(limit)
    with catalog.read_transaction():
        layout = get_layout(catalog)
        if layout.phase == IDLE:
            raise ValueError("rebucket has not begun")
        task = _load_task(catalog, layout.task_id)
    if layout.phase in (READY, FINISHED):
        if layout.phase == READY:
            with catalog.read_transaction():
                head = get_head(catalog)
            if not _retired_segments_exact(head, task):
                raise RuntimeError(
                    "head holds uncopied retired-layout segments"
                )
        return {"processed": 0, "ready": True}
    records, position, offset = _collect_slice(catalog, task, layout, limit)
    if not records:
        raise RuntimeError("rebucket step found no source records to continue")
    outputs = _target_outputs(
        catalog, records, emit, segment_rows, index_stride
    )
    for meta in outputs:
        open_segment(catalog.paths, meta, task.source_commit)
    emit("rebucket_prepared", {
        "task_id": task.task_id,
        "start": layout.processed,
        "stop": layout.processed + len(records),
    })
    with catalog.write_transaction():
        current = get_layout(catalog)
        if (current.task_id != task.task_id or current.phase != COPYING
                or current.processed != layout.processed
                or current.cursor_segment != layout.cursor_segment
                or current.cursor_offset != layout.cursor_offset):
            raise RuntimeError("rebucket progress changed concurrently")
        head = get_head(catalog)
        if head.bucket_count != TARGET_BUCKET_COUNT:
            raise RuntimeError("routing left the target layout")
        processed = current.processed + len(records)
        phase = READY if processed == current.total else COPYING
        manifest_id = create_manifest(
            catalog, head.commit, head.segments + outputs,
            TARGET_BUCKET_COUNT,
        )
        catalog.connection.execute(
            "UPDATE current_head SET manifest_id=? WHERE singleton=1",
            (manifest_id,),
        )
        update_layout(catalog, TARGET_BUCKET_COUNT, phase, task.task_id,
                      task.request_id, current.total, processed,
                      position, offset)
        return {"processed": len(records), "ready": phase == READY}


def finish_rebucket(catalog):
    with catalog.read_transaction():
        layout = get_layout(catalog)
        if layout.phase == IDLE:
            raise ValueError("rebucket has not begun")
        if layout.phase == FINISHED:
            return True
        if layout.phase != READY or layout.processed != layout.total:
            return False
        task = _load_task(catalog, layout.task_id)
        head = get_head(catalog)
        target = tuple(segment for segment in head.segments
                       if segment.bucket_count == TARGET_BUCKET_COUNT)
    if sum(segment.count for segment in task.source_segments) != task.total:
        raise RuntimeError("rebucket source total is inconsistent")
    # Cutover is only safe when every target object is actually readable. This
    # runs outside the write transaction; io and verification hold no lock.
    for meta in target:
        open_segment(catalog.paths, meta, head.commit)
    with catalog.write_transaction():
        current = get_layout(catalog)
        if current.phase == FINISHED:
            return True
        if current.phase != READY or current.processed != current.total:
            return False
        head = get_head(catalog)
        if head.bucket_count != TARGET_BUCKET_COUNT:
            raise RuntimeError("routing left the target layout")
        if not _retired_segments_exact(head, task):
            # A retired-layout segment outside the pinned source would have no
            # guaranteed target copy; refusing keeps the task resumable rather
            # than silently dropping a logical fact.
            raise RuntimeError("head holds uncopied retired-layout segments")
        # Every pinned source fact was copied with its original logical commit,
        # and admissions during the task already routed by the target layout,
        # so only target segments belong in the final head.
        target = tuple(segment for segment in head.segments
                       if segment.bucket_count == TARGET_BUCKET_COUNT)
        if any(meta not in target for meta in head.segments
               if meta.bucket_count not in (FIXED_BUCKET_COUNT,
                                            TARGET_BUCKET_COUNT)):
            raise RuntimeError("unexpected layout mix at cutover")
        manifest_id = create_manifest(
            catalog, head.commit, target, TARGET_BUCKET_COUNT
        )
        catalog.connection.execute(
            "UPDATE current_head SET manifest_id=? WHERE singleton=1",
            (manifest_id,),
        )
        update_layout(catalog, TARGET_BUCKET_COUNT, FINISHED, task.task_id,
                      task.request_id, current.total, current.total,
                      current.cursor_segment, current.cursor_offset)
        return True

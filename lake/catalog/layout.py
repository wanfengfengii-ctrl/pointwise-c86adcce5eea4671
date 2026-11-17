"""Durable table layout: active bucket count and the one-shot rebucket task."""
from dataclasses import dataclass

from lake.catalog.schema import FIXED_BUCKET_COUNT, TARGET_BUCKET_COUNT


IDLE = "idle"
COPYING = "copying"
READY = "ready"
FINISHED = "finished"
PHASES = (IDLE, COPYING, READY, FINISHED)


@dataclass(frozen=True)
class LayoutState:
    bucket_count: int
    phase: str
    task_id: object
    request_id: object
    total: int
    processed: int
    cursor_segment: int
    cursor_offset: int


def get_layout(catalog):
    row = catalog.connection.execute(
        "SELECT bucket_count, phase, task_id, task_request, total, processed, "
        "cursor_segment, cursor_offset FROM layout_state WHERE singleton=1"
    ).fetchone()
    if row is None:
        raise RuntimeError("layout state is missing")
    if row[0] not in (FIXED_BUCKET_COUNT, TARGET_BUCKET_COUNT):
        raise RuntimeError("unsupported bucket count in catalog")
    if row[1] not in PHASES:
        raise RuntimeError("unsupported layout phase in catalog")
    return LayoutState(*row)


def update_layout(catalog, bucket_count, phase, task_id, request_id,
                  total, processed, cursor_segment=0, cursor_offset=0):
    if not catalog.connection.in_transaction:
        raise RuntimeError("layout update needs a transaction")
    catalog.connection.execute(
        "UPDATE layout_state SET bucket_count=?, phase=?, task_id=?, "
        "task_request=?, total=?, processed=?, cursor_segment=?, "
        "cursor_offset=? WHERE singleton=1",
        (bucket_count, phase, task_id, request_id, total, processed,
         cursor_segment, cursor_offset),
    )

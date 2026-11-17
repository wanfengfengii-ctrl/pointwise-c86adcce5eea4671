from dataclasses import dataclass

from lake.objects.integrity import CorruptObject
from lake.reads.scan import merge_record_streams
from lake.segments.reader import open_segment
from lake.segments.writer import write_segment


@dataclass(frozen=True)
class PreparedCompaction:
    plan: object
    outputs: tuple


def merge_revisions(streams):
    previous = None
    for record in merge_record_streams(streams):
        if previous is not None and (
            record.id, record.commit
        ) == (previous.id, previous.commit):
            if previous != record:
                raise CorruptObject(
                    "compaction", "conflicting_revision",
                    "same logical identity has different content",
                )
            continue
        yield record
        previous = record


def prepare_compaction(catalog, plan, emit, index_stride=8):
    streams = [
        open_segment(catalog.paths, meta, plan.commit).iter_records()
        for meta in plan.inputs
    ]
    records = tuple(merge_revisions(streams))
    emit("compaction_inputs_read", {"head_id": plan.head_id,
                                     "bucket": plan.bucket})
    output = write_segment(
        catalog.paths, plan.bucket, records, emit, index_stride
    )
    return PreparedCompaction(plan, (output,))

from dataclasses import dataclass

from lake.reads.scan import merge_revisions
from lake.segments.reader import open_segment
from lake.segments.writer import write_segment


@dataclass(frozen=True)
class PreparedCompaction:
    plan: object
    outputs: tuple


def prepare_compaction(catalog, plan, emit, index_stride=8):
    streams = [
        open_segment(catalog.paths, meta, plan.commit).iter_records()
        for meta in plan.inputs
    ]
    records = tuple(merge_revisions(streams, "compaction"))
    emit("compaction_inputs_read", {"head_id": plan.head_id,
                                     "bucket": plan.bucket})
    output = write_segment(
        catalog.paths, plan.bucket, records, emit, index_stride,
        plan.bucket_count,
    )
    return PreparedCompaction(plan, (output,))

import heapq
import itertools

from lake.objects.integrity import CorruptObject
from lake.segments.reader import open_segment


def merge_record_streams(streams):
    return heapq.merge(*streams, key=lambda record: (record.id, record.commit))


def scan_visible_records(paths, binding, emit):
    streams = []
    for bucket in (0, 1):
        for meta in binding.manifest.segments:
            if meta.bucket == bucket:
                segment = open_segment(paths, meta, binding.manifest.commit)
                streams.append(segment.iter_records())
        emit("scan_bucket", {"bucket": bucket,
                             "manifest_id": binding.manifest.id})
    merged = merge_record_streams(streams)
    for id, group in itertools.groupby(merged, key=lambda record: record.id):
        winner = None
        for record in group:
            if winner is None or record.commit > winner.commit:
                winner = record
            elif record.commit == winner.commit and record != winner:
                raise CorruptObject(
                    paths.database, "conflicting_revision",
                    "same logical identity has different content",
                )
        if winner.kind == "put":
            yield winner

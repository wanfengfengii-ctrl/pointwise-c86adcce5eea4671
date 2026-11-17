import heapq
import itertools

from lake.objects.integrity import CorruptObject
from lake.segments.reader import open_segment


def merge_record_streams(streams):
    return heapq.merge(*streams, key=lambda record: (record.id, record.commit))


def merge_revisions(streams, address="merge"):
    """Merge sorted revision streams, yielding one record per logical fact.

    A record replicated into a new layout keeps its original business commit,
    so the same (id, commit) with identical content is one fact and counts once.
    """
    previous = None
    for record in merge_record_streams(streams):
        if previous is not None and (
            record.id, record.commit
        ) == (previous.id, previous.commit):
            if previous != record:
                raise CorruptObject(
                    address, "conflicting_revision",
                    "same logical identity has different content",
                )
            continue
        yield record
        previous = record


def scan_visible_records(paths, binding, emit):
    streams = []
    manifest = binding.manifest
    buckets = sorted(
        set(range(manifest.bucket_count))
        | {meta.bucket for meta in manifest.segments}
    )
    for bucket in buckets:
        for meta in manifest.segments:
            if meta.bucket == bucket:
                segment = open_segment(paths, meta, manifest.commit)
                streams.append(segment.iter_records())
        emit("scan_bucket", {"bucket": bucket,
                             "manifest_id": manifest.id})
    merged = merge_revisions(streams, paths.database)
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

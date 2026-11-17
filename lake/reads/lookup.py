from lake.admission.batch import bucket_for
from lake.objects.integrity import CorruptObject
from lake.segments.reader import open_segment


def lookup_record(paths, binding, id):
    winner = None
    for meta in binding.manifest.segments:
        if bucket_for(id, meta.bucket_count) != meta.bucket:
            continue
        segment = open_segment(paths, meta, binding.manifest.commit)
        for record in segment.read_id(id):
            winner = _merge_revision(paths, winner, record)
    return winner


def _merge_revision(paths, winner, record):
    if winner is None:
        return record
    if record.commit > winner.commit:
        return record
    if record.commit == winner.commit and record != winner:
        raise CorruptObject(
            paths.database, "conflicting_revision",
            "same logical identity has different content",
        )
    return winner

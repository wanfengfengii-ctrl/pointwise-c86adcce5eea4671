from lake.admission.batch import bucket_for
from lake.objects.integrity import CorruptObject
from lake.segments.reader import open_segment


def lookup_record(paths, binding, id):
    bucket = bucket_for(id)
    winner = None
    for meta in binding.manifest.segments:
        if meta.bucket != bucket:
            continue
        segment = open_segment(paths, meta, binding.manifest.commit)
        for record in segment.read_id(id):
            if winner is None or record.commit > winner.commit:
                winner = record
            elif record.commit == winner.commit and record != winner:
                raise CorruptObject(
                    paths.database, "conflicting_revision",
                    "same logical identity has different content",
                )
    return winner

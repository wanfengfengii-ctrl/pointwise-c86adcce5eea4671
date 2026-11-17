from dataclasses import dataclass

from lake.admission.batch import bucket_for
from lake.objects.integrity import CorruptObject, read_verified_bytes
from lake.objects.paths import object_path
from lake.segments.codec import decode_record, decode_segment_meta, encode_segment_meta
from lake.segments.index import decode_index, validate_index, locate_block


@dataclass(frozen=True)
class VerifiedSegment:
    meta: object
    data: bytes
    records: tuple
    index: object

    def iter_records(self):
        return iter(self.records)

    def read_id(self, id):
        if id < self.meta.min_id or id > self.meta.max_id:
            return ()
        _, start, stop = locate_block(self.index, id)
        result = []
        for line in self.data[start:stop].splitlines(keepends=True):
            record = decode_record(line)
            if record.id == id:
                result.append(record)
            elif record.id > id:
                break
        return tuple(result)


def open_segment(paths, meta, max_commit=None):
    decode_segment_meta(encode_segment_meta(meta))
    data = read_verified_bytes(paths, "data", meta.data_digest, meta.data_size)
    index_data = read_verified_bytes(
        paths, "index", meta.index_digest, meta.index_size
    )
    try:
        lines = tuple(data.splitlines(keepends=True))
        records = tuple(decode_record(line) for line in lines)
        if len(records) != meta.count or not records:
            raise ValueError("segment record count mismatch")
        previous = None
        for record in records:
            key = (record.id, record.commit)
            if previous is not None and key <= previous:
                raise ValueError("records must have strictly increasing identity")
            if bucket_for(record.id, meta.bucket_count) != meta.bucket:
                raise ValueError("record belongs to another bucket")
            if max_commit is not None and record.commit > max_commit:
                raise ValueError("record exceeds manifest commit")
            previous = key
        if (records[0].id, records[-1].id) != (meta.min_id, meta.max_id):
            raise ValueError("segment ID bounds mismatch")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise CorruptObject(
            object_path(paths, "data", meta.data_digest),
            "data_structure", str(exc),
        ) from exc
    try:
        index = decode_index(index_data)
        validate_index(index, records, lines)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise CorruptObject(
            object_path(paths, "index", meta.index_digest),
            "index_structure", str(exc),
        ) from exc
    return VerifiedSegment(meta, data, records, index)

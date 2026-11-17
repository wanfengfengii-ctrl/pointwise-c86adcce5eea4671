from lake.admission.validation import validate_id
from lake.reads.binding import bind_read
from lake.reads.lookup import lookup_record
from lake.reads.scan import scan_visible_records


def get_row(catalog, id, snapshot=None):
    validate_id(id)
    binding = bind_read(catalog, snapshot)
    record = lookup_record(catalog.paths, binding, id)
    if record is None or record.kind == "delete":
        return None
    return record.payload


def scan_rows(catalog, snapshot, emit):
    binding = bind_read(catalog, snapshot)
    return ((record.id, record.payload)
            for record in scan_visible_records(catalog.paths, binding, emit))

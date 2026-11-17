from lake.admission.batch import normalize_batch
from lake.admission.validation import validate_request_id
from lake.admission.fingerprint import batch_fingerprint
from lake.catalog.store import Catalog
from lake.catalog.commits import (
    HeadChanged, find_receipt, next_candidate_commit, publish_batch,
)
from lake.catalog.manifests import freeze_manifest
from lake.segments.writer import write_batch_segments
from lake.reads.api import get_row, scan_rows
from lake.maintenance.api import compact_bucket, verify_objects
from lake.rebucket.api import (
    begin_rebucket, finish_rebucket, rebucket_status, step_rebucket,
)


class Store:
    def __init__(self, path, *, hook=None, segment_rows=128, index_stride=8):
        for name, value in (
            ("segment_rows", segment_rows), ("index_stride", index_stride)
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(name + " must be a positive integer")
        if hook is not None and not callable(hook):
            raise ValueError("hook must be callable")
        self.catalog = Catalog(path)
        self.paths = self.catalog.paths
        self.hook = hook
        self.segment_rows = segment_rows
        self.index_stride = index_stride
        self.closed = False

    def _ensure_open(self):
        if self.closed:
            raise RuntimeError("store is closed")

    def _emit(self, stage, details):
        if self.hook is not None:
            self.hook(stage, details)

    def close(self):
        if not self.closed:
            self.catalog.close()
            self.closed = True

    def __enter__(self):
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def ingest(self, request_id, operations):
        self._ensure_open()
        validate_request_id(request_id)
        batch = normalize_batch(operations)
        fingerprint = batch_fingerprint(batch)
        self._emit("batch_normalized", {"request_id": request_id})
        while True:
            previous = find_receipt(self.catalog, request_id, fingerprint)
            if previous is not None:
                return previous
            head, candidate = next_candidate_commit(self.catalog)
            bucket_count = head.bucket_count
            segments = write_batch_segments(
                self.paths, batch, candidate, self._emit,
                self.segment_rows, self.index_stride, bucket_count,
            )
            self._emit("admission_prepared", {
                "head_id": head.id, "candidate": candidate,
            })
            self._emit("before_batch_publish", {"request_id": request_id})
            try:
                return publish_batch(
                    self.catalog, request_id, fingerprint, head,
                    candidate, segments, len(batch), self._emit,
                )
            except HeadChanged:
                continue

    def freeze(self, name):
        self._ensure_open()
        return freeze_manifest(self.catalog, name)

    def get(self, id, snapshot=None):
        self._ensure_open()
        return get_row(self.catalog, id, snapshot)

    def scan(self, snapshot=None):
        self._ensure_open()
        return scan_rows(self.catalog, snapshot, self._emit)

    def compact(self, bucket):
        self._ensure_open()
        return compact_bucket(
            self.catalog, bucket, self._emit, self.index_stride
        )

    def verify(self, snapshot=None):
        self._ensure_open()
        return verify_objects(self.catalog, snapshot)

    def begin_rebucket(self, request_id):
        self._ensure_open()
        return begin_rebucket(self.catalog, request_id)

    def step_rebucket(self, limit):
        self._ensure_open()
        return step_rebucket(
            self.catalog, limit, self._emit,
            self.segment_rows, self.index_stride,
        )

    def finish_rebucket(self):
        self._ensure_open()
        return finish_rebucket(self.catalog)

    def rebucket_status(self):
        self._ensure_open()
        return rebucket_status(self.catalog)

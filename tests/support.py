import copy
import tempfile
import unittest
import zlib

from lake.admission.api import Store
from lake.catalog.manifests import get_head
from lake.segments.reader import open_segment


def ids_for(bucket, count, prefix="sensor"):
    result = []
    number = 0
    while len(result) < count:
        id = prefix + "-" + str(number)
        if zlib.crc32(id.encode("utf-8")) % 2 == bucket:
            result.append(id)
        number += 1
    return result


class History:
    def __init__(self):
        self.commit = 0
        self.rows = {}

    def apply(self, receipt, changes):
        if receipt.commit != self.commit + 1:
            raise AssertionError("business commit is not the next successful commit")
        self.commit = receipt.commit
        for id, payload in changes:
            if payload is None:
                self.rows.pop(id, None)
            else:
                self.rows[id] = copy.deepcopy(payload)

    def frozen(self):
        return copy.deepcopy(self.rows)


class LakeCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="lake-tests-")
        self.addCleanup(self.temporary.cleanup)
        self.root = self.temporary.name
        self.store = Store(self.root, segment_rows=3, index_stride=2)
        self.addCleanup(lambda: self.store.close())

    def reopen(self):
        self.store.close()
        self.store = Store(self.root, segment_rows=3, index_stride=2)
        return self.store

    def other(self):
        store = Store(self.root, segment_rows=3, index_stride=2)
        self.addCleanup(store.close)
        return store

    def assert_rows(self, expected, snapshot=None):
        actual = list(self.store.scan(snapshot))
        self.assertEqual(actual, sorted(expected.items()))
        for id in expected:
            self.assertEqual(self.store.get(id, snapshot), expected[id])
        self.assertIsNone(self.store.get("definitely-absent", snapshot))

    def catalog_state(self):
        tables = ("manifests", "manifest_segments", "current_head",
                  "commits", "receipts", "snapshots")
        return tuple(
            (name, tuple(self.store.catalog.connection.execute(
                "SELECT * FROM " + name + " ORDER BY rowid"
            )))
            for name in tables
        )

    def head(self):
        return get_head(self.store.catalog)

    def facts(self, manifest, bucket):
        return {
            (record.id, record.commit, record.kind, record.value_json)
            for meta in manifest.segments if meta.bucket == bucket
            for record in open_segment(
                self.store.paths, meta, manifest.commit
            ).iter_records()
        }

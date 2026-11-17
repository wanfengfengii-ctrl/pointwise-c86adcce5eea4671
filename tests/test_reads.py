import copy
import random

from lake.admission.batch import put, delete
from lake.segments.reader import open_segment
from lake.catalog.manifests import create_manifest
from tests.support import LakeCase, History, ids_for


class TestReads(LakeCase):
    def reverse_physical_order(self):
        head = self.head()
        with self.store.catalog.write_transaction():
            manifest_id = create_manifest(
                self.store.catalog, head.commit, tuple(reversed(head.segments))
            )
            self.store.catalog.connection.execute(
                "UPDATE current_head SET manifest_id=? WHERE singleton=1",
                (manifest_id,),
            )

    def test_fixed_snapshot(self):
        self.store.ingest("first", [put("a", {"v": 1}), put("b", {"v": 2})])
        snapshot = self.store.freeze("daily")
        self.store.ingest("second", [put("a", {"v": 3}), delete("b")])
        self.reopen()
        self.assert_rows({"a": {"v": 1}, "b": {"v": 2}}, snapshot)
        self.assert_rows({"a": {"v": 3}})
        with self.assertRaises(ValueError):
            self.store.get("a", "unknown")
        with self.assertRaises(ValueError):
            self.store.scan("unknown")

    def test_freeze_name_conflict(self):
        snapshot = self.store.freeze("same/name")
        self.assertEqual(self.store.freeze("same/name"), snapshot)
        self.store.ingest("delete-empty", [delete("missing")])
        with self.assertRaises(ValueError):
            self.store.freeze("same/name")
        self.reopen()
        self.assert_rows({}, snapshot)
        with self.assertRaises(ValueError):
            self.store.freeze("")
        with self.assertRaises(ValueError):
            self.store.freeze("same/name")

    def test_single_binding(self):
        left, right = ids_for(0, 1)[0], ids_for(1, 1)[0]
        initial = {left: {"v": 1}, right: {"v": 1}}
        self.store.ingest("first", [put(id, value) for id, value in initial.items()])
        snapshot = self.store.freeze("fixed")
        iterator = self.store.scan()
        other = self.other()
        other.ingest("before-iteration", [put(left, {"v": 2}), put(right, {"v": 2})])
        triggered = []

        def interleave(stage, details):
            if stage == "scan_bucket" and details["bucket"] == 0 and not triggered:
                self.assertFalse(self.store.catalog.connection.in_transaction)
                triggered.append(True)
                other.ingest("between-buckets", [put(right, {"v": 3})])
        self.store.hook = interleave
        self.assertEqual(list(iterator), sorted(initial.items()))
        self.assertTrue(triggered)
        self.store.hook = None
        self.assert_rows({left: {"v": 2}, right: {"v": 3}})
        self.reopen()
        self.assert_rows(initial, snapshot)
        held = self.store.scan(snapshot)
        self.store.close()
        self.assertEqual(list(held), sorted(initial.items()))

    def test_index_and_scan_agree(self):
        history = History()
        ids = ids_for(0, 14, "站点é") + ids_for(1, 14, "温度")
        for batch_number in range(4):
            changes = []
            operations = []
            for position, id in enumerate(ids):
                payload = {"nested": [{"v": batch_number}], "序号": position}
                if batch_number == 3 and position % 3 == 0:
                    payload = None
                operations.append(delete(id) if payload is None else put(id, payload))
                changes.append((id, payload))
            receipt = self.store.ingest("unicode-" + str(batch_number), operations)
            history.apply(receipt, changes)
        old = self.store.freeze("unicode-history")
        expected = history.frozen()
        self.assert_rows(expected)
        for id in ids:
            self.assertEqual(self.store.get(id), expected.get(id))
        for meta in self.head().segments:
            segment = open_segment(self.store.paths, meta)
            offset = 0
            offsets = []
            for line in segment.data.splitlines(keepends=True):
                offsets.append(offset)
                offset += len(line)
            for id, ordinal, anchor in segment.index.anchors:
                self.assertEqual(anchor, offsets[ordinal])
                self.assertEqual(id, segment.records[ordinal].id)
                self.assertTrue(ordinal == 0 or
                                segment.records[ordinal - 1].id != id)
        self.store.compact(0)
        self.store.compact(1)
        self.assert_rows(expected)
        self.assert_rows(expected, old)
        surviving = next(iter(expected))
        got = self.store.get(surviving)
        got["nested"][0]["v"] = 1000
        scanned = list(self.store.scan())
        scanned[0][1]["nested"].clear()
        self.assert_rows(expected)
        self.assertIsNone(self.store.get("站点é-does-not-exist"))

    def test_revision_precedence(self):
        id = ids_for(0, 1)[0]
        self.store.ingest("old", [put(id, {"v": 1})])
        self.store.ingest("new", [put(id, {"v": 2})])
        self.reverse_physical_order()
        self.assertEqual(self.store.get(id), {"v": 2})
        self.assertEqual(list(self.store.scan()), [(id, {"v": 2})])
        self.store.compact(0)
        self.assertEqual(self.store.get(id), {"v": 2})
        self.store.ingest("delete", [delete(id)])
        self.reverse_physical_order()
        self.assertIsNone(self.store.get(id))
        self.assertEqual(list(self.store.scan()), [])
        self.store.compact(0)
        self.assertIsNone(self.store.get(id))
        self.assertEqual(list(self.store.scan()), [])

    def test_history_reference(self):
        rng = random.Random(271828)
        history = History()
        ids = ids_for(0, 6) + ids_for(1, 6)
        snapshots = []
        for number in range(35):
            chosen = rng.sample(ids, 3)
            changes = [
                (id, None if rng.randrange(4) == 0
                 else {"v": rng.randrange(-5, 6), "list": [number, None]})
                for id in chosen
            ]
            operations = [delete(id) if value is None else put(id, value)
                          for id, value in changes]
            receipt = self.store.ingest("history-" + str(number), operations)
            history.apply(receipt, changes)
            if number % 7 == 0:
                snapshots.append((self.store.freeze("s-" + str(number)),
                                  copy.deepcopy(history.rows)))
            if number % 5 == 0:
                self.store.compact(number % 2)
            self.assert_rows(history.rows)
        self.reopen()
        for snapshot, expected in snapshots:
            self.assert_rows(expected, snapshot)
        self.assertTrue(self.store.verify().readable)

from lake.admission.batch import put, delete
from lake.maintenance.publication import StalePlan
from tests.support import LakeCase, History, ids_for


class TestMaintenance(LakeCase):
    def seed(self):
        history = History()
        a, b = ids_for(0, 2)
        other = ids_for(1, 1)[0]
        batches = [
            [(a, {"v": 1}), (b, {"v": 2}), (other, {"v": 7})],
            [(a, {"v": 3}), (b, None)],
            [(a, {"v": 4}), (b, {"v": 5})],
            [(b, None)],
        ]
        snapshots = []
        for number, changes in enumerate(batches):
            receipt = self.store.ingest(
                "seed-" + str(number),
                [delete(id) if value is None else put(id, value)
                 for id, value in changes],
            )
            history.apply(receipt, changes)
            snapshots.append((self.store.freeze("seed-s-" + str(number)),
                              history.frozen()))
        return history, snapshots, a, other

    def test_compaction_preserves_snapshots(self):
        history, snapshots, a, other = self.seed()
        before = self.head()
        facts = self.facts(before, 0)
        other_objects = tuple(meta for meta in before.segments if meta.bucket == 1)
        original_paths = list(self.store.paths.objects.rglob("*"))
        manifest_id = self.store.compact(0)
        after = self.head()
        self.assertEqual(after.id, manifest_id)
        self.assertNotEqual(after.id, before.id)
        self.assertEqual(after.commit, before.commit)
        self.assertEqual(self.facts(after, 0), facts)
        self.assertEqual(tuple(meta for meta in after.segments if meta.bucket == 1),
                         other_objects)
        self.assertEqual(sum(meta.bucket == 0 for meta in after.segments), 1)
        self.assertTrue(all(path.exists() for path in original_paths))
        self.assert_rows(history.rows)
        for snapshot, expected in snapshots:
            self.assert_rows(expected, snapshot)
        self.assertEqual(self.store.compact(0), manifest_id)
        receipt = self.store.ingest("after-compact", [put(a, {"v": 99})])
        history.apply(receipt, [(a, {"v": 99})])
        self.assert_rows(history.rows)
        self.reopen()
        for snapshot, expected in snapshots:
            self.assert_rows(expected, snapshot)
        self.assertTrue(self.store.verify().readable)

    def test_stale_plan(self):
        history, snapshots, a, other_id = self.seed()
        other = self.other()
        for number, target in enumerate((a, other_id)):
            triggered = []

            def interleave(stage, details):
                if stage == "compaction_inputs_read" and not triggered:
                    self.assertFalse(self.store.catalog.connection.in_transaction)
                    receipt = other.ingest(
                        "during-compact-" + str(number),
                        [put(target, {"v": 80 + number})],
                    )
                    history.apply(receipt, [(target, {"v": 80 + number})])
                    triggered.append(True)
            self.store.hook = interleave
            with self.assertRaises(StalePlan):
                self.store.compact(0)
            self.assertTrue(triggered)
            self.store.hook = None
            self.assert_rows(history.rows)
            self.assertEqual(self.head().commit, history.commit)
        self.store.compact(0)
        self.assert_rows(history.rows)
        for snapshot, expected in snapshots:
            self.assert_rows(expected, snapshot)

    def test_failed_publication_and_invalid_bucket(self):
        history, snapshots, a, other = self.seed()
        before = self.catalog_state()

        def fail(stage, details):
            if stage == "compaction_metadata_after_manifest":
                raise OSError("compaction metadata failed")
        self.store.hook = fail
        with self.assertRaises(OSError):
            self.store.compact(0)
        self.assertEqual(self.catalog_state(), before)
        self.store.hook = None
        for bucket in (-1, 2, True, 0.0, None):
            with self.assertRaises(ValueError):
                self.store.compact(bucket)
        self.assertEqual(self.catalog_state(), before)
        self.store.compact(0)
        self.assert_rows(history.rows)

    def test_preparation_failure(self):
        history, snapshots, a, other = self.seed()
        before = self.catalog_state()

        def fail(stage, details):
            if stage == "before_object_install":
                raise OSError("maintenance install failed")
        self.store.hook = fail
        with self.assertRaises(OSError):
            self.store.compact(0)
        self.assertEqual(self.catalog_state(), before)
        self.store.hook = None
        self.assert_rows(history.rows)
        self.store.compact(0)
        self.assert_rows(history.rows)

from lake.admission.batch import put
from lake.catalog.commits import find_receipt
from lake.objects.integrity import CorruptObject
from lake.objects.paths import object_path
from tests.support import LakeCase, ids_for


class TestFailures(LakeCase):
    def two_bucket_batch(self):
        return [put(ids_for(0, 1)[0], {"v": 1}),
                put(ids_for(1, 1)[0], {"v": 2})]

    def test_segment_install_error(self):
        batch = self.two_bucket_batch()
        snapshot = self.store.freeze("empty")
        before = self.catalog_state()
        seen = []

        def fail(stage, details):
            if stage == "segment_installed":
                seen.append(details["bucket"])
            if stage == "before_object_install" and details["bucket"] == 1:
                raise OSError("second segment installation unavailable")
        self.store.hook = fail
        with self.assertRaisesRegex(OSError, "second segment"):
            self.store.ingest("retry", batch)
        self.assertEqual(seen, [0])
        self.assertEqual(self.catalog_state(), before)
        self.assertIsNone(find_receipt(self.store.catalog, "retry"))
        self.assert_rows({}, snapshot)
        self.reopen()
        receipt = self.store.ingest("retry", batch)
        self.assertEqual(receipt.commit, 1)
        self.assert_rows({operation.id: operation.payload for operation in batch})
        self.assertEqual(self.store.ingest("retry", batch), receipt)
        self.assertEqual(list(self.store.paths.staging.iterdir()), [])

    def test_metadata_rollback(self):
        self.store.ingest("base", [put("baseline", {"v": 0})])
        snapshot = self.store.freeze("before")
        before = self.catalog_state()

        def fail(stage, details):
            if stage == "metadata_after_manifest":
                self.assertTrue(self.store.catalog.connection.in_transaction)
                raise OSError("metadata publication failed")
        self.store.hook = fail
        with self.assertRaisesRegex(OSError, "metadata publication"):
            self.store.ingest("atomic", self.two_bucket_batch())
        self.assertEqual(self.catalog_state(), before)
        other = self.other()
        self.assertEqual(list(other.scan()), [("baseline", {"v": 0})])
        self.reopen()
        self.assertEqual(self.catalog_state(), before)
        receipt = self.store.ingest("atomic", self.two_bucket_batch())
        self.assertEqual(receipt.commit, 2)
        self.assert_rows({"baseline": {"v": 0}}, snapshot)
        self.assertEqual(find_receipt(self.store.catalog, "atomic"), receipt)

    def test_existing_object_integrity(self):
        batch = self.two_bucket_batch()
        installed = []

        def fail(stage, details):
            if stage == "object_installed" and details["kind"] == "data":
                installed.append(object_path(
                    self.store.paths, details["kind"], details["digest"]
                ))
            if stage == "before_object_install" and details["bucket"] == 1:
                raise OSError("stop after first segment")
        self.store.hook = fail
        before = self.catalog_state()
        with self.assertRaises(OSError):
            self.store.ingest("retry", batch)
        self.assertEqual(len(installed), 1)
        original = installed[0].read_bytes()
        installed[0].write_bytes(original + b"broken")
        self.store.hook = None
        with self.assertRaises(CorruptObject):
            self.store.ingest("retry", batch)
        self.assertEqual(self.catalog_state(), before)
        self.assertEqual(installed[0].read_bytes(), original + b"broken")
        installed[0].write_bytes(original)
        receipt = self.store.ingest("retry", batch)
        self.assertEqual(receipt.commit, 1)
        self.assertTrue(self.store.verify().readable)

    def test_io_outside_write_transaction(self):
        other = self.other()
        seen = []

        def probe(stage, details):
            if stage in ("before_object_install", "segment_installed",
                         "admission_prepared"):
                self.assertFalse(self.store.catalog.connection.in_transaction)
                if not seen:
                    seen.append(True)
                    other.ingest("during-io", [put("other", {"v": 5})])
        self.store.hook = probe
        receipt = self.store.ingest("outer", [put("outer", {})])
        self.assertTrue(seen)
        self.assertEqual(receipt.commit, 2)
        self.assert_rows({"other": {"v": 5}, "outer": {}})

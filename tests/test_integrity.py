from dataclasses import replace

from lake.admission.batch import put
from lake.catalog.manifests import create_manifest
from lake.objects.integrity import CorruptObject
from lake.objects.paths import object_path
from lake.objects.staging import stage_bytes
from lake.objects.install import install_object
from lake.segments.codec import canonical_json_bytes
from lake.segments.reader import open_segment
from tests.support import LakeCase, ids_for


class TestIntegrity(LakeCase):
    def assert_unreadable(self, snapshot, address):
        before = self.catalog_state()
        report = self.store.verify(snapshot)
        checks = {entry.address: entry for entry in report.objects}
        self.assertFalse(report.readable)
        self.assertIn(str(address), checks)
        self.assertFalse(checks[str(address)].ok)
        with self.assertRaises(CorruptObject):
            list(self.store.scan(snapshot))
        id = ids_for(0, 1)[0]
        with self.assertRaises(CorruptObject):
            self.store.get(id, snapshot)
        self.assertEqual(self.catalog_state(), before)
        return checks

    def test_missing_and_corrupt(self):
        id = ids_for(0, 1)[0]
        self.store.ingest("data", [put(id, {"v": 1})])
        snapshot = self.store.freeze("audit")
        meta = self.head().segments[0]
        report = self.store.verify(snapshot)
        expected_addresses = {
            str(object_path(self.store.paths, "data", meta.data_digest)),
            str(object_path(self.store.paths, "index", meta.index_digest)),
        }
        self.assertEqual({entry.address for entry in report.objects},
                         expected_addresses)
        for kind, digest in (("data", meta.data_digest),
                             ("index", meta.index_digest)):
            address = object_path(self.store.paths, kind, digest)
            content = address.read_bytes()
            address.unlink()
            checks = self.assert_unreadable(snapshot, address)
            self.assertEqual(checks[str(address)].code, "missing")
            address.write_bytes(content)
            address.write_bytes(content + b"corrupt")
            checks = self.assert_unreadable(snapshot, address)
            self.assertEqual(checks[str(address)].code, "digest_mismatch")
            address.write_bytes(content)
            self.assertTrue(self.store.verify(snapshot).readable)

    def publish_fixture(self, meta):
        with self.store.catalog.write_transaction():
            manifest_id = create_manifest(
                self.store.catalog, self.head().commit, (meta,)
            )
            self.store.catalog.connection.execute(
                "UPDATE current_head SET manifest_id=? WHERE singleton=1",
                (manifest_id,),
            )
        return self.store.freeze("fixture-" + str(manifest_id))

    def install_fixture(self, kind, data):
        return install_object(
            self.store.paths, stage_bytes(self.store.paths, kind, data)
        )

    def test_structure_mismatch(self):
        id = ids_for(0, 1)[0]
        self.store.ingest("data", [put(id, {"v": 1})])
        healthy = self.store.freeze("healthy")
        meta = self.head().segments[0]
        segment = open_segment(self.store.paths, meta)
        bad_index_bytes = canonical_json_bytes({
            "v": 1, "stride": 2, "count": 1,
            "data_size": meta.data_size, "anchors": [[id, 0, 1]],
        })
        bad_index = self.install_fixture("index", bad_index_bytes)
        invalid_index = replace(meta, index_digest=bad_index.digest,
                                index_size=bad_index.size)
        snapshot = self.publish_fixture(invalid_index)
        index_address = object_path(self.store.paths, "index", bad_index.digest)
        checks = self.assert_unreadable(snapshot, index_address)
        self.assertEqual(checks[str(index_address)].code, "index_structure")
        self.assertEqual(sum(not entry.ok for entry in checks.values()), 2)
        invalid_data = self.install_fixture("data", b'{"v":1}\n')
        invalid_meta = replace(meta, data_digest=invalid_data.digest,
                               data_size=invalid_data.size)
        snapshot = self.publish_fixture(invalid_meta)
        data_address = object_path(self.store.paths, "data", invalid_data.digest)
        checks = self.assert_unreadable(snapshot, data_address)
        self.assertEqual(checks[str(data_address)].code, "data_structure")
        self.assert_rows({id: {"v": 1}}, healthy)
        self.assertEqual(segment.records[0].commit, 1)

    def test_verification_is_bound_and_deduplicated(self):
        id = ids_for(0, 1)[0]
        self.store.ingest("data", [put(id, {"v": 1})])
        snapshot = self.store.freeze("one")
        before = self.catalog_state()
        report = self.store.verify(snapshot)
        self.assertTrue(report.readable)
        self.assertEqual(self.catalog_state(), before)
        self.store.ingest("new", [put(id, {"v": 2})])
        self.assertEqual(self.store.verify(snapshot), report)
        exported = report.to_dict()
        exported["objects"][0]["ok"] = False
        self.assertTrue(self.store.verify(snapshot).readable)
        with self.assertRaises(ValueError):
            self.store.verify("unknown")

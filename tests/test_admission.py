from lake.admission.batch import put, delete, Operation
from lake.catalog.commits import find_receipt
from tests.support import LakeCase, History, ids_for


class TestAdmission(LakeCase):
    def test_replace_delete(self):
        history = History()
        first = self.store.ingest("batch-1", [
            put("a", {"v": 1, "removed": True}), put("b", {"v": 2}),
        ])
        history.apply(first, [("a", {"v": 1, "removed": True}),
                              ("b", {"v": 2})])
        second = self.store.ingest("batch-2", [
            put("a", {"v": 3}), delete("b"), delete("missing"),
        ])
        history.apply(second, [("a", {"v": 3}), ("b", None), ("missing", None)])
        self.assertEqual(first.operation_count, 2)
        self.assertEqual(second.operation_count, 3)
        self.assertEqual(second.commit, first.commit + 1)
        self.assertEqual(find_receipt(self.store.catalog, "batch-2"), second)
        self.assert_rows(history.rows)
        self.assertIsNone(self.store.get("b"))

    def test_request_replay(self):
        left = ids_for(0, 1)[0]
        right = ids_for(1, 1)[0]
        operations = [put(left, {"z": [1, {"b": 2, "a": 3}], "a": None}),
                      delete(right)]
        receipt = self.store.ingest(" retry/请求 ", operations)
        self.store.ingest("later", [put("later", {})])
        current = self.catalog_state()
        replay = [delete(right),
                  put(left, {"a": None, "z": [1, {"a": 3, "b": 2}]})]
        self.assertEqual(self.store.ingest(" retry/请求 ", replay), receipt)
        self.assertEqual(self.catalog_state(), current)
        self.reopen()
        self.assertEqual(self.store.ingest(" retry/请求 ", replay), receipt)
        with self.assertRaises(ValueError):
            self.store.ingest(" retry/请求 ", [put(left, {"z": 9})])
        self.assertEqual(self.catalog_state(), current)

    def test_invalid_batch_atomicity(self):
        self.store.ingest("base", [put("a", {"v": 1}), put("b", {"v": 2})])
        snapshot = self.store.freeze("before-invalid")
        before = self.catalog_state()
        cycle = {}
        cycle["self"] = cycle
        bad_batches = [
            [], [put("a", {}), delete("a")],
            [put("a", {}), put("b", {"bad": float("nan")})],
            [put("a", {}), put("b", {"bad": float("inf")})],
            [put("a", {}), put("b", {"bad": {1: "not a JSON key"}})],
            [put("a", {}), put("b", {"bad": (1, 2)})],
            [put("a", {}), put("b", cycle)],
            [put("", {})], [put("b", [])], [object()],
            [Operation("b", "delete", {})], [Operation("b", "unknown")],
            [put("\ud800", {})], [put("b", {"bad": "\ud800"})],
        ]
        for batch in bad_batches:
            with self.subTest(batch_type=str(type(batch))):
                with self.assertRaises(ValueError):
                    self.store.ingest("bad", batch)
                self.assertEqual(self.catalog_state(), before)
        for request_id in ("", None, 3):
            with self.assertRaises(ValueError):
                self.store.ingest(request_id, [put("a", {})])
        self.assert_rows({"a": {"v": 1}, "b": {"v": 2}}, snapshot)
        receipt = self.store.ingest("bad", [put("a", {"v": 4})])
        self.assertEqual(receipt.commit, 2)

    def test_input_copy(self):
        payload = {"nested": [{"v": 1}], "null": None}

        def mutate(stage, details):
            if stage == "batch_normalized":
                payload["nested"][0]["v"] = 99
                payload["added"] = True
        self.store.hook = mutate
        self.store.ingest("copy", [put(" id/原样 ", payload)])
        self.store.hook = None
        self.assertEqual(self.store.get(" id/原样 "),
                         {"nested": [{"v": 1}], "null": None})
        self.assertIsNone(self.store.get("id/原样"))
        self.assertTrue(self.store.verify().readable)

    def test_head_retry_and_concurrent_request_binding(self):
        other = self.other()
        once = []

        def advance(stage, details):
            if stage == "admission_prepared" and not once:
                once.append(True)
                other.ingest("interleaved", [put("other", {"v": 2})])
        self.store.hook = advance
        receipt = self.store.ingest("outer", [put("outer", {"v": 1})])
        self.assertEqual(receipt.commit, 2)
        self.assert_rows({"outer": {"v": 1}, "other": {"v": 2}})
        once.clear()

        def same_request(stage, details):
            if stage == "admission_prepared" and not once:
                once.append(other.ingest("shared", [put("shared", {"v": 3})]))
        self.store.hook = same_request
        shared = self.store.ingest("shared", [put("shared", {"v": 3})])
        self.assertEqual(shared, once[0])
        self.assertEqual(self.head().commit, 3)

    def test_lifecycle_and_empty_table(self):
        self.assertEqual(list(self.store.scan()), [])
        self.assertIsNone(self.store.get("missing"))
        self.assertTrue(self.store.verify().readable)
        self.store.close()
        self.store.close()
        for action in (
            lambda: self.store.get("a"), lambda: self.store.scan(),
            lambda: self.store.ingest("r", [delete("a")]),
            lambda: self.store.freeze("s"), lambda: self.store.verify(),
            lambda: self.store.compact(0),
        ):
            with self.assertRaises(RuntimeError):
                action()

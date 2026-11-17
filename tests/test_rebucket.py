"""Independent business-history reconciliation for the 2-to-3 rebucket.

Expectations here are replayed from successful commit receipts only. The tests
never call the production reader, compactor or migration code to compute an
expected row set.
"""

import copy
import json
import zlib

from lake.admission.batch import put, delete, bucket_for
from lake.catalog.layout import get_layout
from lake.catalog.manifests import get_head, get_manifest
from lake.objects.integrity import CorruptObject
from lake.objects.paths import object_path
from lake.segments.reader import open_segment
from tests.support import LakeCase, ids_for


class Replay:
    """A successful-business-commit ledger, independent of the lake readers."""

    def __init__(self):
        self.commit = 0
        self.rows = {}

    def apply(self, receipt, changes):
        if receipt.commit != self.commit + 1:
            raise AssertionError("business commit is not the next successful one")
        self.commit = receipt.commit
        for id, payload in changes:
            if payload is None:
                self.rows.pop(id, None)
            else:
                self.rows[id] = copy.deepcopy(payload)

    def frozen(self):
        return copy.deepcopy(self.rows)


def operation(changes):
    return [delete(id) if value is None else put(id, value)
            for id, value in changes]


class TestRebucket(LakeCase):
    def emit(self, request, changes, replay):
        receipt = self.store.ingest(request, operation(changes))
        replay.apply(receipt, changes)
        return receipt

    def assert_replay(self, replay, snapshot=None):
        expected = replay.frozen()
        self.assertEqual(list(self.store.scan(snapshot)), sorted(expected.items()))
        for id, payload in expected.items():
            self.assertEqual(self.store.get(id, snapshot), payload)
        for id in set(dict(sorted(expected.items()))) | {"absent-id"}:
            if id not in expected:
                self.assertIsNone(self.store.get(id, snapshot))

    def step_all(self, limit, hook=None, reopen=True):
        while True:
            self.store.hook = hook
            try:
                result = self.store.step_rebucket(limit)
            finally:
                self.store.hook = None
            if result["ready"]:
                return
            if reopen:
                self.reopen()

    def copy_interval_hook(self, intervals, extra=None):
        def hook(stage, details):
            if stage == "rebucket_prepared":
                self.assertFalse(
                    self.store.catalog.connection.in_transaction
                )
                intervals.append((details["start"], details["stop"]))
                if extra is not None:
                    extra(stage, details)
        return hook

    def assert_no_transaction(self, stage, details):
        self.assertFalse(self.store.catalog.connection.in_transaction,
                         "segment IO held a write transaction at " + stage)

    def assert_target_layout(self, manifest=None):
        manifest = manifest or get_head(self.store.catalog)
        self.assertEqual(manifest.bucket_count, 3)
        for meta in manifest.segments:
            self.assertEqual(meta.bucket_count, 3)
            segment = open_segment(self.store.paths, meta, manifest.commit)
            self.assertEqual(segment.meta.bucket, meta.bucket)
            for record in segment.records:
                self.assertEqual(bucket_for(record.id, 3), meta.bucket)
        return manifest

    def test_history_replays_through_migration(self):
        replay = Replay()
        low = ids_for(0, 3)
        high = ids_for(1, 3)
        self.emit("seed-1", [
            (low[0], {"v": 1}), (low[1], {"v": 1}),
            (high[0], {"v": 1}), (high[1], {"v": 1}),
        ], replay)
        self.emit("seed-2", [
            (low[0], {"v": 2}), (high[0], None),
        ], replay)
        old = self.store.freeze("pre")
        old_rows = replay.frozen()

        task = self.store.begin_rebucket("migrate")
        self.assertEqual(self.assert_json(self.store.rebucket_status()), {
            "task_id": task, "phase": "copying", "source_buckets": 2,
            "target_buckets": 3, "processed": 0, "total": 6,
        })
        during = self.store.freeze("during")
        self.assertEqual(list(self.store.scan(during)), sorted(old_rows.items()))

        intervals = []
        other = self.other()
        changed = []

        def hook(stage, details):
            if stage != "rebucket_prepared":
                return
            self.assertFalse(self.store.catalog.connection.in_transaction)
            intervals.append((details["start"], details["stop"]))
            if not changed:
                changed.append(True)
                receipt = other.ingest("during-1", operation([
                    (low[1], None), (low[2], {"v": 9}),
                ]))
                replay.apply(receipt, [(low[1], None), (low[2], {"v": 9})])
            self.assert_replay(replay)

        self.step_all(2, hook)
        self.assertEqual(intervals, [(0, 2), (2, 4), (4, 6)])
        status = self.store.rebucket_status()
        self.assertEqual(status["phase"], "ready")
        self.assertEqual(status["processed"], status["total"])

        # New admission after ready but before cutover, then cutover.
        receipt = self.emit("after-ready", [(high[1], {"v": 3})], replay)
        self.assertTrue(self.store.finish_rebucket())
        self.assertTrue(self.store.finish_rebucket())
        self.assert_replay(replay)
        self.assertEqual(receipt.commit, 4)

        self.assert_target_layout()
        self.assertEqual(self.store.rebucket_status()["phase"], "finished")
        self.assertEqual(list(self.store.scan(old)), sorted(old_rows.items()))
        self.assertEqual(list(self.store.scan(during)), sorted(old_rows.items()))

        self.reopen()
        self.assert_replay(replay)
        self.assertTrue(self.store.verify().readable)
        self.assertTrue(self.store.verify(old).readable)
        nxt = self.emit("post", [("later", {"v": 5})], replay)
        self.assertEqual(nxt.commit, 5)
        self.assert_target_layout()
        self.assert_replay(replay)

    def assert_json(self, value):
        return json.loads(json.dumps(value, ensure_ascii=False))

    def test_correction_and_delete_after_source_read(self):
        replay = Replay()
        left, victim = ids_for(0, 2)
        other_id = ids_for(1, 1)[0]
        self.emit("seed", [(left, {"v": 1}), (victim, {"v": 1}),
                           (other_id, {"v": 1})], replay)
        old = self.store.freeze("old")
        old_rows = replay.frozen()
        self.store.begin_rebucket("task")
        other = self.other()
        acted = []

        def hook(stage, details):
            if stage != "rebucket_prepared":
                return
            if not acted:
                acted.append(True)
                # left@1 was just copied; the delete must win.
                receipt = other.ingest("kill", [delete(left)])
                replay.apply(receipt, [(left, None)])
            self.assert_replay(replay)

        self.step_all(1, hook)
        self.store.finish_rebucket()
        self.assertIsNone(self.store.get(left))
        self.assert_replay(replay)
        self.assertEqual(list(self.store.scan(old)), sorted(old_rows.items()))
        self.assertTrue(self.store.verify().readable)
        self.assertTrue(self.store.verify(old).readable)

    def test_fresh_lake_correction_after_copy(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            id = ids_for(0, 1)[0]
            replay = Replay()
            with Store(root, segment_rows=1, index_stride=1) as store:
                receipt = store.ingest("seed", [put(id, {"v": 1})])
                replay.apply(receipt, [(id, {"v": 1})])
                store.begin_rebucket("task")
                other = Store(root, segment_rows=1, index_stride=1)
                fired = []

                def hook(stage, details):
                    if stage == "rebucket_prepared" and not fired:
                        fired.append(True)
                        receipt = other.ingest("fix", [put(id, {"v": 3})])
                        replay.apply(receipt, [(id, {"v": 3})])

                store.hook = hook
                step = store.step_rebucket(1)
                store.hook = None
                self.assertTrue(step["ready"])
                self.assertTrue(store.finish_rebucket())
                self.assertEqual(list(store.scan()),
                                 sorted(replay.frozen().items()))
                self.assertEqual(store.get(id), {"v": 3})
                other.close()

    def test_admission_prepared_across_begin_and_finish(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            seed_id = ids_for(0, 1)[0]
            outer_id = ids_for(1, 1)[0]
            replay = Replay()
            store = Store(root, segment_rows=2, index_stride=2)
            other = Store(root, segment_rows=2, index_stride=2)
            receipt = store.ingest("seed", [put(seed_id, {"v": 1})])
            replay.apply(receipt, [(seed_id, {"v": 1})])
            task_holder = []

            def hook(stage, details):
                if stage == "admission_prepared" and not task_holder:
                    task_holder.append(other.begin_rebucket("task"))
                    receipt = other.ingest("inner", [put("inner", {"v": 2})])
                    replay.apply(receipt, [("inner", {"v": 2})])

            store.hook = hook
            receipt = store.ingest("outer", [put(outer_id, {"v": 3})])
            store.hook = None
            replay.apply(receipt, [(outer_id, {"v": 3})])
            self.assertEqual(receipt.commit, 3)
            # The outer admission prepared under two buckets must republish
            # under the target layout rather than into the retired layout.
            while not store.step_rebucket(4)["ready"]:
                pass
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(list(store.scan()), sorted(replay.frozen().items()))
            head = get_head(store.catalog)
            self.assertTrue(all(meta.bucket_count == 3
                                for meta in head.segments))
            # Only the finite pre-begin source was copied: two records total.
            self.assertEqual(store.rebucket_status()["total"], 1)
            store.close()
            other.close()

    def test_step_failure_retries_interval_without_new_commits(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            ids = ids_for(0, 2) + ids_for(1, 2)
            store = Store(root, segment_rows=1, index_stride=1)
            other = Store(root, segment_rows=1, index_stride=1)
            receipt = store.ingest("seed", operation(
                [(id, {"v": number}) for number, id in enumerate(ids)]
            ))
            replay.apply(receipt, [(id, {"v": number})
                                   for number, id in enumerate(ids)])
            store.begin_rebucket("task")
            intervals = []
            fail = [True]

            def hook(stage, details):
                if stage != "rebucket_prepared":
                    return
                intervals.append((details["start"], details["stop"]))
                if fail[0]:
                    fail[0] = False
                    receipt = other.ingest("during", [put(ids[0], {"v": 7})])
                    replay.apply(receipt, [(ids[0], {"v": 7})])
                    raise OSError("injected after preparation")

            store.hook = hook
            with self.assertRaisesRegex(OSError, "injected"):
                store.step_rebucket(2)
            store.hook = None
            self.assertEqual(store.rebucket_status()["processed"], 0)
            self.assertEqual(store.rebucket_status()["total"], 4)
            self.assertEqual(get_head(store.catalog).commit, 2)
            self.assert_replay_in(store, replay)

            store.hook = hook
            store.step_rebucket(2)
            store.step_rebucket(2)
            store.hook = None
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(intervals, [(0, 2), (0, 2), (2, 4)])
            self.assertEqual(get_head(store.catalog).commit, 2)
            self.assertEqual(list(store.scan()), sorted(replay.frozen().items()))
            receipt = store.ingest("after", [put("tail", {"v": 1})])
            self.assertEqual(receipt.commit, 3)
            store.close()
            other.close()

    def assert_replay_in(self, store, replay):
        self.assertEqual(list(store.scan()), sorted(replay.frozen().items()))

    def test_bound_reads_do_not_drift(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            ids = ids_for(0, 2) + ids_for(1, 2)
            store = Store(root, segment_rows=1, index_stride=1)
            receipt = store.ingest("seed", operation(
                [(id, {"v": 1}) for id in ids]))
            replay.apply(receipt, [(id, {"v": 1}) for id in ids])
            old = store.freeze("old")
            old_rows = replay.frozen()
            store.begin_rebucket("task")
            during = store.freeze("during")
            held = store.scan()
            receipt = store.ingest("change", operation(
                [(ids[0], {"v": 2}), (ids[1], None)]))
            replay.apply(receipt, [(ids[0], {"v": 2}), (ids[1], None)])
            while not store.step_rebucket(1)["ready"]:
                pass
            self.assertTrue(store.finish_rebucket())
            store.close()
            self.assertEqual(list(held), sorted(old_rows.items()))
            with Store(root) as reopened:
                self.assertEqual(list(reopened.scan(old)), sorted(old_rows.items()))
                self.assertEqual(list(reopened.scan(during)),
                                 sorted(old_rows.items()))
                self.assertEqual(list(reopened.scan()),
                                 sorted(replay.frozen().items()))

    def test_corrupt_source_does_not_advance(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            id = ids_for(0, 1)[0]
            store = Store(root, segment_rows=1, index_stride=1)
            store.ingest("seed", [put(id, {"v": 1})])
            store.begin_rebucket("task")
            meta = get_head(store.catalog).segments[0]
            path = object_path(store.paths, "data", meta.data_digest)
            original = path.read_bytes()
            path.write_bytes(original + b"corrupt")
            with self.assertRaises(CorruptObject):
                store.step_rebucket(1)
            self.assertEqual(store.rebucket_status()["processed"], 0)
            path.write_bytes(original)
            self.assertTrue(store.step_rebucket(1)["ready"])
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(list(store.scan()), [(id, {"v": 1})])
            store.close()

    def test_empty_lake_and_invalid_arguments(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            store = Store(root)
            self.assertEqual(store.rebucket_status(), {
                "task_id": None, "phase": "idle", "source_buckets": 2,
                "target_buckets": 3, "processed": 0, "total": 0,
            })
            for bad in ("", None, 3, "\ud800"):
                with self.assertRaises(ValueError):
                    store.begin_rebucket(bad)
            with self.assertRaises(ValueError):
                store.step_rebucket(1)
            with self.assertRaises(ValueError):
                store.finish_rebucket()
            task = store.begin_rebucket("empty")
            self.assertEqual(store.rebucket_status()["phase"], "ready")
            self.assertEqual(store.step_rebucket(1),
                             {"processed": 0, "ready": True})
            for bad in (0, -1, True, 1.0, "1", None):
                with self.assertRaises(ValueError):
                    store.step_rebucket(bad)
            self.assertTrue(store.finish_rebucket())
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(store.begin_rebucket("empty"), task)
            with self.assertRaises(ValueError):
                store.begin_rebucket("other")
            status = store.rebucket_status()
            self.assertEqual(status["phase"], "finished")
            status["processed"] = 99
            self.assertEqual(store.rebucket_status()["processed"], 0)
            receipt = store.ingest("after", [put("a", {"v": 1})])
            self.assertEqual(receipt.commit, 1)
            self.assertEqual(list(store.scan()), [("a", {"v": 1})])
            store.close()
            for action in (lambda: store.begin_rebucket("empty"),
                           lambda: store.step_rebucket(1),
                           lambda: store.finish_rebucket(),
                           lambda: store.rebucket_status()):
                with self.assertRaises(RuntimeError):
                    action()

    def test_old_snapshot_keeps_old_layout_objects(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            id = ids_for(0, 1)[0]
            with Store(root, segment_rows=1, index_stride=1) as store:
                store.ingest("seed", [put(id, {"v": 1})])
                old = store.freeze("old")
                old_manifest_id = store.catalog.connection.execute(
                    "SELECT manifest_id FROM snapshots WHERE id=?", (old,)
                ).fetchone()[0]
                store.begin_rebucket("task")
                while not store.step_rebucket(1)["ready"]:
                    pass
                self.assertTrue(store.finish_rebucket())
                head = get_head(store.catalog)
                self.assertTrue(all(meta.bucket_count == 3
                                    for meta in head.segments))
                old_manifest = get_manifest(store.catalog, old_manifest_id)
                self.assertTrue(all(meta.bucket_count == 2
                                    for meta in old_manifest.segments))
                for meta in old_manifest.segments:
                    for kind, digest in (
                        ("data", meta.data_digest), ("index", meta.index_digest)
                    ):
                        self.assertTrue(
                            object_path(store.paths, kind, digest).exists()
                        )
                self.assertEqual(list(store.scan(old)), [(id, {"v": 1})])
                self.assertTrue(store.verify(old).readable)
                # The current head must not retain the retired layout.
                self.assertFalse(any(
                    meta.bucket_count == 2 for meta in head.segments))

    def test_three_bucket_routing_and_current_only_new_layout(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            ids = [id for bucket in range(3)
                   for id in ids_for(0, 1, "三分-" + str(bucket))]
            # Pick ids that actually span all three target buckets.
            chosen = []
            for target in range(3):
                number = 0
                while True:
                    candidate = "lane-" + str(number)
                    if bucket_for(candidate, 3) == target:
                        chosen.append(candidate)
                        break
                    number += 1
            with Store(root, segment_rows=1, index_stride=1) as store:
                receipt = store.ingest("seed", operation(
                    [(id, {"v": 1}) for id in chosen]))
                replay.apply(receipt, [(id, {"v": 1}) for id in chosen])
                store.begin_rebucket("task")
                while not store.step_rebucket(1)["ready"]:
                    pass
                self.assertTrue(store.finish_rebucket())
                head = get_head(store.catalog)
                self.assertEqual(head.bucket_count, 3)
                occupied = {meta.bucket for meta in head.segments}
                self.assertEqual(occupied, {0, 1, 2})
                for meta in head.segments:
                    segment = open_segment(store.paths, meta, head.commit)
                    for record in segment.records:
                        self.assertEqual(bucket_for(record.id, 3), meta.bucket)
                receipt = store.ingest("post", [
                    put("lane-new-a", {"v": 2}), put("lane-new-b", {"v": 2}),
                ])
                replay.apply(receipt, [("lane-new-a", {"v": 2}),
                                       ("lane-new-b", {"v": 2})])
                head = get_head(store.catalog)
                for meta in head.segments:
                    if meta.bucket_count == 3:
                        for record in open_segment(
                            store.paths, meta, head.commit
                        ).records:
                            self.assertEqual(bucket_for(record.id, 3),
                                             meta.bucket)
                self.assertEqual(list(store.scan()),
                                 sorted(replay.frozen().items()))

    def test_layout_survives_reopen_mid_copy(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            ids = ids_for(0, 2) + ids_for(1, 2)
            store = Store(root, segment_rows=1, index_stride=1)
            receipt = store.ingest("seed", operation(
                [(id, {"v": n}) for n, id in enumerate(ids)]))
            replay.apply(receipt, [(id, {"v": n})
                                   for n, id in enumerate(ids)])
            task = store.begin_rebucket("task")
            store.step_rebucket(1)
            store.close()
            with Store(root, segment_rows=1, index_stride=1) as reopened:
                self.assertEqual(reopened.begin_rebucket("task"), task)
                self.assertEqual(reopened.rebucket_status()["processed"], 1)
                self.assertEqual(reopened.rebucket_status()["total"], 4)
                step = reopened.step_rebucket(1)
                self.assertEqual(step, {"processed": 1, "ready": False})
                self.assertEqual(list(reopened.scan()),
                                 sorted(replay.frozen().items()))
                self.assertEqual(reopened.step_rebucket(10),
                                 {"processed": 2, "ready": True})
                self.assertTrue(reopened.finish_rebucket())
                self.assertEqual(list(reopened.scan()),
                                 sorted(replay.frozen().items()))
                self.assertEqual(reopened.rebucket_status()["phase"],
                                 "finished")

    def test_admission_prepared_before_finish_republishes(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            seed_id = ids_for(0, 1)[0]
            store = Store(root, segment_rows=1, index_stride=1)
            other = Store(root, segment_rows=1, index_stride=1)
            receipt = store.ingest("seed", [put(seed_id, {"v": 1})])
            replay.apply(receipt, [(seed_id, {"v": 1})])
            store.begin_rebucket("task")
            while not store.step_rebucket(1)["ready"]:
                pass
            fired = []

            def hook(stage, details):
                if stage == "admission_prepared" and not fired:
                    fired.append(True)
                    # Cutover happens while this admission is prepared but
                    # not yet published. Its segments must be republished into
                    # the surviving head, not lost with the retired layout.
                    self.assertTrue(other.finish_rebucket())

            store.hook = hook
            receipt = store.ingest("crossing", [put("crossing", {"v": 2})])
            store.hook = None
            replay.apply(receipt, [("crossing", {"v": 2})])
            self.assertEqual(receipt.commit, 2)
            self.assertEqual(list(store.scan()), sorted(replay.frozen().items()))
            head = get_head(store.catalog)
            self.assertTrue(all(meta.bucket_count == 3
                                for meta in head.segments))
            self.assertEqual(store.rebucket_status()["phase"], "finished")
            store.close()
            other.close()

    def test_total_counts_revisions_tombstones_and_physical_rows(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            id = ids_for(0, 1)[0]
            store = Store(root, segment_rows=2, index_stride=2)
            receipt = store.ingest("one", [put(id, {"v": 1})])
            replay.apply(receipt, [(id, {"v": 1})])
            receipt = store.ingest("two", [put(id, {"v": 2})])
            replay.apply(receipt, [(id, {"v": 2})])
            receipt = store.ingest("three", [delete(id)])
            replay.apply(receipt, [(id, None)])
            store.begin_rebucket("task")
            self.assertEqual(store.rebucket_status()["total"], 3)
            self.assertEqual(list(store.scan()), list(replay.frozen().items()))
            while not store.step_rebucket(1)["ready"]:
                pass
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(list(store.scan()), [])
            head = get_head(store.catalog)
            copies = [
                record for meta in head.segments if meta.bucket_count == 3
                for record in open_segment(
                    store.paths, meta, head.commit).records
            ]
            self.assertEqual(len(copies), 3)
            self.assertEqual({(record.id, record.commit) for record in copies},
                             {(id, 1), (id, 2), (id, 3)})
            self.assertEqual(store.rebucket_status()["processed"], 3)
            store.close()

    def test_migration_adds_no_business_commits(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            ids = ids_for(0, 2) + ids_for(1, 1)
            store = Store(root, segment_rows=1, index_stride=1)
            for number, id in enumerate(ids):
                store.ingest("seed-" + str(number), [put(id, {"v": number})])
            commits_before = tuple(store.catalog.connection.execute(
                "SELECT * FROM commits ORDER BY commit_id"))
            receipts_before = tuple(store.catalog.connection.execute(
                "SELECT * FROM receipts ORDER BY request_id"))
            head_commit = get_head(store.catalog).commit
            task = store.begin_rebucket("task")
            while not store.step_rebucket(1)["ready"]:
                pass
            store.finish_rebucket()
            self.assertEqual(tuple(store.catalog.connection.execute(
                "SELECT * FROM commits ORDER BY commit_id")), commits_before)
            self.assertEqual(tuple(store.catalog.connection.execute(
                "SELECT * FROM receipts ORDER BY request_id")), receipts_before)
            self.assertEqual(get_head(store.catalog).commit, head_commit)
            self.assertEqual(store.begin_rebucket("task"), task)
            store.close()

    def test_direct_open_of_v1_directory(self):
        import json
        import sqlite3
        import tempfile

        from lake.admission.api import Store
        from lake.segments.codec import (
            SegmentRecord, canonical_json_bytes,
        )
        from lake.segments.writer import write_segment
        from lake.objects.paths import (
            ensure_storage_root, resolve_storage_paths,
        )
        with tempfile.TemporaryDirectory() as root:
            paths = resolve_storage_paths(root)
            ensure_storage_root(paths)
            legacy_id = ids_for(0, 1)[0]
            segment = write_segment(
                paths, 0, [SegmentRecord(
                    legacy_id, 1, "put",
                    canonical_json_bytes({"v": 1}).decode("utf-8"),
                )], lambda stage, details: None, 2,
            )
            descriptor = dict(vars(segment))
            descriptor.pop("bucket_count")
            descriptor["v"] = 1
            connection = sqlite3.connect(str(paths.database))
            connection.executescript("""
                CREATE TABLE catalog_info (
                    key TEXT PRIMARY KEY, value INTEGER NOT NULL);
                CREATE TABLE manifests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    business_commit INTEGER NOT NULL);
                CREATE TABLE manifest_segments (
                    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
                    position INTEGER NOT NULL, descriptor TEXT NOT NULL,
                    PRIMARY KEY(manifest_id, position));
                CREATE TABLE current_head (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    manifest_id INTEGER NOT NULL REFERENCES manifests(id));
                CREATE TABLE commits (
                    commit_id INTEGER PRIMARY KEY CHECK(commit_id > 0),
                    manifest_id INTEGER NOT NULL UNIQUE REFERENCES manifests(id),
                    request_id TEXT NOT NULL UNIQUE);
                CREATE TABLE receipts (
                    request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    commit_id INTEGER NOT NULL UNIQUE REFERENCES commits(commit_id),
                    manifest_id INTEGER NOT NULL REFERENCES manifests(id),
                    operation_count INTEGER NOT NULL CHECK(operation_count > 0));
                CREATE TABLE snapshots (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                    manifest_id INTEGER NOT NULL REFERENCES manifests(id));
            """)
            connection.execute("INSERT INTO catalog_info VALUES ('version', 1)")
            connection.execute(
                "INSERT INTO manifests(business_commit) VALUES (0)")
            connection.execute("INSERT INTO current_head VALUES (1, 1)")
            connection.execute(
                "INSERT INTO manifests(business_commit) VALUES (1)")
            connection.execute(
                "INSERT INTO manifest_segments VALUES (2, 0, ?)",
                (canonical_json_bytes(descriptor).decode("utf-8"),))
            connection.execute(
                "UPDATE current_head SET manifest_id=2 WHERE singleton=1")
            connection.execute("INSERT INTO commits VALUES (1, 2, 'legacy')")
            connection.execute(
                "INSERT INTO receipts VALUES ('legacy', 'fp', 1, 2, 1)")
            connection.commit()
            connection.close()

            with Store(root) as store:
                self.assertEqual(list(store.scan()), [(legacy_id, {"v": 1})])
                self.assertTrue(store.verify().readable)
                self.assertEqual(store.rebucket_status(), {
                    "task_id": None, "phase": "idle", "source_buckets": 2,
                    "target_buckets": 3, "processed": 0, "total": 0,
                })
                receipt = store.ingest("resume", [put(legacy_id, {"v": 2})])
                self.assertEqual(receipt.commit, 2)
                store.begin_rebucket("task")
                while not store.step_rebucket(1)["ready"]:
                    pass
                self.assertTrue(store.finish_rebucket())
                self.assertEqual(list(store.scan()), [(legacy_id, {"v": 2})])
                self.assertEqual(get_head(store.catalog).bucket_count, 3)

    def test_request_namespace_is_separate_from_ingest(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            with Store(root, segment_rows=1, index_stride=1) as store:
                store.ingest("shared-name", [put("a", {"v": 1})])
                task = store.begin_rebucket("shared-name")
                while not store.step_rebucket(1)["ready"]:
                    pass
                self.assertTrue(store.finish_rebucket())
                self.assertEqual(store.begin_rebucket("shared-name"), task)
                receipt = store.ingest("shared-name", [put("a", {"v": 1})])
                self.assertEqual(receipt.commit, 1)
                self.assertEqual(store.get("a"), {"v": 1})
                with self.assertRaises(ValueError):
                    store.begin_rebucket("other-name")

    def test_batch_publish_crossing_begin_uses_target_routing(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            replay = Replay()
            seed_id = ids_for(0, 1)[0]
            store = Store(root, segment_rows=2, index_stride=2)
            other = Store(root, segment_rows=2, index_stride=2)
            receipt = store.ingest("seed", [put(seed_id, {"v": 1})])
            replay.apply(receipt, [(seed_id, {"v": 1})])
            fired = []

            def hook(stage, details):
                if stage == "before_batch_publish" and not fired:
                    fired.append(True)
                    other.begin_rebucket("task")

            store.hook = hook
            receipt = store.ingest("crossing", [put("crossing", {"v": 2})])
            store.hook = None
            replay.apply(receipt, [("crossing", {"v": 2})])
            self.assertEqual(receipt.commit, 2)
            # Its objects were first prepared under the old layout; the head
            # change forces a re-prepare under the target layout.
            head = get_head(store.catalog)
            self.assertEqual(head.bucket_count, 3)
            holders = [
                meta for meta in head.segments
                if any(record.id == "crossing" for record in open_segment(
                    store.paths, meta, head.commit).records)
            ]
            self.assertEqual(len(holders), 1)
            self.assertEqual(holders[0].bucket_count, 3)
            self.assertEqual(holders[0].bucket, bucket_for("crossing", 3))
            while not store.step_rebucket(4)["ready"]:
                pass
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(store.get("crossing"), {"v": 2})
            self.assertEqual(list(store.scan()),
                             sorted(replay.frozen().items()))
            store.close()
            other.close()

    def test_finish_refuses_unreadable_target_and_holds_no_write_txn(self):
        import tempfile
        from lake.admission.api import Store
        from lake.objects.integrity import CorruptObject
        with tempfile.TemporaryDirectory() as root:
            id = ids_for(0, 1)[0]
            store = Store(root, segment_rows=1, index_stride=1)
            other = Store(root, segment_rows=1, index_stride=1)
            store.ingest("seed", [put(id, {"v": 1})])
            store.begin_rebucket("task")
            while not store.step_rebucket(1)["ready"]:
                pass
            meta = next(meta for meta in get_head(store.catalog).segments
                        if meta.bucket_count == 3)
            path = object_path(store.paths, "data", meta.data_digest)
            original = path.read_bytes()
            path.write_bytes(original + b"broken")

            with self.assertRaises(CorruptObject):
                store.finish_rebucket()
            self.assertEqual(store.rebucket_status()["phase"], "ready")
            path.write_bytes(original)
            self.assertTrue(store.finish_rebucket())
            self.assertEqual(list(store.scan()), [(id, {"v": 1})])
            store.close()
            other.close()

    def test_segment_io_and_checks_hold_no_write_transaction(self):
        import tempfile
        from lake.admission.api import Store
        with tempfile.TemporaryDirectory() as root:
            ids = ids_for(0, 2) + ids_for(1, 1)
            store = Store(root, segment_rows=1, index_stride=1)
            other = Store(root, segment_rows=1, index_stride=1)
            store.ingest("seed", [put(id, {"v": 1}) for id in ids])
            stages = []
            injected = []

            def hook(stage, details):
                if stage in (
                    "before_object_install", "object_installed",
                    "segment_installed", "rebucket_prepared",
                ):
                    self.assertFalse(
                        store.catalog.connection.in_transaction,
                        "write transaction held at " + stage,
                    )
                    stages.append(stage)
                    if stage == "segment_installed" and not injected:
                        # Another connection must be able to commit here.
                        injected.append("concurrent-row")
                        other.ingest("concurrent-request",
                                     [put("concurrent-row", {"v": 2})])

            store.hook = hook
            store.begin_rebucket("task")
            while not store.step_rebucket(1)["ready"]:
                pass
            self.assertTrue(store.finish_rebucket())
            store.hook = None
            self.assertIn("rebucket_prepared", stages)
            self.assertIn("before_object_install", stages)
            self.assertEqual(
                store.rebucket_status()["phase"], "finished")
            expected = sorted([(id, {"v": 1}) for id in ids]
                              + [("concurrent-row", {"v": 2})])
            self.assertEqual(list(store.scan()), expected)
            self.assertEqual(store.verify().readable, True)
            store.close()
            other.close()

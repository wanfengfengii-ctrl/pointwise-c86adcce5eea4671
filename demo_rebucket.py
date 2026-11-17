"""End-to-end rebucket demonstration: corrections, deletes, staged reopen.

Shows the one-shot two-to-three bucket migration on a fresh directory:
a fixed snapshot taken before the task, a correction and a delete admitted
while the source is being copied, incremental steps that each reopen the
store, the final cutover, and reads confined to the target layout afterwards.
Prints a JSON report and exits non-zero if any invariant is violated.
"""

import json
import tempfile

from lake.admission.api import Store
from lake.admission.batch import put, delete, bucket_for
from lake.catalog.manifests import get_head
from lake.segments.reader import open_segment


def rows(store, snapshot=None):
    return [list(row) for row in store.scan(snapshot)]


def assert_target_layout(store):
    head = get_head(store.catalog)
    assert head.bucket_count == 3, head.bucket_count
    for meta in head.segments:
        assert meta.bucket_count == 3, meta.bucket_count
        segment = open_segment(store.paths, meta, head.commit)
        for record in segment.records:
            assert bucket_for(record.id, 3) == meta.bucket
    return head


def main():
    with tempfile.TemporaryDirectory(prefix="rebucket-demo-") as root:
        store = Store(root, segment_rows=1, index_stride=1)
        store.ingest("demo-001", [
            put("a", {"v": 1}), put("b", {"v": 2}),
            put("sensor-0", {"v": 10}), put("sensor-4", {"v": 20}),
        ])
        old = store.freeze("demo-before")
        old_rows = rows(store, old)

        task = store.begin_rebucket("demo-resize")
        assert store.begin_rebucket("demo-resize") == task
        status = store.rebucket_status()
        total = status["total"]
        assert status["phase"] == "copying" and status["processed"] == 0
        assert store.finish_rebucket() is False

        # Corrections and deletes arrive while the copy is in progress.
        store.ingest("demo-002", [put("a", {"v": 3}), delete("b")])
        corrected_rows = rows(store)
        during = store.freeze("demo-during")
        assert rows(store, during) == corrected_rows
        intervals = []
        processed = 0
        done = False
        while not done:
            store.hook = lambda stage, details: (
                intervals.append([details["start"], details["stop"]])
                if stage == "rebucket_prepared" else None
            )
            step = store.step_rebucket(2)
            store.hook = None
            assert 0 <= step["processed"] <= 2
            assert step["processed"] > 0
            processed += step["processed"]
            status = store.rebucket_status()
            assert status["processed"] == processed
            assert status["total"] == total
            # Corrections already made during the copy stay visible.
            assert rows(store) == corrected_rows
            done = step["ready"]
            # Reopen between steps: progress and identity must survive.
            store.close()
            store = Store(root, segment_rows=1, index_stride=1)
            assert store.begin_rebucket("demo-resize") == task
            assert store.rebucket_status()["processed"] == processed
        assert processed == total, (processed, total)
        assert intervals == [[0, 2], [2, 4]], intervals

        assert store.finish_rebucket() is True
        assert store.finish_rebucket() is True
        assert store.step_rebucket(1) == {"processed": 0, "ready": True}
        current_rows = rows(store)
        assert current_rows == [["a", {"v": 3}], ["sensor-0", {"v": 10}],
                                ["sensor-4", {"v": 20}]], current_rows
        assert rows(store, old) == old_rows
        assert rows(store, during) == corrected_rows
        head = assert_target_layout(store)
        report = store.verify()
        assert report.readable
        tail = store.ingest("demo-003", [put("later", {"v": 5})])
        assert tail.commit == 3
        status = store.rebucket_status()
        store.close()

        with Store(root) as reopened:
            final_rows = rows(reopened)
            assert final_rows == [["a", {"v": 3}], ["later", {"v": 5}],
                                  ["sensor-0", {"v": 10}],
                                  ["sensor-4", {"v": 20}]], final_rows
            assert rows(reopened, old) == old_rows
            assert reopened.verify().readable
            assert reopened.verify(old).readable
            print(json.dumps({
                "task_id": task,
                "status": status,
                "source_intervals": intervals,
                "head_bucket_count": head.bucket_count,
                "current": final_rows,
                "before_snapshot": old_rows,
                "next_business_commit": tail.commit,
                "readable": True,
            }, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

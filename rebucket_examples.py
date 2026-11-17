"""Public non-answer fixtures and the rebucketing business example."""
import argparse
import copy
import json
import tempfile
import zlib

from lake.admission.api import Store
from lake.admission.batch import put, delete


INITIAL = [
    ("first-a", [("a", {"v": 1})]),
    ("first-b", [("b", {"v": 2})]),
]
DURING = [
    ("correct-a", [("a", {"v": 3})]),
    ("delete-b", [("b", None)]),
]


def replay(batches):
    """Independent expected state: no production reader or migration calls."""
    rows = {}
    for _, changes in batches:
        for key, value in changes:
            if value is None:
                rows.pop(key, None)
            else:
                rows[key] = copy.deepcopy(value)
    return sorted(rows.items())


def check_fixtures():
    assert replay(INITIAL) == [("a", {"v": 1}), ("b", {"v": 2})]
    assert replay(INITIAL + DURING) == [("a", {"v": 3})]
    assert [(key, zlib.crc32(key.encode("utf-8")) % 3)
            for key in ("a", "b")] == [("a", 0), ("b", 2)]
    print("REBUCKET_FIXTURES_OK")


def ingest_batches(store, batches, next_commit):
    for request, changes in batches:
        receipt = store.ingest(request, [
            delete(key) if value is None else put(key, value)
            for key, value in changes
        ])
        assert receipt.commit == next_commit, receipt
        next_commit += 1
    return next_commit


def assert_rows(store, expected, snapshot=None):
    assert list(store.scan(snapshot)) == expected
    by_key = dict(expected)
    for key in ("a", "b", "later", "absent"):
        assert store.get(key, snapshot) == by_key.get(key)


def demonstrate():
    check_fixtures()
    before = replay(INITIAL)
    after = replay(INITIAL + DURING)
    intervals = []

    def observe(stage, details):
        if stage == "rebucket_prepared":
            intervals.append((details["start"], details["stop"]))

    with tempfile.TemporaryDirectory(prefix="rebucket-example-") as root:
        with Store(root, segment_rows=1, index_stride=1) as store:
            next_commit = ingest_batches(store, INITIAL, 1)
            old = store.freeze("before-rebucket")
            task = store.begin_rebucket("resize-once")
            assert isinstance(task, str) and task
            assert store.begin_rebucket("resize-once") == task
            status = store.rebucket_status()
            assert status["total"] == 2
            assert status["processed"] == 0
            assert store.finish_rebucket() is False
            next_commit = ingest_batches(store, DURING, next_commit)
            held = store.scan()
            during = store.freeze("during-rebucket")
            assert_rows(store, after)
            assert_rows(store, before, old)
        assert list(held) == after

        remaining = 2
        processed = 0
        while remaining:
            with Store(root, hook=observe,
                       segment_rows=1, index_stride=1) as store:
                assert store.begin_rebucket("resize-once") == task
                assert store.rebucket_status()["processed"] == processed
                step = store.step_rebucket(1)
                assert step["processed"] == 1
                processed += 1
                remaining -= 1
                status = store.rebucket_status()
                assert status["processed"] == processed
                assert status["total"] == 2
                assert step["ready"] == (remaining == 0)
                assert_rows(store, after)
                assert_rows(store, before, old)
                assert_rows(store, after, during)

        assert intervals == [(0, 1), (1, 2)]
        with Store(root) as store:
            assert store.finish_rebucket() is True
            assert store.finish_rebucket() is True
            assert store.step_rebucket(1) == {"processed": 0, "ready": True}
            assert store.rebucket_status()["phase"] == "finished"
            assert store.begin_rebucket("resize-once") == task
            assert_rows(store, after)
            assert_rows(store, before, old)
            assert_rows(store, after, during)
            receipt = store.ingest("after-cutover", [put("later", {"v": 5})])
            assert receipt.commit == next_commit == 5
            assert store.verify().readable
            assert store.verify(old).readable
            result = {
                "task_id": task,
                "status": store.rebucket_status(),
                "source_intervals": intervals,
                "current": list(store.scan()),
                "old_snapshot": list(store.scan(old)),
                "during_snapshot": list(store.scan(during)),
                "next_business_commit": receipt.commit,
            }
        with Store(root) as store:
            assert_rows(store, after + [("later", {"v": 5})])
            assert_rows(store, before, old)
            assert_rows(store, after, during)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check-fixtures", action="store_true")
    mode.add_argument("--demo", action="store_true")
    args = parser.parse_args()
    if args.check_fixtures:
        check_fixtures()
    else:
        demonstrate()


if __name__ == "__main__":
    main()

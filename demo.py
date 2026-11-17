import json
import tempfile

from lake.admission.api import Store
from lake.admission.batch import put, delete


def main():
    with tempfile.TemporaryDirectory(prefix="telemetry-lake-demo-") as root:
        with Store(root, segment_rows=2, index_stride=2) as store:
            first = store.ingest("telemetry-001", [
                put("a", {"v": 1, "site": "north"}),
                put("b", {"v": 2, "site": "south"}),
                put("sensor-0", {"v": 10}),
                put("sensor-4", {"v": 20}),
            ])
            snapshot = store.freeze("daily-close")
            second = store.ingest("telemetry-002", [
                put("a", {"v": 3}), delete("b"),
                put("sensor-0", {"v": 11}), put("sensor-4", {"v": 21}),
            ])
            expected = [
                ("a", {"v": 3}), ("sensor-0", {"v": 11}),
                ("sensor-4", {"v": 21}),
            ]
            assert list(store.scan()) == expected
            store.compact(0)
            store.compact(1)
            assert list(store.scan()) == expected
            assert store.ingest("telemetry-001", [
                put("sensor-4", {"v": 20}), put("sensor-0", {"v": 10}),
                put("b", {"site": "south", "v": 2}),
                put("a", {"site": "north", "v": 1}),
            ]) == first
        with Store(root) as reopened:
            old = list(reopened.scan(snapshot))
            assert old == [
                ("a", {"site": "north", "v": 1}),
                ("b", {"site": "south", "v": 2}),
                ("sensor-0", {"v": 10}), ("sensor-4", {"v": 20}),
            ]
            assert list(reopened.scan()) == expected
            report = reopened.verify()
            assert report.readable
            print(json.dumps({
                "business_commits": [first.commit, second.commit],
                "current_rows": list(reopened.scan()),
                "daily_close_rows": old,
                "verified_objects": len(report.objects),
                "readable": report.readable,
            }, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

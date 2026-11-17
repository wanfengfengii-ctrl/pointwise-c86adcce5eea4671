"""Independent business reconciliation for the 2-to-3 bucket rebucket.

The ledger below is advanced only from successful ``CommitReceipt`` values and
from the operations this script itself submitted. Expected rows are never
derived by calling the lake's ``get``/``scan``/compaction/migration code.

Run directly (non-zero exit on any mismatch) or through ``verify.py``.
"""

import copy
import json
import tempfile
import zlib

from lake.admission.api import Store
from lake.admission.batch import put, delete, bucket_for


def ids_for_source_bucket(bucket, count, prefix="key"):
    found = []
    number = 0
    while len(found) < count:
        candidate = prefix + "-" + str(number)
        if zlib.crc32(candidate.encode("utf-8")) % 2 == bucket:
            found.append(candidate)
        number += 1
    return found


def ids_for_target_bucket(bucket, count, prefix="lane"):
    found = []
    number = 0
    while len(found) < count:
        candidate = prefix + "-" + str(number)
        if zlib.crc32(candidate.encode("utf-8")) % 3 == bucket:
            found.append(candidate)
        number += 1
    return found


class Ledger:
    """Successful business commits only; independent of lake reads."""

    def __init__(self):
        self.commit = 0
        self.rows = {}

    def apply(self, receipt, changes):
        if receipt.commit != self.commit + 1:
            raise AssertionError(
                "business commit %r follows %r" % (receipt.commit, self.commit)
            )
        self.commit = receipt.commit
        for id, payload in changes:
            if payload is None:
                self.rows.pop(id, None)
            else:
                self.rows[id] = copy.deepcopy(payload)

    def expected(self):
        return sorted((id, copy.deepcopy(payload))
                      for id, payload in self.rows.items())


def ingest(store, ledger, request, changes):
    receipt = store.ingest(request, [
        delete(id) if payload is None else put(id, payload)
        for id, payload in changes
    ])
    ledger.apply(receipt, changes)
    return receipt


def check_current(store, ledger, label, snapshot=None):
    actual = list(store.scan(snapshot))
    expected = ledger.expected()
    if actual != expected:
        raise AssertionError("%s scan mismatch: %r != %r"
                             % (label, actual, expected))
    for id, payload in expected:
        if store.get(id, snapshot) != payload:
            raise AssertionError("%s get(%r) mismatch" % (label, id))
    for id in list(ledger.rows) + ["absent-key"]:
        if id not in ledger.rows and store.get(id, snapshot) is not None:
            raise AssertionError("%s resurrected %r" % (label, id))


def check_finished_layout(store):
    from lake.catalog.manifests import get_head
    from lake.segments.reader import open_segment

    head = get_head(store.catalog)
    if head.bucket_count != 3:
        raise AssertionError("head is not on the three-bucket layout")
    for meta in head.segments:
        if meta.bucket_count != 3:
            raise AssertionError("current head retains a retired-layout segment")
        segment = open_segment(store.paths, meta, head.commit)
        for record in segment.records:
            if bucket_for(record.id, 3) != meta.bucket:
                raise AssertionError("record routed to the wrong bucket")
    report = store.verify()
    if not report.readable:
        raise AssertionError("current head has unreadable objects")
    return head


def run():
    report = {"checks": [], "intervals": []}

    def note(name):
        report["checks"].append(name)

    with tempfile.TemporaryDirectory(prefix="rebucket-reconcile-") as root:
        ledger = Ledger()
        source_low = ids_for_source_bucket(0, 3)
        source_high = ids_for_source_bucket(1, 3)
        # Ensure both source buckets have physical rows and revisions.
        seed_one = [(source_low[0], {"v": 1, "rev": 1}),
                    (source_low[1], {"v": 1, "rev": 1}),
                    (source_high[0], {"v": 1, "rev": 1}),
                    (source_high[1], {"v": 1, "rev": 1})]
        seed_two = [(source_low[0], {"v": 2, "rev": 2}),
                    (source_high[0], None)]

        store = Store(root, segment_rows=2, index_stride=2)
        ingest(store, ledger, "reconcile-seed-1", seed_one)
        ingest(store, ledger, "reconcile-seed-2", seed_two)
        old_snapshot = store.freeze("reconcile-old")
        old_rows = ledger.expected()
        source_total = 6
        note("seed history replays")

        task = store.begin_rebucket("reconcile-task")
        if store.begin_rebucket("reconcile-task") != task:
            raise AssertionError("begin is not idempotent for the same request")
        status = store.rebucket_status()
        if (status["total"], status["processed"], status["phase"]) != (
            source_total, 0, "copying"
        ):
            raise AssertionError("unexpected initial status %r" % status)
        during_snapshot = store.freeze("reconcile-during")
        check_current(store, ledger, "after begin")
        note("begin pins the finite source")

        other = Store(root, segment_rows=1, index_stride=1)
        acted = []

        def hook(stage, details):
            if stage != "rebucket_prepared":
                return
            if store.catalog.connection.in_transaction:
                raise AssertionError("rebucket_prepared inside a write txn")
            report["intervals"].append([details["start"], details["stop"]])
            if not acted:
                acted.append(True)
                # A correction and a delete land after some source rows were
                # already copied. Neither may be resurrected by the copy.
                ingest(other, ledger, "reconcile-correct", [
                    (source_low[0], {"v": 9, "rev": 3}),
                    (source_low[1], None),
                ])
            check_current(store, ledger, "inside step hook")

        processed = 0
        while True:
            store.hook = hook
            try:
                step = store.step_rebucket(2)
            finally:
                store.hook = None
            processed += step["processed"]
            if step["ready"]:
                break
            if step["processed"] == 0:
                raise AssertionError("non-final step made no progress")
            check_current(store, ledger, "between steps")
            store.hook = hook
            # Reopen between steps to prove durable continuation.
            store.close()
            store = Store(root, segment_rows=2, index_stride=2)
            store.hook = None
            if store.begin_rebucket("reconcile-task") != task:
                raise AssertionError("task identity changed after reopen")
            if store.rebucket_status()["processed"] != processed:
                raise AssertionError("progress was not durable")
        if processed != source_total:
            raise AssertionError("processed %r != total %r"
                                 % (processed, source_total))
        note("steps resume across reopen and close the source")

        expected_intervals = [[0, 2], [2, 4], [4, 6]]
        if report["intervals"] != expected_intervals:
            raise AssertionError("source intervals %r" % report["intervals"])
        note("source intervals are contiguous")

        # Admission after ready but before cutover must survive cutover.
        ingest(store, ledger, "reconcile-after-ready", [
            (source_high[1], {"v": 7}),
        ])
        note("admission after ready is accepted")

        # An admission prepared under the old layout but published after the
        # switch must republish into the target layout, never vanish.
        crossing = []

        def crossing_hook(stage, details):
            if stage == "admission_prepared" and not crossing:
                crossing.append(True)
                if not other.finish_rebucket():
                    raise AssertionError("cutover refused a ready task")

        store.hook = crossing_hook
        try:
            ingest(store, ledger, "reconcile-crossing", [
                ("crossing-row", {"v": 1}),
            ])
        finally:
            store.hook = None
        if not crossing:
            raise AssertionError("crossing admission was not exercised")
        note("prepared admission survives cutover")

        if not store.finish_rebucket():
            raise AssertionError("finish refused a fully prepared task")
        if not store.finish_rebucket():
            raise AssertionError("finish is not idempotent")
        if store.step_rebucket(1) != {"processed": 0, "ready": True}:
            raise AssertionError("step after finish is not a no-op")
        note("cutover is idempotent")

        check_current(store, ledger, "after cutover")
        if list(store.scan(old_snapshot)) != old_rows:
            raise AssertionError("old snapshot drifted")
        if list(store.scan(during_snapshot)) != old_rows:
            raise AssertionError("mid-task snapshot drifted")
        check_finished_layout(store)
        note("current head depends only on the target layout")

        # Business commits continue from the last successful admission.
        receipt = ingest(store, ledger, "reconcile-tail", [("tail", {"v": 5})])
        if receipt.commit != ledger.commit:
            raise AssertionError("tail commit mismatch")
        if list(store.scan(old_snapshot)) != old_rows:
            raise AssertionError("old snapshot drifted after tail write")
        note("business commit sequence has no migration gaps")

        store.close()
        with Store(root) as reopened:
            check_current(reopened, ledger, "reopened current")
            if list(reopened.scan(old_snapshot)) != old_rows:
                raise AssertionError("reopened old snapshot drifted")
            check_finished_layout(reopened)
            if reopened.rebucket_status()["phase"] != "finished":
                raise AssertionError("task phase was not durable")
        note("finished state survives reopen")
        other.close()

    return report


def main():
    report = run()
    for name in report["checks"]:
        print("RECONCILE_OK " + name)
    print("RECONCILE_SUMMARY " + json.dumps(
        {"checks": len(report["checks"]),
         "intervals": report["intervals"]},
        ensure_ascii=False, sort_keys=True,
    ))


if __name__ == "__main__":
    main()

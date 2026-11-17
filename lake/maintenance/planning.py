from dataclasses import dataclass

from lake.catalog.manifests import get_head
from lake.catalog.schema import FIXED_BUCKET_COUNT


@dataclass(frozen=True)
class CompactionPlan:
    head_id: int
    commit: int
    bucket: int
    inputs: tuple
    retained: tuple
    bucket_count: int = FIXED_BUCKET_COUNT

    @property
    def needs_merge(self):
        return len(self.inputs) > 1


def validate_bucket(bucket):
    if type(bucket) is not int or not 0 <= bucket < FIXED_BUCKET_COUNT:
        raise ValueError("bucket must be 0 or 1")
    return bucket


def plan_compaction(catalog, bucket):
    validate_bucket(bucket)
    with catalog.read_transaction():
        head = get_head(catalog)
        if bucket >= head.bucket_count:
            raise ValueError("bucket is outside the active layout")
        # Only segments belonging to the head's own layout are compacted. A
        # rebucket keeps its pinned source segments in the same head under the
        # retired layout; those are inputs to the copy, not to compaction.
        inputs = tuple(
            meta for meta in head.segments
            if meta.bucket == bucket and meta.bucket_count == head.bucket_count
        )
        retained = tuple(
            meta for meta in head.segments
            if not (meta.bucket == bucket
                    and meta.bucket_count == head.bucket_count)
        )
        return CompactionPlan(
            head.id, head.commit, bucket, inputs, retained,
            head.bucket_count,
        )

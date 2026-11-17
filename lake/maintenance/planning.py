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
        return CompactionPlan(
            head.id, head.commit, bucket,
            tuple(meta for meta in head.segments if meta.bucket == bucket),
            tuple(meta for meta in head.segments if meta.bucket != bucket),
        )

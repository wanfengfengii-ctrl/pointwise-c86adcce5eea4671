from dataclasses import dataclass, replace

from lake.maintenance.planning import plan_compaction
from lake.maintenance.merge import prepare_compaction
from lake.maintenance.publication import publish_compaction
from lake.objects.integrity import CorruptObject, check_object
from lake.objects.paths import object_path
from lake.reads.binding import bind_read
from lake.segments.reader import open_segment


@dataclass(frozen=True)
class VerificationReport:
    manifest_id: int
    commit: int
    objects: tuple

    @property
    def readable(self):
        return all(item.ok for item in self.objects)

    def to_dict(self):
        return {
            "manifest_id": self.manifest_id, "commit": self.commit,
            "readable": self.readable,
            "objects": [item.to_dict() for item in self.objects],
        }


def compact_bucket(catalog, bucket, emit, index_stride=8):
    plan = plan_compaction(catalog, bucket)
    if not plan.needs_merge:
        return plan.head_id
    prepared = prepare_compaction(catalog, plan, emit, index_stride)
    return publish_compaction(catalog, prepared, emit)


def verify_objects(catalog, snapshot=None):
    binding = bind_read(catalog, snapshot)
    manifest = binding.manifest
    checks = {}
    dependencies = []
    for meta in manifest.segments:
        pair = []
        for kind, digest, size in (
            ("data", meta.data_digest, meta.data_size),
            ("index", meta.index_digest, meta.index_size),
        ):
            address = str(object_path(catalog.paths, kind, digest))
            pair.append(address)
            result = check_object(catalog.paths, kind, digest, size)
            if address not in checks or not result.ok:
                checks[address] = result
        dependencies.append((meta, pair))
    for meta, pair in dependencies:
        if not all(checks[address].ok for address in pair):
            continue
        try:
            open_segment(catalog.paths, meta, manifest.commit)
        except CorruptObject as exc:
            for address in pair:
                checks[address] = replace(
                    checks[address], ok=False, code=exc.code, reason=exc.reason
                )
    return VerificationReport(
        manifest.id, manifest.commit,
        tuple(checks[address] for address in sorted(checks)),
    )

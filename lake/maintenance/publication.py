from lake.catalog.manifests import get_head, create_manifest
from lake.segments.reader import open_segment


class StalePlan(Exception):
    pass


def publish_compaction(catalog, prepared, emit):
    plan = prepared.plan
    for meta in prepared.outputs:
        open_segment(catalog.paths, meta, plan.commit)
    emit("before_compaction_publish", {"head_id": plan.head_id})
    with catalog.write_transaction():
        head = get_head(catalog)
        inputs = tuple(meta for meta in head.segments
                       if meta.bucket == plan.bucket)
        if head.id != plan.head_id or inputs != plan.inputs:
            raise StalePlan("compaction input head has changed")
        manifest_id = create_manifest(
            catalog, plan.commit, plan.retained + prepared.outputs
        )
        emit("compaction_metadata_after_manifest", {"manifest_id": manifest_id})
        catalog.connection.execute(
            "UPDATE current_head SET manifest_id=? WHERE singleton=1",
            (manifest_id,),
        )
        return manifest_id

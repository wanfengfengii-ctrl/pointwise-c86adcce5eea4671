from dataclasses import dataclass

from lake.catalog.manifests import get_head, get_manifest, resolve_snapshot


@dataclass(frozen=True)
class ReadBinding:
    manifest: object


def bind_read(catalog, snapshot=None):
    with catalog.read_transaction():
        if snapshot is None:
            manifest = get_head(catalog)
        else:
            fixed = resolve_snapshot(catalog, snapshot)
            manifest = get_manifest(catalog, fixed.manifest_id)
        return ReadBinding(manifest)

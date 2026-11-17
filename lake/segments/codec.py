from __future__ import annotations

import json
import re
from dataclasses import dataclass

from lake.admission.validation import validate_id, validate_payload

SEGMENT_FORMAT_VERSION = 1
_DIGEST = re.compile(r"[0-9a-f]{64}")


def canonical_json_bytes(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def strict_json_loads(data):
    def reject(value):
        raise ValueError("nonfinite JSON constant")
    return json.loads(data, object_pairs_hook=_pairs, parse_constant=reject)


def _positive(value, name, allow_zero=False):
    if type(value) is not int or value < (0 if allow_zero else 1):
        raise ValueError("invalid " + name)


@dataclass(frozen=True)
class SegmentRecord:
    id: str
    commit: int
    kind: str
    value_json: str | None = None

    @property
    def payload(self):
        return None if self.value_json is None else json.loads(self.value_json)


@dataclass(frozen=True)
class SegmentMeta:
    bucket: int
    count: int
    min_id: str
    max_id: str
    data_digest: str
    index_digest: str
    data_size: int
    index_size: int


def encode_record(record):
    validate_id(record.id)
    _positive(record.commit, "logical commit")
    value = {"v": SEGMENT_FORMAT_VERSION, "id": record.id,
             "commit": record.commit, "op": record.kind}
    if record.kind == "put":
        payload = record.payload
        validate_payload(payload)
        value["payload"] = payload
    elif record.kind != "delete" or record.value_json is not None:
        raise ValueError("invalid record operation")
    return canonical_json_bytes(value) + b"\n"


def decode_record(line):
    if not isinstance(line, bytes) or not line.endswith(b"\n"):
        raise ValueError("record must be a complete JSONL line")
    value = strict_json_loads(line)
    if type(value) is not dict:
        raise ValueError("record must be an object")
    kind = value.get("op")
    expected = {"v", "id", "commit", "op"}
    if kind == "put":
        expected.add("payload")
    if set(value) != expected or type(value.get("v")) is not int:
        raise ValueError("invalid record fields")
    if value["v"] != SEGMENT_FORMAT_VERSION:
        raise ValueError("unsupported segment version")
    validate_id(value["id"])
    _positive(value["commit"], "logical commit")
    if kind == "put":
        validate_payload(value["payload"])
        payload = canonical_json_bytes(value["payload"]).decode("utf-8")
    elif kind == "delete":
        payload = None
    else:
        raise ValueError("invalid record operation")
    return SegmentRecord(value["id"], value["commit"], kind, payload)


def encode_segment_meta(meta):
    value = dict(vars(meta))
    value["v"] = SEGMENT_FORMAT_VERSION
    encoded = canonical_json_bytes(value)
    decode_segment_meta(encoded)
    return encoded


def decode_segment_meta(data):
    value = strict_json_loads(data)
    fields = {"bucket", "count", "min_id", "max_id", "data_digest",
              "index_digest", "data_size", "index_size", "v"}
    if type(value) is not dict or set(value) != fields:
        raise ValueError("invalid segment descriptor fields")
    version = value.pop("v")
    if type(version) is not int or version != SEGMENT_FORMAT_VERSION:
        raise ValueError("unsupported descriptor version")
    if type(value["bucket"]) is not int or value["bucket"] not in (0, 1):
        raise ValueError("invalid fixed bucket")
    for name in ("count", "data_size", "index_size"):
        _positive(value[name], name)
    validate_id(value["min_id"])
    validate_id(value["max_id"])
    if value["min_id"] > value["max_id"]:
        raise ValueError("invalid id bounds")
    for name in ("data_digest", "index_digest"):
        if type(value[name]) is not str or not _DIGEST.fullmatch(value[name]):
            raise ValueError("invalid digest")
    return SegmentMeta(**value)

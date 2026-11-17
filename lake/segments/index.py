from bisect import bisect_right
from dataclasses import dataclass

from lake.segments.codec import canonical_json_bytes, strict_json_loads
from lake.admission.validation import validate_id


@dataclass(frozen=True)
class SparseIndex:
    stride: int
    count: int
    data_size: int
    anchors: tuple


def build_index(records, lines, stride=8):
    if type(stride) is not int or stride <= 0:
        raise ValueError("index stride must be positive")
    if len(records) != len(lines) or not records:
        raise ValueError("index needs nonempty matching records and lines")
    anchors = []
    offset = 0
    last_anchor = -stride
    previous_id = None
    for ordinal, (record, line) in enumerate(zip(records, lines)):
        if record.id != previous_id and ordinal - last_anchor >= stride:
            anchors.append((record.id, ordinal, offset))
            last_anchor = ordinal
        previous_id = record.id
        offset += len(line)
    return SparseIndex(stride, len(records), offset, tuple(anchors))


def encode_index(index):
    return canonical_json_bytes({
        "v": 1, "stride": index.stride, "count": index.count,
        "data_size": index.data_size, "anchors": index.anchors,
    })


def decode_index(data):
    value = strict_json_loads(data)
    if type(value) is not dict or set(value) != {
        "v", "stride", "count", "data_size", "anchors"
    }:
        raise ValueError("invalid index fields")
    if type(value["v"]) is not int or value["v"] != 1:
        raise ValueError("unsupported index version")
    for key in ("stride", "count", "data_size"):
        if type(value[key]) is not int or value[key] <= 0:
            raise ValueError("invalid index " + key)
    if type(value["anchors"]) is not list or not value["anchors"]:
        raise ValueError("missing index anchors")
    anchors = []
    for anchor in value["anchors"]:
        if type(anchor) is not list or len(anchor) != 3:
            raise ValueError("invalid index anchor")
        id, ordinal, offset = anchor
        validate_id(id)
        if any(type(number) is not int or number < 0
               for number in (ordinal, offset)):
            raise ValueError("invalid anchor coordinate")
        if ordinal >= value["count"] or offset >= value["data_size"]:
            raise ValueError("anchor outside segment")
        if anchors and not all(
            left < right for left, right in zip(anchors[-1], anchor)
        ):
            raise ValueError("anchors must increase")
        anchors.append(tuple(anchor))
    if anchors[0][1:] != (0, 0):
        raise ValueError("first anchor must start at zero")
    return SparseIndex(value["stride"], value["count"],
                       value["data_size"], tuple(anchors))


def validate_index(index, records, lines):
    expected = build_index(records, lines, index.stride)
    if index != expected:
        raise ValueError("index coordinates do not match data ID groups")
    return index


def locate_block(index, id):
    position = bisect_right([entry[0] for entry in index.anchors], id) - 1
    position = max(position, 0)
    _, ordinal, start = index.anchors[position]
    stop = (index.anchors[position + 1][2]
            if position + 1 < len(index.anchors) else index.data_size)
    return ordinal, start, stop

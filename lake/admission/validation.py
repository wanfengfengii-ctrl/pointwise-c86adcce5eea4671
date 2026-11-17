import math


def validate_id(value):
    if not isinstance(value, str) or not value:
        raise ValueError("id must be a nonempty string")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError("id must be UTF-8 encodable") from exc
    return value


def validate_request_id(value):
    return validate_id(value)


def _json_value(value, active):
    kind = type(value)
    if kind is str:
        try:
            value.encode("utf-8")
        except UnicodeError as exc:
            raise ValueError("invalid JSON string") from exc
    elif kind in (bool, int, type(None)):
        return
    elif kind is float:
        if not math.isfinite(value):
            raise ValueError("nonfinite JSON number")
    elif kind in (dict, list):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON value")
        active.add(identity)
        try:
            if kind is dict:
                for key, item in value.items():
                    if type(key) is not str:
                        raise ValueError("JSON object keys must be strings")
                    _json_value(key, active)
                    _json_value(item, active)
            else:
                for item in value:
                    _json_value(item, active)
        finally:
            active.remove(identity)
    else:
        raise ValueError("unsupported JSON value")


def validate_payload(value):
    if type(value) is not dict:
        raise ValueError("put payload must be a JSON object")
    _json_value(value, set())
    return value


def validate_operation(operation):
    validate_id(operation.id)
    if operation.kind == "put":
        validate_payload(operation.payload)
    elif operation.kind == "delete":
        if operation.payload is not None:
            raise ValueError("delete cannot carry a payload")
    else:
        raise ValueError("unknown operation")


def validate_unique_ids(operations):
    seen = set()
    for operation in operations:
        if operation.id in seen:
            raise ValueError("duplicate id in batch")
        seen.add(operation.id)

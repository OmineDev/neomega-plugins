"""Explicit, non-coercing subset for consumer-owned profile field schemas."""
import math
from neomega_runtime.services import ServiceRejected


def invalid():
    raise ServiceRejected('invalid_field_schema')


def check_schema(schema, depth=0):
    if not isinstance(schema, dict) or depth > 16:
        invalid()
    allowed = {'type', 'properties', 'required', 'additionalProperties', 'items',
               'enum', 'minimum', 'maximum', 'minLength', 'maxLength', 'minItems',
               'maxItems', 'title', 'description'}
    if set(schema) - allowed:
        invalid()
    kind = schema.get('type')
    if kind not in ('object', 'array', 'string', 'integer', 'number', 'boolean', 'null'):
        invalid()
    scoped = {'properties': 'object', 'required': 'object', 'additionalProperties': 'object',
              'items': 'array', 'minItems': 'array', 'maxItems': 'array',
              'minLength': 'string', 'maxLength': 'string'}
    if any(key in schema and kind != expected for key, expected in scoped.items()): invalid()
    if any(key in schema for key in ('minimum', 'maximum')) and kind not in ('integer', 'number'): invalid()
    for key in ('title', 'description'):
        if key in schema and not isinstance(schema[key], str): invalid()
    if 'enum' in schema and (not isinstance(schema['enum'], list) or not schema['enum']): invalid()
    for key in ('minimum', 'maximum'):
        if key in schema and (type(schema[key]) not in (int, float) or not math.isfinite(schema[key])): invalid()
    for key in ('minLength', 'maxLength', 'minItems', 'maxItems'):
        if key in schema and (type(schema[key]) is not int or schema[key] < 0): invalid()
    for low, high in [('minimum', 'maximum'), ('minLength', 'maxLength'), ('minItems', 'maxItems')]:
        if low in schema and high in schema and schema[low] > schema[high]: invalid()
    if kind == 'object':
        props = schema.get('properties', {})
        required = schema.get('required', [])
        if not isinstance(props, dict) or not isinstance(required, list) or any(not isinstance(k, str) or k not in props for k in required): invalid()
        for child in props.values(): check_schema(child, depth + 1)
        extra = schema.get('additionalProperties', False)
        if isinstance(extra, dict): check_schema(extra, depth + 1)
        elif type(extra) is not bool: invalid()
    if kind == 'array':
        check_schema(schema.get('items'), depth + 1)


def validate(schema, value):
    kind = schema['type']
    valid = {'object': isinstance(value, dict), 'array': isinstance(value, list),
             'string': isinstance(value, str), 'integer': type(value) is int,
             'number': type(value) in (int, float), 'boolean': type(value) is bool,
             'null': value is None}[kind]
    if not valid: raise ServiceRejected('fields_schema_mismatch')
    if 'enum' in schema and not any(type(value) is type(v) and value == v for v in schema['enum']): raise ServiceRejected('fields_schema_mismatch')
    if kind in ('integer', 'number'):
        if not math.isfinite(value) or value < schema.get('minimum', -math.inf) or value > schema.get('maximum', math.inf): raise ServiceRejected('fields_schema_mismatch')
    if kind in ('string', 'array'):
        lower, upper = ('minLength', 'maxLength') if kind == 'string' else ('minItems', 'maxItems')
        if not schema.get(lower, 0) <= len(value) <= schema.get(upper, math.inf): raise ServiceRejected('fields_schema_mismatch')
    if kind == 'array':
        for item in value: validate(schema['items'], item)
    if kind == 'object':
        if any(k not in value for k in schema.get('required', [])): raise ServiceRejected('fields_schema_mismatch')
        for key, item in value.items():
            child = schema.get('properties', {}).get(key, schema.get('additionalProperties', False))
            if child is False: raise ServiceRejected('fields_schema_mismatch')
            if isinstance(child, dict): validate(child, item)

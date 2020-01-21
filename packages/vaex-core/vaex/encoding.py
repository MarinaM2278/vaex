import json

import numpy as np

from .json import VaexJsonDecoder, VaexJsonEncoder
import vaex

registry = {}


def register(name):
    def wrapper(cls):
        assert name not in registry
        registry[name] = cls
        return cls
    return wrapper


@register("dtype")
class dtype_encoding:
    @staticmethod
    def encode(encoding, dtype):
        if dtype == str:
            return "str"
        else:
            if type(dtype) == type:
                dtype = dtype().dtype
        return str(dtype)

    @staticmethod
    def decode(encoding, type_spec):
        if type_spec == "str":
            return str
        else:
            return np.dtype(type_spec)


@register("binner")
class grid_encoding:
    @staticmethod
    def encode(encoding, binner):
        name = type(binner).__name__
        if name.startswith('BinnerOrdinal_'):
            datatype = name[len('BinnerOrdinal_'):]
            if datatype.endswith("_non_native"):
                datatype = datatype[:-len('64_non_native')]
                datatype = encoding.encode('dtype', np.dtype(datatype).newbyteorder())
            return {'type': 'ordinal', 'expression': binner.expression, 'datatype': datatype, 'count': binner.ordinal_count, 'minimum': binner.min_value}
        elif name.startswith('BinnerScalar_'):
            datatype = name[len('BinnerScalar_'):]
            if datatype.endswith("_non_native"):
                datatype = datatype[:-len('64_non_native')]
                datatype = encoding.encode('dtype', np.dtype(datatype).newbyteorder())
            return {'type': 'scalar', 'expression': binner.expression, 'datatype': datatype, 'count': binner.bins, 'minimum': binner.vmin, 'maximum': binner.vmax}
        else:
            raise ValueError('Cannot serialize: %r' % binner)

    @staticmethod
    def decode(encoding, binner_spec):
        type = binner_spec['type']
        dtype = encoding.decode('dtype', binner_spec['datatype'])
        if type == 'ordinal':
            cls = vaex.utils.find_type_from_dtype(vaex.superagg, "BinnerOrdinal_", dtype)
            return cls(binner_spec['expression'], binner_spec['count'], binner_spec['minimum'])
        elif type == 'scalar':
            cls = vaex.utils.find_type_from_dtype(vaex.superagg, "BinnerScalar_", dtype)
            return cls(binner_spec['expression'], binner_spec['minimum'], binner_spec['maximum'], binner_spec['count'])
        else:
            raise ValueError('Cannot deserialize: %r' % binner_spec)


@register("grid")
class grid_encoding:
    @staticmethod
    def encode(encoding, grid):
        return encoding.encode_list('binner', grid.binners)

    @staticmethod
    def decode(encoding, grid_spec):
        return vaex.superagg.Grid(encoding.decode_list('binner', grid_spec))


class Encoding:
    def __init__(self, next=None):
        self.registry = {**registry}
        # self.next = None
        self.buffers = []
        self.buffer_paths = []

    def encode(self, typename, value):
        encoded = self.registry[typename].encode(self, value)
        return encoded

    def encode_list(self, typename, values):
        encoded = [self.registry[typename].encode(self, k) for k in values]
        return encoded
    
    def encode_dict(self, typename, values):
        encoded = {key: self.registry[typename].encode(self, value) for key, value in values.items()}
        return encoded

    def decode(self, typename, value, **kwargs):
        decoded = self.registry[typename].decode(self, value, **kwargs)
        return decoded

    def decode_list(self, typename, values, **kwargs):
        decoded = [self.registry[typename].decode(self, k, **kwargs) for k in values]
        return decoded

    def decode_dict(self, typename, values, **kwargs):
        decoded = {key: self.registry[typename].decode(self, value, **kwargs) for key, value in values.items()}
        return decoded



def serialize(data, encoding):
    return json.dumps(data, indent=2, cls=VaexJsonEncoder)


def deserialize(data, encoding):
    return json.loads(data, cls=VaexJsonDecoder)

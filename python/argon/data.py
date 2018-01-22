#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from argon.common import _Buffer, _struct, _hex, _namedtuple, _uuid_bytes

class _AmqpDataType:
    def __init__(self, name, python_type, format_code, descriptor=None):
        assert name is not None
        assert python_type is not None
        assert format_code is not None

        self.name = name
        self.python_type = python_type
        self.format_code = format_code
        self.descriptor = descriptor

    def __repr__(self):
        return self.name

    def emit(self, buff, offset, value):
        assert isinstance(value, self.python_type)

        offset, format_code_offset = self.emit_constructor(buff, offset)
        offset, format_code = self.emit_value(buff, offset, value)

        buff.pack(format_code_offset, 1, "!B", format_code)

        return offset

    def emit_constructor(self, buff, offset):
        if self.descriptor is not None:
            offset = buff.pack(offset, 1, "!B", 0x00)
            offset = emit_data(buff, offset, self.descriptor)

        offset, format_code_offset = buff.skip(offset, 1) # The format code is filled in later

        return offset, format_code_offset

    def emit_value(self, buff, offset, value):
        return self.emit_value_long(buff, offset, value)

    def emit_value_long(self, buff, offset, value):
        raise NotImplementedError()

class AmqpNullType(_AmqpDataType):
    def __init__(self, descriptor=None):
        super().__init__("null", type(None), 0x40, descriptor=descriptor)

    def emit_value_long(self, buff, offset, value):
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return offset, None

class AmqpBooleanType(_AmqpDataType):
    def __init__(self, descriptor=None):
        super().__init__("boolean", bool, 0x56, descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value is True: return offset, 0x41
        if value is False: return offset, 0x42

        raise Exception()

    def emit_value_long(self, buff, offset, value):
        if value is True: return buff.pack(offset, 1, "!B", 0x01), self.format_code
        if value is False: return buff.pack(offset, 1, "!B", 0x00), self.format_code

        raise Exception()

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x41: return offset, True
        if format_code == 0x42: return offset, False

        offset, value = buff.unpack(offset, 1, "!B")

        return offset, value == 0x01

class _AmqpFixedWidthType(_AmqpDataType):
    def __init__(self, name, python_type, format_code, format_string, descriptor=None):
        super().__init__(name, python_type, format_code, descriptor=descriptor)

        self.format_string = format_string
        self.format_size = _struct.calcsize(self.format_string)

    def emit_value_long(self, buff, offset, value):
        offset = buff.pack(offset, self.format_size, self.format_string, value)
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return buff.unpack(offset, self.format_size, self.format_string)

class AmqpUnsignedByteType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("ubyte", int, 0x50, "!B", descriptor=descriptor)

class AmqpUnsignedShortType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("ushort", int, 0x60, "!H", descriptor=descriptor)

class AmqpUnsignedIntType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("uint", int, 0x70, "!I", descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value == 0: return offset, 0x43
        if value < 256: return buff.pack(offset, 1, "!B", value), 0x52

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x43: return offset, 0
        if format_code == 0x52: return buff.unpack(offset, 1, "!B")

        return super().parse_value(buff, offset, format_code)

class AmqpUnsignedLongType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("ulong", int, 0x80, "!Q", descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value == 0: return offset, 0x44
        if value < 256: return buff.pack(offset, 1, "!B", value), 0x53

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x44: return offset, 0
        if format_code == 0x53: return buff.unpack(offset, 1, "!B")

        return super().parse_value(buff, offset, format_code)

class AmqpByteType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("byte", int, 0x51, "!b", descriptor=descriptor)

class AmqpShortType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("short", int, 0x61, "!h", descriptor=descriptor)

class AmqpIntType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("int", int, 0x71, "!i", descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 1, "!b", value), 0x54

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x54: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class AmqpLongType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("long", int, 0x81, "!q", descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 1, "!b", value), 0x55

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x55: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class AmqpFloatType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("float", float, 0x72, "!f", descriptor=descriptor)

class AmqpDoubleType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("double", float, 0x82, "!d", descriptor=descriptor)

class AmqpCharType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("char", str, 0x73, "!4s", descriptor=descriptor)

    def emit_value_long(self, buff, offset, value):
        value = value.encode("utf-32-be")
        return super().emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, value.decode("utf-32-be")

class AmqpUuidType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("uuid", bytes, 0x98, "!16s", descriptor=descriptor)

class AmqpTimestampType(_AmqpFixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("timestamp", float, 0x83, "!q", descriptor=descriptor)

    def emit_value_long(self, buff, offset, value):
        value = int(round(value * 1000))
        return super().emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, round(value / 1000, 3)

class _AmqpVariableWidthType(_AmqpDataType):
    def __init__(self, name, python_type, short_format_code, long_format_code, descriptor=None):
        super().__init__(name, python_type, long_format_code, descriptor=descriptor)

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        return value

    def decode(self, octets):
        return octets

    def emit_value(self, buff, offset, value):
        if len(value) < 256:
            return self.emit_value_short(buff, offset, value)

        return self.emit_value_long(buff, offset, value)

    def emit_value_short(self, buff, offset, value):
        octets = self.encode(value)

        offset = buff.pack(offset, 1, "!B", len(octets))
        offset = buff.write(offset, octets)

        return offset, self.short_format_code

    def emit_value_long(self, buff, offset, value):
        octets = self.encode(value)

        offset = buff.pack(offset, 4, "!I", len(octets))
        offset = buff.write(offset, octets)

        return offset, self.long_format_code

    def parse_value(self, buff, offset, format_code):
        assert format_code in (self.short_format_code, self.long_format_code)

        if format_code == self.short_format_code:
            offset, size = buff.unpack(offset, 1, "!B")
        else:
            offset, size = buff.unpack(offset, 4, "!I")

        offset, octets = buff.read(offset, size)
        value = self.decode(octets)

        return offset, value

class AmqpBinaryType(_AmqpVariableWidthType):
    def __init__(self, descriptor=None):
        super().__init__("binary", bytes, 0xa0, 0xb0, descriptor=descriptor)

class AmqpStringType(_AmqpVariableWidthType):
    def __init__(self, descriptor=None):
        super().__init__("string", str, 0xa1, 0xb1, descriptor=descriptor)

    def encode(self, value):
        return value.encode("utf-8")

    def decode(self, octets):
        return bytes(octets).decode("utf-8")

    def emit_value(self, buff, offset, value):
        if len(value) < 64:
            return self.emit_value_short(buff, offset, value)

        return self.emit_value_long(buff, offset, value)

class AmqpSymbolType(_AmqpVariableWidthType):
    def __init__(self, descriptor=None):
        super().__init__("symbol", str, 0xa3, 0xb3, descriptor=descriptor)

    def encode(self, value):
        return value.encode("ascii")

    def decode(self, octets):
        return bytes(octets).decode("ascii")

class _AmqpCollectionType(_AmqpDataType):
    def __init__(self, name, python_type, short_format_code, long_format_code, descriptor=None):
        super().__init__(name, python_type, long_format_code, descriptor=descriptor)

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def parse_size_and_count(self, buff, offset, format_code):
        assert format_code in (self.short_format_code, self.long_format_code)

        if format_code == self.short_format_code:
            return buff.unpack(offset, 2, "!BB")

        if format_code == self.long_format_code:
            return buff.unpack(offset, 8, "!II")

        raise Exception()

class _AmqpCompoundType(_AmqpCollectionType):
    def __init__(self, name, python_type, short_format_code, long_format_code, descriptor=None):
        super().__init__(name, python_type, short_format_code, long_format_code, descriptor=descriptor)

    def encode_into(self, buff, offset, value):
        for item in value:
            offset = emit_data(buff, offset, item)

        return offset, len(value)

    def decode_from(self, buff, offset, count):
        # assert count < 1000, count # XXX This is incorrect, but it catches some codec bugs

        value = [None] * count

        for i in range(count):
            offset, value[i] = parse_data(buff, offset)

        return offset, value

    def emit_value_long(self, buff, offset, value):
        offset, size_offset = buff.skip(offset, 4)
        offset, count_offset = buff.skip(offset, 4)

        offset, count = self.encode_into(buff, offset, value)

        size = offset - count_offset
        buff.pack(size_offset, 8, "!II", size, count)

        return offset, self.long_format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)
        offset, value = self.decode_from(buff, offset, count)

        return offset, value

class AmqpListType(_AmqpCompoundType):
    def __init__(self, descriptor=None):
        super().__init__("list", list, 0xc0, 0xd0, descriptor=descriptor)

class AmqpMapType(_AmqpCompoundType):
    def __init__(self, descriptor=None):
        super().__init__("map", dict, 0xc1, 0xd1, descriptor=descriptor)

    def encode_into(self, buff, offset, value):
        elems = list()

        for item in value.items():
            elems.extend(item)

        return super().encode_into(buff, offset, elems)

    def decode_from(self, buff, offset, count):
        offset, elems = super().decode_from(buff, offset, count)
        pairs = dict()

        for i in range(0, len(elems), 2):
            pairs[elems[i]] = elems[i + 1]

        return offset, pairs

class AmqpArrayType(_AmqpCollectionType):
    def __init__(self, element_type, descriptor=None):
        super().__init__("array", list, 0xf0, 0xe0, descriptor=descriptor)

        self.element_type = element_type

    def __repr__(self):
        return "{}<{}>".format(self.name, self.element_type)

    def encode_into(self, buff, offset, value):
        for elem in value:
            offset, format_code = self.element_type.emit_value_long(buff, offset, elem)

        return offset, len(value)

    def decode_from(self, buff, offset, count, element_type, element_format_code):
        value = [None] * count

        for i in range(count):
            offset, value[i] = element_type.parse_value(buff, offset, element_format_code)

        return offset, value

    def emit_value_long(self, buff, offset, value):
        assert self.element_type is not None

        offset, size_offset = buff.skip(offset, 4)
        offset, count_offset = buff.skip(offset, 4)

        offset, element_format_code_offset = self.element_type.emit_constructor(buff, offset)
        buff.pack(element_format_code_offset, 1, "!B", self.element_type.format_code)

        offset, count = self.encode_into(buff, offset, value)

        size = offset - count_offset
        buff.pack(size_offset, 8, "!II", size, count)

        return offset, self.long_format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)
        offset, element_format_code = buff.unpack(offset, 1, "!B")

        element_type = _get_data_type_for_format_code(element_format_code)

        offset, value = self.decode_from(buff, offset, count, element_type, element_format_code)

        return offset, value

amqp_null_type = AmqpNullType()
amqp_boolean_type = AmqpBooleanType()

amqp_ubyte_type = AmqpUnsignedByteType()
amqp_ushort_type = AmqpUnsignedShortType()
amqp_uint_type = AmqpUnsignedIntType()
amqp_ulong_type = AmqpUnsignedLongType()

amqp_byte_type = AmqpByteType()
amqp_short_type = AmqpShortType()
amqp_int_type = AmqpIntType()
amqp_long_type = AmqpLongType()

amqp_float_type = AmqpFloatType()
amqp_double_type = AmqpDoubleType()

amqp_char_type = AmqpCharType()
amqp_timestamp_type = AmqpTimestampType()
amqp_uuid_type = AmqpUuidType()

amqp_binary_type = AmqpBinaryType()
amqp_string_type = AmqpStringType()
amqp_symbol_type = AmqpSymbolType()

amqp_list_type = AmqpListType()
amqp_map_type = AmqpMapType()
amqp_array_type = AmqpArrayType(None)

_data_types_by_format_code = {
    0x40: amqp_null_type,
    0x41: amqp_boolean_type,
    0x42: amqp_boolean_type,
    0x43: amqp_uint_type,
    0x44: amqp_ulong_type,
    0x50: amqp_ubyte_type,
    0x51: amqp_byte_type,
    0x52: amqp_uint_type,
    0x53: amqp_ulong_type,
    0x54: amqp_int_type,
    0x55: amqp_long_type,
    0x56: amqp_boolean_type,
    0x60: amqp_ushort_type,
    0x61: amqp_short_type,
    0x70: amqp_uint_type,
    0x71: amqp_int_type,
    0x72: amqp_float_type,
    0x73: amqp_char_type,
    0x80: amqp_ulong_type,
    0x81: amqp_long_type,
    0x82: amqp_double_type,
    0x83: amqp_timestamp_type,
    0x98: amqp_uuid_type,
    0xa0: amqp_binary_type,
    0xa1: amqp_string_type,
    0xa3: amqp_symbol_type,
    0xb0: amqp_binary_type,
    0xb1: amqp_string_type,
    0xb3: amqp_symbol_type,
    0xc0: amqp_list_type,
    0xc1: amqp_map_type,
    0xd0: amqp_list_type,
    0xd1: amqp_map_type,
    0xe0: amqp_array_type,
    0xf0: amqp_array_type,
}

def _get_data_type_for_format_code(format_code):
    try:
        return _data_types_by_format_code[format_code]
    except KeyError:
        raise Exception("No data type for format code 0x{:02X}".format(format_code))

class UnsignedLong(int):
    _amqp_type = amqp_ulong_type

class Symbol(str):
    _amqp_type = amqp_symbol_type

def _get_data_type_for_python_type(python_type):
    if hasattr(python_type, "_amqp_type"):
        return python_type._amqp_type

    if issubclass(python_type, int):
        return amqp_long_type

    if issubclass(python_type, float):
        return amqp_double_type

    if issubclass(python_type, bytes):
        return amqp_binary_type

    if issubclass(python_type, str):
        return amqp_string_type

    if issubclass(python_type, list):
        return amqp_list_type

    if issubclass(python_type, dict):
        return amqp_map_type

    if python_type is type(None):
        return amqp_null_type

    raise Exception("No data type for Python type {}".format(python_type))

def get_data_type(value):
    return _get_data_type_for_python_type(type(value))

def emit_data(buff, offset, value):
    data_type = get_data_type(value)
    return data_type.emit(buff, offset, value)

def parse_data(buff, offset):
    offset, format_code, descriptor = _parse_constructor(buff, offset)
    data_type = _get_data_type_for_format_code(format_code)

    return data_type.parse_value(buff, offset, format_code)

def _parse_constructor(buff, offset):
    offset, format_code = buff.unpack(offset, 1, "!B")
    descriptor = None

    if format_code == 0x00:
        offset, descriptor = parse_data(buff, offset)
        offset, format_code = buff.unpack(offset, 1, "!B")

    return offset, format_code, descriptor

def _main():
    try:
        import utime as time
    except ImportError:
        import time

    try:
        import urandom as random
    except ImportError:
        import random

    def _shorten(string, max_=20):
        return string[:min(max_, len(string))]

    data = [
        (amqp_null_type, None),
        (AmqpNullType(Symbol("a")), None),

        (amqp_boolean_type, True),
        (amqp_boolean_type, False),

        (amqp_ubyte_type, 0),
        (amqp_ubyte_type, 0xff),
        (amqp_ushort_type, 0),
        (amqp_ushort_type, 0xffff),
        (amqp_uint_type, 0),
        (amqp_uint_type, 128),
        (amqp_uint_type, 0xffffffff),
        (amqp_ulong_type, 0),
        (amqp_ulong_type, 128),
        (amqp_ulong_type, 0xffffffffffffffff),

        (amqp_byte_type, 127),
        (amqp_byte_type, -128),
        (amqp_short_type, -32768),
        (amqp_short_type, 32767),
        (amqp_int_type, -2147483648),
        (amqp_int_type, 0),
        (amqp_int_type, 2147483647),
        (amqp_long_type, -9223372036854775808),
        (amqp_long_type, 9223372036854775807),

        (amqp_float_type, 1.0),
        (amqp_float_type, -1.0),
        (amqp_double_type, 1.0),
        (amqp_double_type, -1.0),

        # XXX Fails to decode to a single char on micropython
        # (amqp_char_type, "a"),

        (amqp_timestamp_type, round(time.time(), 3)),
        (amqp_uuid_type, _uuid_bytes()),

        (amqp_binary_type, b"123"),
        (amqp_binary_type, b"x" * 256),
        (amqp_string_type, "Hello, \U0001F34B!"),
        (amqp_string_type, "\U0001F34B" * 256),
        (amqp_symbol_type, "hello"),
        (amqp_symbol_type, "x" * 256),

        (amqp_list_type, [None, 0, 1, "abc"]),
        (amqp_list_type, [0, 1, ["a", "b", "c"]]),
        (amqp_list_type, [0, 1, {"a": 0, "b": 1}]),
        (amqp_map_type, {None: 0, "a": 1, "b": 2}),
        (amqp_map_type, {"a": 0, "b": {0: "x", 1: "y"}}),
        (amqp_map_type, {"a": 0, "b": [0, 1, {"a": 0, "b": 1}]}),

        (AmqpArrayType(amqp_null_type), [None, None, None]),
        (AmqpArrayType(amqp_ubyte_type), [0, 1, 2]),
        (AmqpArrayType(amqp_ushort_type), [0, 1, 2]),
        (AmqpArrayType(amqp_uint_type), [0, 1, 2]),
        (AmqpArrayType(amqp_long_type), [0, 1, 2]),
        (AmqpArrayType(amqp_float_type), [0.0, 1.5, 3.0]),

        (AmqpArrayType(amqp_double_type), [0.0, 1.5, 3.0]),

        (AmqpArrayType(amqp_timestamp_type), [0.0, round(time.time(), 3), -1.0]),
        (AmqpArrayType(amqp_uuid_type), [_uuid_bytes(), _uuid_bytes(), _uuid_bytes()]),

        (AmqpArrayType(amqp_list_type), [[0, 1, "abc"], [0, 1, "abc"], [0, 1, "abc"]]),
        (AmqpArrayType(amqp_map_type), [{"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}]),
        (AmqpArrayType(AmqpArrayType(amqp_boolean_type)), [[True, False], [True, False], [True, False]]),
    ]

    debug = False

    buff = _Buffer()
    offset = 0
    output_hexes = list()

    for i in range(1):
        for type_, input_value in data:
            if debug:
                print("Emitting {} {}".format(type_, input_value))

            start = offset

            if type(type_) is tuple:
                type_, element_type = type_
                offset = type_.emit(buff, offset, input_value)
            else:
                offset = type_.emit(buff, offset, input_value)

            hex_ = _hex(buff[start:offset])
            output_hexes.append(hex_)

            if debug:
                print("Emitted {}".format(hex_))

        offset = 0
        output_values = list()

        for type_, input_value in data:
            if debug:
                lookahead = _hex(buff[offset:offset + 10])
                print("Parsing {}... for {} {}".format(lookahead, type_, input_value))

            start = offset
            offset, value = parse_data(buff, offset)

            if debug:
                print("Parsed {}".format(_hex(buff[start:offset])))

            assert value == input_value, "Expected {} but got {}".format(input_value, value)

            output_values.append(value)

    row = "{:4} {:22} {:>22} {:>22} {}"

    for i, item in enumerate(data):
        type_, input_value = item
        output_value = output_values[i]
        output_hex = output_hexes[i]

        print(row.format(i, repr(type_), _shorten(str(input_value)), _shorten(str(output_value)), output_hex))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

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
    def __init__(self, name, python_type, format_code, descriptor_type=None):
        assert name is not None
        assert python_type is not None
        assert format_code is not None

        self.name = name
        self.python_type = python_type
        self.format_code = format_code
        self.descriptor_type = descriptor_type

    def __repr__(self):
        return self.name

    def emit(self, buff, offset, value, element_type=None):
        descriptor = None

        if self.descriptor_type is not None:
            descriptor, value = value

        assert isinstance(value, self.python_type)

        offset, format_code_offset = self.emit_constructor(buff, offset, descriptor)
        offset, format_code = self.emit_value(buff, offset, value, element_type=element_type)

        buff.pack(format_code_offset, 1, "!B", format_code)

        return offset

    def emit_constructor(self, buff, offset, descriptor):
        if self.descriptor_type is not None:
            offset = buff.pack(offset, 1, "!B", 0x00)
            offset = self.descriptor_type.emit(buff, offset, descriptor)

        format_code_offset = offset # The format code is filled in later
        offset += 1

        return offset, format_code_offset

class _AmqpNull(_AmqpDataType):
    def __init__(self, descriptor_type=None):
        super().__init__("null", type(None), 0x40, descriptor_type=descriptor_type)

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return offset, None

class _AmqpBoolean(_AmqpDataType):
    def __init__(self):
        super().__init__("boolean", bool, 0x56)

    def emit(self, buff, offset, value, element_type=None):
        if value is True: return buff.pack(offset, 1, "!B", 0x41)
        if value is False: return buff.pack(offset, 1, "!B", 0x42)

        raise Exception()

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.format_code)

        return buff.pack(offset, 1, "!?", value), self.format_code

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x41: return offset, True
        if format_code == 0x42: return offset, False

        return buff.unpack(offset, 1, "!?")

class _AmqpFixedWidthType(_AmqpDataType):
    def __init__(self, name, python_type, format_code, format_string):
        super().__init__(name, python_type, format_code)

        self.format_string = format_string
        self.format_size = _struct.calcsize(self.format_string)

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.format_code)

        offset = buff.pack(offset, self.format_size, self.format_string, value)

        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return buff.unpack(offset, self.format_size, self.format_string)

class _AmqpUnsignedInt(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("uint", int, 0x70, "!I")

    def emit(self, buff, offset, value, element_type=None):
        if value == 0: return buff.pack(offset, 1, "!B", 0x43)
        if value < 256: return buff.pack(offset, 2, "!BB", 0x52, value)

        return super().emit(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x43: return offset, 0
        if format_code == 0x52: return buff.unpack(offset, 1, "!B")

        return super().parse_value(buff, offset, format_code)

class _AmqpUnsignedLong(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("ulong", int, 0x80, "!Q")

    def emit(self, buff, offset, value, element_type=None):
        if value == 0: return buff.pack(offset, 1, "!B", 0x44)
        if value < 256: return buff.pack(offset, 2, "!BB", 0x53, value)

        return super().emit(buff, offset, value, element_type)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x44: return offset, 0
        if format_code == 0x53: return buff.unpack(offset, 1, "!B")

        return super().parse_value(buff, offset, format_code)

class _AmqpInt(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("int", int, 0x71, "!i")

    def emit(self, buff, offset, value, element_type=None):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 2, "!Bb", 0x54, value)

        return super().emit(buff, offset, value, element_type)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x54: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class _AmqpLong(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("long", int, 0x81, "!q")

    def emit(self, buff, offset, value, element_type=None):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 2, "!Bb", 0x55, value)

        return super().emit(buff, offset, value, element_type)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x55: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class _AmqpChar(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("char", str, 0x73, "!4s")

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        value = value.encode("utf-32-be")
        return super().emit_value(buff, offset, value, format_code, element_type)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, value.decode("utf-32-be")

class _AmqpTimestamp(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("timestamp", float, 0x83, "!q")

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        value = int(round(value * 1000))
        return super().emit_value(buff, offset, value, format_code)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, round(value / 1000, 3)

class _AmqpVariableWidthType(_AmqpDataType):
    def __init__(self, name, python_type, short_format_code, long_format_code):
        super().__init__(name, python_type, long_format_code)

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        return value

    def decode(self, octets):
        return bytes(octets) # XXX memoryview

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.short_format_code, self.long_format_code)

        octets = self.encode(value)
        size = len(octets)

        if format_code is None:
            if size < 256:
                format_code = self.short_format_code
            else:
                format_code = self.long_format_code

        if format_code == self.short_format_code:
            assert size < 256
            offset = buff.pack(offset, 1, "!B", size)
        else:
            offset = buff.pack(offset, 4, "!I", size)

        end = offset + size
        buff[offset:end] = octets

        return end, format_code

    def parse_value(self, buff, offset, format_code):
        assert format_code in (self.short_format_code, self.long_format_code)

        if format_code == self.short_format_code:
            offset, size = buff.unpack(offset, 1, "!B")
        else:
            offset, size = buff.unpack(offset, 4, "!I")

        end = offset + size
        value = self.decode(buff[offset:end]) # XXX memoryview

        return end, value

class _AmqpBinary(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("binary", bytes, 0xa0, 0xb0)

class _AmqpString(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("string", str, 0xa1, 0xb1)

    def encode(self, value):
        return value.encode("utf-8")

    def decode(self, octets):
        return bytes(octets).decode("utf-8")

class _AmqpSymbol(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("symbol", str, 0xa3, 0xb3)

    def encode(self, value):
        return value.encode("ascii")

    def decode(self, octets):
        return bytes(octets).decode("ascii")

class _AmqpCollection(_AmqpDataType):
    def __init__(self, name, python_type, short_format_code, long_format_code):
        super().__init__(name, python_type, long_format_code)

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def emit_size_and_count(self, buff, offset, size, count, format_code=None):
        if format_code is None:
            if size < 256 and count < 256:
                format_code = self.short_format_code
            else:
                format_code = self.long_format_code

        if format_code == self.short_format_code:
            assert size < 256 and count < 256

            offset = buff.pack(offset, 2, "!BB", size, count)
        else:
            offset = buff.pack(offset, 8, "!II", size, count)

        return offset, format_code

    def parse_size_and_count(self, buff, offset, format_code):
        if format_code == self.short_format_code:
            return buff.unpack(offset, 2, "!BB")

        if format_code == self.long_format_code:
            return buff.unpack(offset, 8, "!II")

        raise Exception()

class _AmqpCompoundType(_AmqpCollection):
    def __init__(self, name, python_type, short_format_code, long_format_code):
        super().__init__(name, python_type, short_format_code, long_format_code)

    def encode(self, value):
        buff = _Buffer()
        offset = 0

        offset = self.encode_into(buff, offset, value)

        return buff[:offset], offset, len(value)

    def encode_into(self, buff, offset, value):
        for item in value:
            offset = emit_value(buff, offset, item)

        return offset

    def decode_from(self, buff, offset, size, count):
        value = [None] * count

        for i in range(count):
            offset, value[i] = parse_data(buff, offset)

        return offset, value

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.short_format_code, self.long_format_code)

        octets, size, count = self.encode(value)
        offset, format_code = self.emit_size_and_count(buff, offset, size, count, format_code)

        end = offset + size
        buff[offset:end] = octets

        return end, format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)
        offset, value = self.decode_from(buff, offset, size, count)

        return offset, value

class _AmqpList(_AmqpCompoundType):
    def __init__(self):
        super().__init__("list", list, 0xc0, 0xd0)

class _AmqpMap(_AmqpCompoundType):
    def __init__(self):
        super().__init__("map", dict, 0xc1, 0xd1)

    def encode(self, value):
        elems = list()

        for item in value.items():
            elems.extend(item)

        return super().encode(elems)

    def decode_from(self, buff, offset, size, count):
        offset, elems = super().decode_from(buff, offset, size, count)
        pairs = dict()

        for i in range(0, len(elems), 2):
            pairs[elems[i]] = elems[i + 1]

        return offset, pairs

class _AmqpArray(_AmqpCollection):
    def __init__(self):
        super().__init__("array", list, 0xf0, 0xe0)

    def encode(self, value, element_type):
        buff = _Buffer()
        offset = 0

        for elem in value:
            offset, format_code = element_type.emit_value(buff, offset, elem, element_type.format_code)

        return buff[:offset], offset, len(value)

    def decode_from(self, buff, offset, size, count, element_type):
        value = [None] * count

        for i in range(count):
            offset, value[i] = element_type.parse_value(buff, offset, element_type.format_code)

        return offset, value

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.short_format_code, self.long_format_code)
        assert element_type is not None
        assert element_type is not amqp_array # Not supported

        descriptor = None

        if self.descriptor_type is not None:
            descriptor, value = value

        octets, size, count = self.encode(value, element_type)
        offset, format_code = self.emit_size_and_count(buff, offset, size, count, format_code)

        offset, element_format_code_offset = element_type.emit_constructor(buff, offset, descriptor)
        buff.pack(element_format_code_offset, 1, "!B", element_type.format_code)

        end = offset + size
        buff[offset:end] = octets

        return end, format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)

        offset, element_format_code = buff.unpack(offset, 1, "!B")

        element_type = _get_data_type_for_format_code(element_format_code)

        offset, value = self.decode_from(buff, offset, size, count, element_type)

        return offset, value

amqp_null = _AmqpNull()
amqp_boolean = _AmqpBoolean()

amqp_ubyte = _AmqpFixedWidthType("ubyte", int, 0x50, "!B")
amqp_ushort = _AmqpFixedWidthType("ushort", int, 0x60, "!H")
amqp_uint = _AmqpUnsignedInt()
amqp_ulong = _AmqpUnsignedLong()

amqp_byte = _AmqpFixedWidthType("byte", int, 0x51, "!b")
amqp_short = _AmqpFixedWidthType("short", int, 0x61, "!h")
amqp_int = _AmqpInt()
amqp_long = _AmqpLong()

amqp_float =_AmqpFixedWidthType("float", float, 0x72, "!f")
amqp_double = _AmqpFixedWidthType("double", float, 0x82, "!d")

amqp_char = _AmqpChar()
amqp_timestamp = _AmqpTimestamp()
amqp_uuid = _AmqpFixedWidthType("uuid", bytes, 0x98, "!16s")

amqp_binary = _AmqpBinary()
amqp_string = _AmqpString()
amqp_symbol = _AmqpSymbol()

amqp_list = _AmqpList()
amqp_map = _AmqpMap()
amqp_array = _AmqpArray()

_data_types_by_format_code = {
    0x40: amqp_null,
    0x41: amqp_boolean,
    0x42: amqp_boolean,
    0x43: amqp_uint,
    0x44: amqp_ulong,
    0x50: amqp_ubyte,
    0x51: amqp_byte,
    0x52: amqp_uint,
    0x53: amqp_ulong,
    0x54: amqp_int,
    0x55: amqp_long,
    0x56: amqp_boolean,
    0x60: amqp_ushort,
    0x61: amqp_short,
    0x70: amqp_uint,
    0x71: amqp_int,
    0x72: amqp_float,
    0x73: amqp_char,
    0x80: amqp_ulong,
    0x81: amqp_long,
    0x82: amqp_double,
    0x83: amqp_timestamp,
    0x98: amqp_uuid,
    0xa0: amqp_binary,
    0xa1: amqp_string,
    0xa3: amqp_symbol,
    0xb0: amqp_binary,
    0xb1: amqp_string,
    0xb3: amqp_symbol,
    0xc0: amqp_list,
    0xc1: amqp_map,
    0xd0: amqp_array,
    0xd0: amqp_list,
    0xd1: amqp_map,
    0xf0: amqp_array,
}

def _get_data_type_for_format_code(format_code):
    try:
        return _data_types_by_format_code[format_code]
    except KeyError:
        raise Exception("No data type for format code 0x{:02X}".format(format_code))

def _get_data_type_for_python_type(python_type):
    if issubclass(python_type, int):
        return amqp_long

    if issubclass(python_type, float):
        return amqp_double

    if issubclass(python_type, bytes):
        return amqp_binary

    if issubclass(python_type, str):
        return amqp_string

    if issubclass(python_type, list):
        return amqp_list

    if issubclass(python_type, dict):
        return amqp_map

    if python_type is type(None):
        return amqp_null

    raise Exception("No data type for Python type {}".format(python_type))

def get_data_type(value):
    return _get_data_type_for_python_type(type(value))

def emit_value(buff, offset, value):
    data_type = get_data_type(value)
    return data_type.emit(buff, offset, value)

def parse_data(buff, offset):
    offset, format_code, descriptor = _parse_constructor(buff, offset)
    data_type = _get_data_type_for_format_code(format_code)

    offset, value = data_type.parse_value(buff, offset, format_code)

    if descriptor is not None:
        value = (descriptor, value)

    return offset, value

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
        (amqp_null, None),
        (_AmqpNull(amqp_symbol), ("a", None)),

        (amqp_boolean, True),
        (amqp_boolean, False),

        (amqp_ubyte, 0),
        (amqp_ubyte, 0xff),
        (amqp_ushort, 0),
        (amqp_ushort, 0xffff),
        (amqp_uint, 0),
        (amqp_uint, 128),
        (amqp_uint, 0xffffffff),
        (amqp_ulong, 0),
        (amqp_ulong, 128),
        (amqp_ulong, 0xffffffffffffffff),

        (amqp_byte, 127),
        (amqp_byte, -128),
        (amqp_short, -32768),
        (amqp_short, 32767),
        (amqp_int, -2147483648),
        (amqp_int, 0),
        (amqp_int, 2147483647),
        (amqp_long, -9223372036854775808),
        (amqp_long, 9223372036854775807),

        (amqp_float, 1.0),
        (amqp_float, -1.0),
        (amqp_double, 1.0),
        (amqp_double, -1.0),

        # XXX Fails to decode to a single char on micropython
        # (amqp_char, "a"),

        (amqp_timestamp, round(time.time(), 3)),
        (amqp_uuid, _uuid_bytes()),

        (amqp_binary, b"123"),
        (amqp_binary, b"x" * 256),
        (amqp_string, "Hello, \U0001F34B!"),
        (amqp_string, "\U0001F34B" * 256),
        (amqp_symbol, "hello"),
        (amqp_symbol, "x" * 256),

        (amqp_list, [None, 0, 1, "abc"]),
        (amqp_list, [0, 1, ["a", "b", "c"]]),
        (amqp_list, [0, 1, {"a": 0, "b": 1}]),
        (amqp_map, {None: 0, "a": 1, "b": 2}),
        (amqp_map, {"a": 0, "b": {0: "x", 1: "y"}}),
        (amqp_map, {"a": 0, "b": [0, 1, {"a": 0, "b": 1}]}),

        ((amqp_array, amqp_null), [None, None, None]),
        ((amqp_array, amqp_ubyte), [0, 1, 2]),
        ((amqp_array, amqp_ushort), [0, 1, 2]),
        ((amqp_array, amqp_uint), [0, 1, 2]),
        ((amqp_array, amqp_long), [0, 1, 2]),
        ((amqp_array, amqp_float), [0.0, 1.5, 3.0]),

        ((amqp_array, amqp_double), [0.0, 1.5, 3.0]),

        ((amqp_array, amqp_timestamp), [0.0, round(time.time(), 3), -1.0]),
        ((amqp_array, amqp_uuid), [_uuid_bytes(), _uuid_bytes(), _uuid_bytes()]),

        ((amqp_array, amqp_list), [[0, 1, "abc"], [0, 1, "abc"], [0, 1, "abc"]]),
        ((amqp_array, amqp_map), [{"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}]),
    ]

    debug = True

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
                offset = type_.emit(buff, offset, input_value, element_type=element_type)
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

    row = "{:4} {:18} {:>22} {:>22} {}"

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

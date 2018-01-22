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

from argon.common import _struct, _hex, _namedtuple, _uuid_bytes

class _DataType:
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

class NullType(_DataType):
    def __init__(self, descriptor=None):
        super().__init__("null", type(None), 0x40, descriptor=descriptor)

    def emit_value_long(self, buff, offset, value):
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return offset, None

class BooleanType(_DataType):
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

class _FixedWidthType(_DataType):
    def __init__(self, name, python_type, format_code, format_string, descriptor=None):
        super().__init__(name, python_type, format_code, descriptor=descriptor)

        self.format_string = format_string
        self.format_size = _struct.calcsize(self.format_string)

    def emit_value_long(self, buff, offset, value):
        offset = buff.pack(offset, self.format_size, self.format_string, value)
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return buff.unpack(offset, self.format_size, self.format_string)

class UnsignedByteType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("ubyte", int, 0x50, "!B", descriptor=descriptor)

class UnsignedShortType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("ushort", int, 0x60, "!H", descriptor=descriptor)

class UnsignedIntType(_FixedWidthType):
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

class UnsignedLongType(_FixedWidthType):
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

class ByteType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("byte", int, 0x51, "!b", descriptor=descriptor)

class ShortType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("short", int, 0x61, "!h", descriptor=descriptor)

class IntType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("int", int, 0x71, "!i", descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 1, "!b", value), 0x54

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x54: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class LongType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("long", int, 0x81, "!q", descriptor=descriptor)

    def emit_value(self, buff, offset, value):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 1, "!b", value), 0x55

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x55: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class FloatType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("float", float, 0x72, "!f", descriptor=descriptor)

class DoubleType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("double", float, 0x82, "!d", descriptor=descriptor)

class CharType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("char", str, 0x73, "!4s", descriptor=descriptor)

    def emit_value_long(self, buff, offset, value):
        value = value.encode("utf-32-be")
        return super().emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, value.decode("utf-32-be")

class UuidType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("uuid", bytes, 0x98, "!16s", descriptor=descriptor)

class TimestampType(_FixedWidthType):
    def __init__(self, descriptor=None):
        super().__init__("timestamp", float, 0x83, "!q", descriptor=descriptor)

    def emit_value_long(self, buff, offset, value):
        value = int(round(value * 1000))
        return super().emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, round(value / 1000, 3)

class _VariableWidthType(_DataType):
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

class BinaryType(_VariableWidthType):
    def __init__(self, descriptor=None):
        super().__init__("binary", bytes, 0xa0, 0xb0, descriptor=descriptor)

class StringType(_VariableWidthType):
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

class SymbolType(_VariableWidthType):
    def __init__(self, descriptor=None):
        super().__init__("symbol", str, 0xa3, 0xb3, descriptor=descriptor)

    def encode(self, value):
        return value.encode("ascii")

    def decode(self, octets):
        return bytes(octets).decode("ascii")

class _CollectionType(_DataType):
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

class _CompoundType(_CollectionType):
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

class ListType(_CompoundType):
    def __init__(self, descriptor=None):
        super().__init__("list", list, 0xc0, 0xd0, descriptor=descriptor)

class MapType(_CompoundType):
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

class ArrayType(_CollectionType):
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

_null_type = NullType()
_boolean_type = BooleanType()

_ubyte_type = UnsignedByteType()
_ushort_type = UnsignedShortType()
_uint_type = UnsignedIntType()
_ulong_type = UnsignedLongType()

_byte_type = ByteType()
_short_type = ShortType()
_int_type = IntType()
_long_type = LongType()

_float_type = FloatType()
_double_type = DoubleType()

_char_type = CharType()
_timestamp_type = TimestampType()
_uuid_type = UuidType()

_binary_type = BinaryType()
_string_type = StringType()
_symbol_type = SymbolType()

_list_type = ListType()
_map_type = MapType()
_array_type = ArrayType(None)

_data_types_by_format_code = {
    0x40: _null_type,
    0x41: _boolean_type,
    0x42: _boolean_type,
    0x43: _uint_type,
    0x44: _ulong_type,
    0x50: _ubyte_type,
    0x51: _byte_type,
    0x52: _uint_type,
    0x53: _ulong_type,
    0x54: _int_type,
    0x55: _long_type,
    0x56: _boolean_type,
    0x60: _ushort_type,
    0x61: _short_type,
    0x70: _uint_type,
    0x71: _int_type,
    0x72: _float_type,
    0x73: _char_type,
    0x80: _ulong_type,
    0x81: _long_type,
    0x82: _double_type,
    0x83: _timestamp_type,
    0x98: _uuid_type,
    0xa0: _binary_type,
    0xa1: _string_type,
    0xa3: _symbol_type,
    0xb0: _binary_type,
    0xb1: _string_type,
    0xb3: _symbol_type,
    0xc0: _list_type,
    0xc1: _map_type,
    0xd0: _list_type,
    0xd1: _map_type,
    0xe0: _array_type,
    0xf0: _array_type,
}

def _get_data_type_for_format_code(format_code):
    try:
        return _data_types_by_format_code[format_code]
    except KeyError:
        raise Exception("No data type for format code 0x{:02X}".format(format_code))

class UnsignedByte(int):
    __argon_type = _ubyte_type

class UnsignedShort(int):
    __argon_type = _ushort_type

class UnsignedInt(int):
    __argon_type = _uint_type

class UnsignedLong(int):
    __argon_type = _ulong_type

class Byte(int):
    __argon_type = _byte_type

class Short(int):
    __argon_type = _short_type

class Int(int):
    __argon_type = _int_type

class Float(float):
    __argon_type = _float_type

class Symbol(str):
    __argon_type = _symbol_type

def _get_data_type_for_python_type(python_type):
    if python_type is type(None):
        return _null_type

    if issubclass(python_type, int):
        return _long_type

    if issubclass(python_type, float):
        return _double_type

    if issubclass(python_type, bytes):
        return _binary_type

    if issubclass(python_type, str):
        return _string_type

    if issubclass(python_type, list):
        return _list_type

    if issubclass(python_type, dict):
        return _map_type

    if hasattr(python_type, "__argon_type"):
        return python_type.__argon_type

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

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

from argon.common import *
from argon.common import _hex, _struct

class UnsignedByte(int): pass
class UnsignedShort(int): pass
class UnsignedInt(int): pass
class UnsignedLong(int): pass
class Byte(int): pass
class Short(int): pass
class Int(int): pass
class Float(float): pass
class Decimal32(bytes): pass
class Decimal64(bytes): pass
class Decimal128(bytes): pass
class Char(str): pass
class Timestamp(float): pass
class Uuid(bytes): pass
class Symbol(str): pass

class Array:
    def __init__(self, element_type, elements, element_descriptor=None):
        self.element_type = element_type
        self.element_descriptor = element_descriptor
        self.elements = elements

    def __eq__(self, other):
        return self.elements == other.elements

    def __repr__(self):
        return "{}({}, {})".format(self.__class__.__name__, self.element_type, self.elements)

class DescribedValue:
    def __init__(self, descriptor, value):
        self.descriptor = descriptor
        self.value = value

    def __repr__(self):
        return "{}:{}".format(self.descriptor, self.value)

    def __eq__(self, other):
        return self.descriptor == other.descriptor and self.value == other.value

class _DataType:
    def __init__(self, python_type, format_code):
        assert python_type is not None
        assert format_code is not None

        self.python_type = python_type
        self.format_code = format_code

    def __repr__(self):
        return self.python_type.__name__

    def emit(self, buff, offset, value):
        descriptor = None

        if isinstance(value, DescribedValue):
            descriptor = value.descriptor
            value = value.value

        print(111, value, self.python_type)
        assert isinstance(value, self.python_type)

        offset, format_code_offset = self.emit_constructor(buff, offset, descriptor)
        offset, format_code = self.emit_value(buff, offset, value)

        buff.pack(format_code_offset, 1, "!B", format_code)

        return offset

    def emit_constructor(self, buff, offset, descriptor):
        if descriptor is not None:
            offset = buff.pack(offset, 1, "!B", 0x00)
            offset = emit_data(buff, offset, descriptor)

        # The format code is filled in after the value is emitted
        offset, format_code_offset = buff.skip(offset, 1)

        return offset, format_code_offset

    def emit_value(self, buff, offset, value):
        return self.emit_value_long(buff, offset, value)

    def emit_value_long(self, buff, offset, value):
        raise NotImplementedError()

class _NullType(_DataType):
    def __init__(self):
        super().__init__(type(None), 0x40)

    def emit_value_long(self, buff, offset, value):
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return offset, None

class _BooleanType(_DataType):
    def __init__(self):
        super().__init__(bool, 0x56)

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
    def __init__(self, python_type, format_code, format_string):
        super().__init__(python_type, format_code)

        self.format_string = format_string
        self.format_size = _struct.calcsize(self.format_string)

    def emit_value_long(self, buff, offset, value):
        offset = buff.pack(offset, self.format_size, self.format_string, value)
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return buff.unpack(offset, self.format_size, self.format_string)

class _UnsignedByteType(_FixedWidthType):
    def __init__(self):
        super().__init__(UnsignedByte, 0x50, "!B")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, UnsignedByte(value)

class _UnsignedShortType(_FixedWidthType):
    def __init__(self):
        super().__init__(UnsignedShort, 0x60, "!H")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, UnsignedShort(value)

class _UnsignedIntType(_FixedWidthType):
    def __init__(self):
        super().__init__(UnsignedInt, 0x70, "!I")

    def emit_value(self, buff, offset, value):
        if value == 0: return offset, 0x43
        if value < 256: return buff.pack(offset, 1, "!B", value), 0x52

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x43:
            value = 0
        elif format_code == 0x52:
            offset, value = buff.unpack(offset, 1, "!B")
        else:
            offset, value = super().parse_value(buff, offset, format_code)

        return offset, UnsignedInt(value)

class _UnsignedLongType(_FixedWidthType):
    def __init__(self):
        super().__init__(UnsignedLong, 0x80, "!Q")

    def emit_value(self, buff, offset, value):
        if value == 0: return offset, 0x44
        if value < 256: return buff.pack(offset, 1, "!B", value), 0x53

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x44:
            value = 0
        elif format_code == 0x53:
            offset, value = buff.unpack(offset, 1, "!B")
        else:
            offset, value = super().parse_value(buff, offset, format_code)

        return offset, UnsignedLong(value)

class _ByteType(_FixedWidthType):
    def __init__(self):
        super().__init__(Byte, 0x51, "!b")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Byte(value)

class _ShortType(_FixedWidthType):
    def __init__(self):
        super().__init__(Short, 0x61, "!h")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Short(value)

class _IntType(_FixedWidthType):
    def __init__(self):
        super().__init__(Int, 0x71, "!i")

    def emit_value(self, buff, offset, value):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 1, "!b", value), 0x54

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x54:
            offset, value = buff.unpack(offset, 1, "!b")
        else:
            offset, value = super().parse_value(buff, offset, format_code)

        return offset, Int(value)

class _LongType(_FixedWidthType):
    def __init__(self):
        super().__init__(int, 0x81, "!q")

    def emit_value(self, buff, offset, value):
        if value >= -128 and value <= 127:
            return buff.pack(offset, 1, "!b", value), 0x55

        return self.emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x55: return buff.unpack(offset, 1, "!b")

        return super().parse_value(buff, offset, format_code)

class _FloatType(_FixedWidthType):
    def __init__(self):
        super().__init__(Float, 0x72, "!f")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Float(value)

class _DoubleType(_FixedWidthType):
    def __init__(self):
        super().__init__(float, 0x82, "!d")

class _Decimal32Type(_FixedWidthType):
    def __init__(self):
        super().__init__(Decimal32, 0x74, "!4s")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Decimal32(value)

class _Decimal64Type(_FixedWidthType):
    def __init__(self):
        super().__init__(Decimal64, 0x84, "!8s")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Decimal64(value)

class _Decimal128Type(_FixedWidthType):
    def __init__(self):
        super().__init__(Decimal128, 0x94, "!16s")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Decimal128(value)

class _CharType(_FixedWidthType):
    def __init__(self):
        super().__init__(Char, 0x73, "!4s")

    def emit_value_long(self, buff, offset, value):
        value = value.encode("utf-32-be")
        return super().emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Char(value.decode("utf-32-be"))

class _UuidType(_FixedWidthType):
    def __init__(self):
        super().__init__(Uuid, 0x98, "!16s")

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Uuid(value)

class _TimestampType(_FixedWidthType):
    def __init__(self):
        super().__init__(Timestamp, 0x83, "!q")

    def emit_value_long(self, buff, offset, value):
        value = int(round(value * 1000))
        return super().emit_value_long(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        offset, value = super().parse_value(buff, offset, format_code)
        return offset, Timestamp(round(value / 1000, 3))

class _VariableWidthType(_DataType):
    def __init__(self, python_type, short_format_code, long_format_code):
        super().__init__(python_type, long_format_code)

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        raise NotImplementedError()

    def decode(self, octets):
        raise NotImplementedError()

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

class _BinaryType(_VariableWidthType):
    def __init__(self):
        super().__init__(bytes, 0xa0, 0xb0)

    def encode(self, value):
        return value

    def decode(self, octets):
        return bytes(octets)

class _StringType(_VariableWidthType):
    def __init__(self):
        super().__init__(str, 0xa1, 0xb1)

    def encode(self, value):
        return value.encode("utf-8")

    def decode(self, octets):
        return bytes(octets).decode("utf-8")

    def emit_value(self, buff, offset, value):
        if len(value) < 64:
            return self.emit_value_short(buff, offset, value)

        return self.emit_value_long(buff, offset, value)

class _SymbolType(_VariableWidthType):
    def __init__(self):
        super().__init__(Symbol, 0xa3, 0xb3)

    def encode(self, value):
        return value.encode("ascii")

    def decode(self, octets):
        return Symbol(bytes(octets).decode("ascii"))

class _CollectionType(_DataType):
    def __init__(self, python_type, short_format_code, long_format_code):
        super().__init__(python_type, long_format_code)

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
    def __init__(self, python_type, short_format_code, long_format_code):
        super().__init__(python_type, short_format_code, long_format_code)

    def emit_value(self, buff, offset, value):
        offset, size_offset = buff.skip(offset, 1)
        offset, count_offset = buff.skip(offset, 1)

        value_offset = offset
        offset = self.encode_into(buff, offset, value)

        size = offset - count_offset
        count = self.get_count(value)

        if size >= 256 or count >= 256:
            encoded_value = bytes(buff[value_offset:offset])
            return self.emit_value_long(buff, size_offset, value, encoded_value)

        buff.pack(size_offset, 2, "!BB", size, count)

        return offset, self.short_format_code

    def emit_value_long(self, buff, offset, value, encoded_value=None):
        offset, size_offset = buff.skip(offset, 4)
        offset, count_offset = buff.skip(offset, 4)

        if encoded_value is None:
            offset = self.encode_into(buff, offset, value)
        else:
            offset = buff.write(offset, encoded_value)

        size = offset - count_offset
        count = self.get_count(value)

        buff.pack(size_offset, 8, "!II", size, count)

        return offset, self.long_format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)
        offset, value = self.decode_from(buff, offset, count)

        return offset, value

class _ListType(_CompoundType):
    def __init__(self):
        super().__init__(list, 0xc0, 0xd0)

    def get_count(self, value):
        return len(value)

    def encode_into(self, buff, offset, value):
        for item in value:
            offset = emit_data(buff, offset, item)

        return offset

    def decode_from(self, buff, offset, count):
        # assert count < 1000, count # XXX This is incorrect, but it catches some codec bugs

        value = [None] * count

        for i in range(count):
            offset, value[i] = parse_data(buff, offset)

        return offset, value

    def emit_value(self, buff, offset, value):
        if len(value) == 0: return offset, 0x45

        return super().emit_value(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x45:
            value = list()
        else:
            offset, value = super().parse_value(buff, offset, format_code)

        return offset, value

class _MapType(_CompoundType):
    def __init__(self):
        super().__init__(dict, 0xc1, 0xd1)

    def get_count(self, value):
        return len(value) * 2

    def encode_into(self, buff, offset, value):
        for item_key, item_value in value.items():
            offset = emit_data(buff, offset, item_key)
            offset = emit_data(buff, offset, item_value)

        return offset

    def decode_from(self, buff, offset, count):
        items = dict()

        for i in range(0, count, 2):
            offset, item_key = parse_data(buff, offset)
            offset, item_value = parse_data(buff, offset)

            items[item_key] = item_value

        return offset, items

class _ArrayType(_CollectionType):
    def __init__(self):
        super().__init__(Array, 0xf0, 0xe0)

    def encode_into(self, buff, offset, value):
        elem_type = _get_data_type_for_python_type(value.element_type)

        for elem in value.elements:
            offset, format_code = elem_type.emit_value_long(buff, offset, elem)

        return offset

    def decode_from(self, buff, offset, count, elem_type, elem_format_code, elem_descriptor):
        elems = [None] * count

        for i in range(count):
            offset, elems[i] = elem_type.parse_value(buff, offset, elem_format_code)

        return offset, Array(elem_type, elems, elem_descriptor)

    def emit_elem_constructor(self, buff, offset, value):
        elem_type = _get_data_type_for_python_type(value.element_type)
        elem_descriptor = value.element_descriptor

        offset, elem_format_code_offset = elem_type.emit_constructor(buff, offset, elem_descriptor)
        buff.pack(elem_format_code_offset, 1, "!B", elem_type.format_code)

        return offset

    def emit_value(self, buff, offset, value):
        offset, size_offset = buff.skip(offset, 1)
        offset, count_offset = buff.skip(offset, 1)

        offset = self.emit_elem_constructor(buff, offset, value)

        value_offset = offset
        offset = self.encode_into(buff, offset, value)

        size = offset - count_offset
        count = len(value.elements)

        if size >= 256 or count >= 256:
            encoded_value = bytes(buff[value_offset:offset])
            return self.emit_value_long(buff, size_offset, value, encoded_value)

        buff.pack(size_offset, 2, "!BB", size, count)

        return offset, self.short_format_code

    def emit_value_long(self, buff, offset, value, encoded_value=None):
        offset, size_offset = buff.skip(offset, 4)
        offset, count_offset = buff.skip(offset, 4)

        offset = self.emit_elem_constructor(buff, offset, value)

        if encoded_value is None:
            offset = self.encode_into(buff, offset, value)
        else:
            offset = buff.write(offset, encoded_value)

        size = offset - count_offset
        count = len(value.elements)

        buff.pack(size_offset, 8, "!II", size, count)

        return offset, self.long_format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)
        offset, elem_format_code, elem_descriptor = _parse_constructor(buff, offset)

        elem_type = _get_data_type_for_format_code(elem_format_code)

        offset, value = self.decode_from \
            (buff, offset, count, elem_type, elem_format_code, elem_descriptor)

        return offset, value

_null_type = _NullType()
_boolean_type = _BooleanType()
_ubyte_type = UnsignedByte._data_type = _UnsignedByteType()
_ushort_type = UnsignedShort._data_type = _UnsignedShortType()
_uint_type = UnsignedInt._data_type = _UnsignedIntType()
_ulong_type = UnsignedLong._data_type = _UnsignedLongType()
_byte_type = Byte._data_type = _ByteType()
_short_type = Short._data_type = _ShortType()
_int_type = Int._data_type = _IntType()
_long_type = _LongType()
_float_type = Float._data_type = _FloatType()
_double_type = _DoubleType()
_decimal32_type = Decimal32._data_type = _Decimal32Type()
_decimal64_type = Decimal64._data_type = _Decimal64Type()
_decimal128_type = Decimal128._data_type = _Decimal128Type()
_char_type = Char._data_type = _CharType()
_timestamp_type = Timestamp._data_type = _TimestampType()
_uuid_type = Uuid._data_type = _UuidType()
_binary_type = _BinaryType()
_string_type = _StringType()
_symbol_type = Symbol._data_type = _SymbolType()
_list_type = _ListType()
_map_type = _MapType()
_array_type = Array._data_type = _ArrayType()

_data_types_by_format_code = {
    0x40: _null_type,
    0x41: _boolean_type,
    0x42: _boolean_type,
    0x43: _uint_type,
    0x44: _ulong_type,
    0x45: _list_type,
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
    0x74: _decimal32_type,
    0x80: _ulong_type,
    0x81: _long_type,
    0x82: _double_type,
    0x83: _timestamp_type,
    0x84: _decimal64_type,
    0x94: _decimal128_type,
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

def _get_data_type_for_python_type(python_type):
    if hasattr(python_type, "_data_type"):
        return python_type._data_type

    if python_type is type(None):
        return _null_type

    if issubclass(python_type, bool):
        return _boolean_type

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

    raise Exception("No data type for Python type {}".format(python_type))

def emit_data(buff, offset, value):
    python_type = type(value)

    if issubclass(python_type, DescribedValue):
        python_type = type(value.value)

    data_type = _get_data_type_for_python_type(python_type)

    return data_type.emit(buff, offset, value)

def parse_data(buff, offset):
    #print("parse_data", _data_hex(buff[offset:offset + 20]), "...")

    offset, format_code, descriptor = _parse_constructor(buff, offset)
    data_type = _get_data_type_for_format_code(format_code)
    offset, value = data_type.parse_value(buff, offset, format_code)

    if descriptor is not None:
        value = DescribedValue(descriptor, value)

    return offset, value

def _parse_constructor(buff, offset):
    offset, format_code = buff.unpack(offset, 1, "!B")
    descriptor = None

    if format_code == 0x00:
        offset, descriptor = parse_data(buff, offset)
        offset, format_code = buff.unpack(offset, 1, "!B")

    return offset, format_code, descriptor

def _data_hex(octets):
    o = _hex(octets)
    return "{} {}".format(o[0:2], o[2:])

import sys as _sys

try:
    import struct as _struct
except ImportError:
    import ustruct as _struct

_micropython = _sys.implementation.name == "micropython"

_data_types_by_format_code = dict()
_data_types_by_python_type = dict()

class _Buffer(bytearray):
    if _micropython:
        def ensure(self, size):
            raise NotImplementedError()
    else:
        def __init__(self):
            super(_Buffer, self).__init__(128)

        def ensure(self, size):
            if len(self) < size:
                self.extend([0] * max(size, len(self)))

    def pack(self, format_string, offset, size, *values):
        self.ensure(offset + size)

        _struct.pack_into(format_string, self, offset, *values)

        return offset + size

class _AmqpDataType:
    def __init__(self, name, python_type, format_code, special_format_codes=()):
        assert name is not None
        assert python_type is not None
        assert format_code is not None

        self.name = name
        self.python_type = python_type
        self.format_code = format_code

        _data_types_by_format_code[self.format_code] = self

        for code in special_format_codes:
            _data_types_by_format_code[code] = self

    def __repr__(self):
        return self.name

    def emit(self, buff, offset, value, element_type=None):
        assert isinstance(value, self.python_type)

        offset, format_code_offset = self.emit_constructor(buff, offset, None)
        offset, format_code = self.emit_value(buff, offset, value, element_type=element_type)

        buff.ensure(offset + 1)
        _struct.pack_into("!B", buff, format_code_offset, format_code)

        return offset

    def emit_constructor(self, buff, offset, descriptor):
        # buff.ensure(offset + 1) XXX
        format_code_offset = offset
        return offset + 1, format_code_offset

class _AmqpNull(_AmqpDataType):
    def __init__(self):
        super().__init__("null", type(None), 0x40)

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        return offset, None

class _AmqpBoolean(_AmqpDataType):
    def __init__(self):
        super().__init__("boolean", bool, 0x56, (0x41, 0x42))

    def emit(self, buff, offset, value, element_type=None):
        if value is True:
            return buff.pack("!B", offset, 1, 0x41)

        if value is False:
            return buff.pack("!B", offset, 1, 0x42)

        raise Exception()

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.format_code)

        if value is True:
            return buff.pack("!B", offset, 1, 0x01), self.format_code

        if value is False:
            return buff.pack("!B", offset, 1, 0x00), self.format_code

        raise Exception()

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x41: return offset, True
        if format_code == 0x42: return offset, False

        assert format_code == self.format_code

        if buff[offset] == 0x01:
            return offset + 1, True

        if buff[offset] == 0x00:
            return offset + 1, True

class _AmqpFixedWidthType(_AmqpDataType):
    def __init__(self, name, python_type, format_code, format_string, special_format_codes=()):
        super().__init__(name, python_type, format_code, special_format_codes)

        self.format_string = format_string
        self.format_size = _struct.calcsize(self.format_string)

    def emit_value(self, buff, offset, value, format_code=None, element_type=None):
        assert format_code in (None, self.format_code)

        offset = buff.pack(self.format_string, offset, self.format_size, value)

        return offset, self.format_code

    def parse_value(self, buff, offset, format_code):
        assert offset + self.format_size <= len(buff)

        value = _struct.unpack_from(self.format_string, buff, offset)[0]
        offset += self.format_size

        return offset, value

class _AmqpUnsignedInt(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("uint", int, 0x70, "!I", (0x43, 0x52))

    def emit(self, buff, offset, value, element_type=None):
        if value == 0:
            return buff.pack("!B", offset, 1, 0x43)

        if value < 256:
            return buff.pack("!BB", offset, 2, 0x52, value)

        return super().emit(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x43:
            return offset, 0

        if format_code == 0x52:
            value = _struct.unpack_from("!B", buff, offset)[0]
            return offset + 1, value

        return super().parse_value(buff, offset, format_code)

class _AmqpUnsignedLong(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("ulong", int, 0x80, "!Q", (0x44, 0x53))

    def emit(self, buff, offset, value, element_type=None):
        if value == 0:
            return buff.pack("!B", offset, 1, 0x44)

        if value < 256:
            return buff.pack("!BB", offset, 2, 0x53, value)

        return super().emit(buff, offset, value, element_type)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x44:
            return offset, 0

        if format_code == 0x53:
            value = _struct.unpack_from("!B", buff, offset)[0]
            return offset + 1, value

        return super().parse_value(buff, offset, format_code)

class _AmqpInt(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("int", int, 0x71, "!i", (0x54,))

    def emit(self, buff, offset, value, element_type=None):
        if value >= -128 and value <= 127:
            return buff.pack("!Bb", offset, 2, 0x54, value)

        return super().emit(buff, offset, value, element_type)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x54:
            value = _struct.unpack_from("!b", buff, offset)[0]
            return offset + 1, value

        return super().parse_value(buff, offset, format_code)

class _AmqpLong(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("long", int, 0x81, "!q", (0x55,))

    def emit(self, buff, offset, value, element_type=None):
        if value >= -128 and value <= 127:
            return buff.pack("!Bb", offset, 2, 0x55, value)

        return super().emit(buff, offset, value, element_type)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x55:
            value = _struct.unpack_from("!b", buff, offset)[0]
            return offset + 1, value

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
        super().__init__(name, python_type, long_format_code, (short_format_code,))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        return value

    def decode(self, octets):
        return bytes(octets)

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
            offset = buff.pack("!B", offset, 1, size)
        else:
            offset = buff.pack("!I", offset, 4, size)

        end = offset + size
        buff[offset:end] = octets

        return end, format_code

    def parse_value(self, buff, offset, format_code):
        assert format_code in (self.short_format_code, self.long_format_code)

        if format_code == self.short_format_code:
            size = _struct.unpack_from("!B", buff, offset)[0]
            offset += 1
        else:
            size = _struct.unpack_from("!I", buff, offset)[0]
            offset += 4

        end = offset + size
        value = self.decode(buff[offset:end])

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
        super().__init__(name, python_type, long_format_code, (short_format_code,))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def emit_size_and_count(self, buff, offset, size, count, format_code=None):
        if format_code is None:
            if size < 256:
                format_code = self.short_format_code
            else:
                format_code = self.long_format_code

        if format_code == self.short_format_code:
            assert size < 256 and count < 256

            offset = buff.pack("!BB", offset, 2, size, count)
        else:
            offset = buff.pack("!II", offset, 8, size, count)

        return offset, format_code

    def parse_size_and_count(self, buff, offset, format_code):
        if format_code == self.short_format_code:
            size, count = _struct.unpack_from("!BB", buff, offset)
            offset += 2
        elif format_code == self.long_format_code:
            size, count = _struct.unpack_from("!II", buff, offset)
            offset += 8
        else:
            raise Exception()

        return offset, size, count

class _AmqpCompoundType(_AmqpCollection):
    def __init__(self, name, python_type, short_format_code, long_format_code):
        super().__init__(name, python_type, short_format_code, long_format_code)

    def encode(self, value):
        buff = _Buffer()
        offset = 0

        for item in value:
            offset = emit_value(buff, offset, item)

        return buff[:offset], offset, len(value)

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

        octets, size, count = self.encode(value, element_type)
        offset, format_code = self.emit_size_and_count(buff, offset, size, count, format_code)

        offset, element_format_code_offset = element_type.emit_constructor(buff, offset, None)
        _struct.pack_into("!B", buff, element_format_code_offset, element_type.format_code)

        end = offset + size
        buff[offset:end] = octets

        return end, format_code

    def parse_value(self, buff, offset, format_code):
        offset, size, count = self.parse_size_and_count(buff, offset, format_code)

        element_format_code = _struct.unpack_from("!B", buff, offset)[0]
        offset += 1

        element_type = get_data_type_for_format_code(element_format_code)

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

# XXX get_data_type for obj

def get_data_type_for_python_type(python_type):
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

def get_data_type_for_format_code(format_code):
    try:
        return _data_types_by_format_code[format_code]
    except KeyError:
        raise Exception("No data type for format code 0x{:02X}".format(format_code))

def emit_value(buff, offset, value):
    data_type = get_data_type_for_python_type(type(value))
    return data_type.emit(buff, offset, value)

def parse_data(buff, offset):
    format_code = _struct.unpack_from("!B", buff, offset)[0]
    offset += 1

    data_type = get_data_type_for_format_code(format_code)

    return data_type.parse_value(buff, offset, format_code)

def _hex(buff):
    try:
        import binascii
    except ImportError:
        import ubinascii as binascii

    return binascii.hexlify(buff)

def _uuid_bytes():
    try:
        import utime as time
    except ImportError:
        import time

    try:
        import urandom as random
    except ImportError:
        import random

    random.seed(round(time.time() * 1000))

    values = (
        random.getrandbits(32),
        random.getrandbits(32),
        random.getrandbits(32),
        random.getrandbits(32),
    )

    return _struct.pack("IIII", *values)

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

    debug = False

    buff = _Buffer()
    offset = 0
    output_hexes = list()

    for i in range(10000):
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

    row = "{:5} {:17} {:>22} {:>22} {}"

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

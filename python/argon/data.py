try:
    import struct as _struct
except ImportError:
    import ustruct as _struct

_data_types_by_format_code = dict()
_data_types_by_python_type = dict()

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

    def encode(self, value):
        raise NotImplementedError()

    def decode(self, octets):
        raise NotImplementedError()

    def emit(self, buff, offset, value):
        raise NotImplementedError()

    def emit(self, buff, offset, value):
        assert isinstance(value, self.python_type)

        format_code_offset, offset = self.emit_constructor(buff, offset, None)
        offset = self.emit_value(buff, offset, value, format_code_offset)

        return offset

    def emit_constructor(self, buff, offset, descriptor):
        format_code_offset = offset
        return format_code_offset, offset + 1

    def emit_value(self, buff, offset, value, format_code_offset):
        octets = self.encode(value)

        end = offset + len(octets)
        buff[offset:end] = octets

        if format_code_offset is not None:
            _struct.pack_into("!B", buff, format_code_offset, self.format_code)

        return end

    def parse_value(self, buff, offset, format_code):
        assert format_code == self.format_code

        end = offset + 1
        value = self.decode(buff[offset:end])

        return end, value

class _AmqpNull(_AmqpDataType):
    def __init__(self):
        super().__init__("null", type(None), 0x40)

    def emit(self, buff, offset, value):
        _struct.pack_into("!B", buff, offset, self.format_code)
        return offset + 1

    def parse_value(self, buff, offset, format_code):
        return offset, None

class _AmqpBoolean(_AmqpDataType):
    def __init__(self):
        super().__init__("boolean", bool, 0x56, (0x41, 0x42))

    # XXX don't need these any more
    def encode(self, value):
        if value is True:
            return binary(0x01)

        if value is False:
            return binary(0x00)

        raise Exception()

    def decode(self, octets):
        if octets[0] == 0x00:
            return False

        if octets[0] == 0x01:
            return True

        raise Exception()

    def emit(self, buff, offset, value):
        if value is True:
            _struct.pack_into("!B", buff, offset, 0x41)
            return offset + 1

        if value is False:
            _struct.pack_into("!B", buff, offset, 0x42)
            return offset + 1

        raise Exception()

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x41: return offset, True
        if format_code == 0x42: return offset, False

        if format_code == 0x56:
            return super().parse_value(buff, offset, format_code)

        raise Exception()

class _AmqpFixedWidthType(_AmqpDataType):
    def __init__(self, name, python_type, format_code, format_string, special_format_codes=()):
        super().__init__(name, python_type, format_code, special_format_codes)

        self.format_string = format_string
        self.format_width = _struct.calcsize(self.format_string)

    def encode(self, value):
        print(111, value)
        return _struct.pack(self.format_string, value)

    def decode(self, octets):
        assert len(octets) == self.format_width

        return _struct.unpack(self.format_string, octets)[0]

    def parse_value(self, buff, offset, format_code):
        assert offset + self.format_width <= len(buff)

        end = offset + self.format_width
        value = self.decode(buff[offset:end])

        return end, value

class _AmqpUnsignedInt(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("uint", int, 0x70, "!I", (0x43, 0x52))

    def emit(self, buff, offset, value):
        if value == 0:
            _struct.pack_into("!B", buff, offset, 0x43)
            return offset + 1

        if value < 256:
            _struct.pack_into("!BB", buff, offset, 0x52, value)
            return offset + 2

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

    def emit(self, buff, offset, value):
        if value == 0:
            _struct.pack_into("!B", buff, offset, 0x44)
            return offset + 1

        if value < 256:
            _struct.pack_into("!BB", buff, offset, 0x53, value)
            return offset + 2

        return super().emit(buff, offset, value)

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

    def emit(self, buff, offset, value):
        if value >= -128 and value <= 127:
            _struct.pack_into("!Bb", buff, offset, 0x54, value)
            return offset + 2

        return super().emit(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x54:
            value = _struct.unpack_from("!b", buff, offset)[0]
            return offset + 1, value

        return super().parse_value(buff, offset, format_code)

class _AmqpLong(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("long", int, 0x81, "!q", (0x55,))

    def emit(self, buff, offset, value):
        if value >= -128 and value <= 127:
            _struct.pack_into("!Bb", buff, offset, 0x55, value)
            return offset + 2

        return super().emit(buff, offset, value)

    def parse_value(self, buff, offset, format_code):
        if format_code == 0x55:
            value = _struct.unpack_from("!b", buff, offset)[0]
            return offset + 1, value

        return super().parse_value(buff, offset, format_code)

class _AmqpChar(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("char", str, 0x73, "!4s")

    def encode(self, value):
        return value.encode("utf-32-be")

    def decode(self, octets):
        return octets.decode("utf-32-be")

class _AmqpTimestamp(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("timestamp", float, 0x83, "!q")

    def encode(self, value):
        value = int(round(value * 1000))
        return super().encode(value)

    def decode(self, octets):
        value = super().decode(octets)
        return round(value / 1000, 3)

class _AmqpVariableWidthType(_AmqpDataType):
    def __init__(self, name, python_type, short_format_code, long_format_code):
        super().__init__(name, python_type, long_format_code, (short_format_code,))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        return value

    def decode(self, octets):
        return bytes(octets)

    def emit_value(self, buff, offset, value, format_code_offset):
        octets = self.encode(value)
        size = len(octets)

        if size < 256:
            format_code = self.short_format_code

            _struct.pack_into("!B", buff, offset, size)
            offset += 1
        else:
            format_code = self.long_format_code

            _struct.pack_into("!I", buff, offset, size)
            offset += 4

        end = offset + size
        buff[offset:end] = octets

        if format_code_offset is not None:
            _struct.pack_into("!B", buff, format_code_offset, format_code)

        return end

    def parse_value(self, buff, offset, format_code):
        if format_code == self.short_format_code:
            size = _struct.unpack_from("!B", buff, offset)[0]
            offset += 1
        elif format_code == self.long_format_code:
            size = _struct.unpack_from("!I", buff, offset)[0]
            offset += 4
        else:
            raise Exception()

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

class _AmqpCompoundType(_AmqpDataType):
    def __init__(self, name, python_type, short_format_code, long_format_code):
        super().__init__(name, python_type, long_format_code, (short_format_code,))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        buff = memoryview(bytearray(10000)) # XXX buffers
        offset = 0

        for item in value:
            offset = emit_value(buff, offset, item)

        return buff[:offset]

    def decode(self, octets):
        value = list()
        offset = 0
        end = len(octets)

        while offset < end:
            offset, item = parse_data(octets, offset)
            value.append(item)

        assert offset == end

        return value

    def emit_value(self, buff, offset, value, format_code_offset):
        octets = self.encode(value)
        size = len(octets)
        count = len(value)

        if size < 256 and count < 256:
            format_code = self.short_format_code

            _struct.pack_into("!BB", buff, offset, size, count)
            offset += 2
        else:
            format_code = self.long_format_code

            _struct.pack_into("!II", buff, offset, size, count)
            offset += 8

        end = offset + size
        buff[offset:end] = octets

        if format_code_offset is not None:
            _struct.pack_into("!B", buff, format_code_offset, format_code)

        return end

    def parse_value(self, buff, offset, format_code):
        if format_code == self.short_format_code:
            size, count = _struct.unpack_from("!BB", buff, offset)
            offset += 2
        elif format_code == self.long_format_code:
            size, count = _struct.unpack_from("!II", buff, offset)
            offset += 8
        else:
            raise Exception()

        end = offset + size
        value = self.decode(buff[offset:end])

        return end, value

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

    def decode(self, octets):
        elems = super().decode(octets)
        pairs = dict()

        for i in range(0, len(elems), 2):
            pairs[elems[i]] = elems[i + 1]

        return pairs

class _AmqpArray(_AmqpDataType):
    def __init__(self, element_data_type):
        super().__init__("array", list, 0xf0, (0xe0,))

        self.short_format_code = 0xe0
        self.long_format_code = 0xf0

        self.element_data_type = element_data_type

    def __repr__(self):
        return "{}<{}>".format(self.name, self.element_data_type.name)

    def encode(self, value):
        buff = memoryview(bytearray(100000)) # XXX buffers
        offset = 0

        for item in value:
            offset = self.element_data_type.emit_value(buff, offset, item, None)

        return buff[:offset]

    def decode(self, octets):
        raise NotImplementedError()

    def emit_value(self, buff, offset, value, format_code_offset):
        octets = self.encode(value)
        size = len(octets)
        count = len(value)

        if size < 256 and count < 256:
            format_code = self.short_format_code

            _struct.pack_into("!BB", buff, offset, size, count)
            offset += 2
        else:
            format_code = self.long_format_code

            _struct.pack_into("!II", buff, offset, size, count)
            offset += 8

        if format_code_offset is not None:
            _struct.pack_into("!B", buff, format_code_offset, format_code)

        format_code_offset, offset = self.element_data_type.emit_constructor(buff, offset, None)

        _struct.pack_into("!B", buff, format_code_offset, self.element_data_type.format_code)

        end = offset + size
        buff[offset:end] = octets

        return end

    def parse_value(self, buff, offset, format_code):
        if format_code == self.short_format_code:
            size, count = _struct.unpack_from("!BB", buff, offset)
            offset += 2
        elif format_code == self.long_format_code:
            size, count = _struct.unpack_from("!II", buff, offset)
            offset += 8
        else:
            raise Exception()

        element_format_code = _struct.unpack_from("!B", buff, offset)[0]
        offset += 1

        element_data_type = get_data_type_for_format_code(element_format_code)

        value = list()
        end = offset + size

        while offset < end:
            offset, elem = element_data_type.parse_value(buff, offset, element_format_code)
            value.append(elem)

        assert offset == end

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

        (amqp_list, [0, 1, "abc"]),
        (amqp_list, [0, 1, ["a", "b", "c"]]),
        (amqp_list, [0, 1, {"a": 0, "b": 1}]),
        (amqp_map, {"a": 0, "b": 1, "c": 2}),
        (amqp_map, {"a": 0, "b": {0: "x", 1: "y"}}),
        (amqp_map, {"a": 0, "b": [0, 1, {"a": 0, "b": 1}]}),

        (_AmqpArray(amqp_ubyte), [0, 1, 2]),
        (_AmqpArray(amqp_short), [0, 1, 2]),
        (_AmqpArray(amqp_uint), [0, 1, 2]),
        (_AmqpArray(amqp_long), [0, 1, 2]),
        (_AmqpArray(amqp_float), [0.0, 1.5, 3.0]),
        (_AmqpArray(amqp_double), [0.0, 1.5, 3.0]),

        (_AmqpArray(amqp_timestamp), [0.0, round(time.time(), 3), -1.0]),
        (_AmqpArray(amqp_uuid), [_uuid_bytes(), _uuid_bytes(), _uuid_bytes(), ]),

        # (_AmqpArray(amqp_list), [[0, 1, "abc"], [0, 1, "abc"], [0, 1, "abc"]]),
        # (_AmqpArray(amqp_map), [{"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}]),
    ]

    buff = memoryview(bytearray(10000)) # XXX buffers
    offset = 0
    output_hexes = list()

    for type_, input_value in data:
        print("Emitting {} {}".format(type_, input_value))

        start = offset
        offset = type_.emit(buff, offset, input_value)

        hex_ = _hex(buff[start:offset])
        output_hexes.append(hex_)

        print("Emitted {}".format(hex_))

    offset = 0
    output_values = list()

    for type_, input_value in data:
        lookahead = _hex(buff[offset:offset + 10])
        print("Parsing {}... for {} {}".format(lookahead, type_, input_value))

        start = offset
        offset, value = parse_data(buff, offset)

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

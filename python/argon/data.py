try:
    import struct as _struct
except ImportError:
    import ustruct as _struct

_data_types_by_format_code = dict()
_data_types_by_python_type = dict()

class _AmqpDataType:
    def __init__(self, name, python_types, format_codes):
        self.name = name
        self.python_types = python_types
        self.format_codes = format_codes

        self.primary_format_code = self.format_codes[0]

        for type_ in self.python_types:
            _data_types_by_python_type[type_] = self

        for code in self.format_codes:
            _data_types_by_format_code[code] = self

    def __repr__(self):
        return self.name

    def encode(self, value):
        raise NotImplementedError()

    def decode(self, octets):
        raise NotImplementedError()

    def emit(self, buff, offset, value):
        octets = self.encode(value)
        size = len(octets)

        try:
            count = len(value)
        except TypeError:
            count = 1

        assert size < 0xffffffff
        assert count < 0xffffffff

        offset = self.emit_constructor(buff, offset, size, count)
        offset = self.emit_data(buff, offset, size, count, octets)

        return offset

    def emit_constructor(self, buff, offset, size, count):
        _struct.pack_into("!B", buff, offset, self.primary_format_code)
        return offset + 1

    def emit_data(self, buff, offset, size, count, octets):
        end = offset + size
        buff[offset:end] = octets

        return end

    def parse(self, buff, offset, format_code):
        return self.parse_data(buff, offset, format_code)

    def parse_data(self, buff, offset, format_code):
        end = offset + 1
        value = self.decode(buff[offset:end])

        return end, value

class _AmqpNull(_AmqpDataType):
    def __init__(self):
        super().__init__("null", (type(None),), (0x40,))

    def encode(self, value):
        return bytes()

    # XXX
    def decode(self, octets):
        return None

    def parse_data(self, buff, offset, format_code):
        assert format_code in self.format_codes
        return offset, None

class _AmqpBoolean(_AmqpDataType):
    def __init__(self):
        super().__init__("boolean", (bool,), (0x56, 0x41, 0x42))

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

    def parse_data(self, buff, offset, format_code):
        assert format_code in self.format_codes

        if format_code == 0x41: return offset, True
        if format_code == 0x42: return offset, False

        if format_code == 0x56:
            return super().parse_data(buff, offset, format_code)

        raise Exception()

class _AmqpFixedWidthType(_AmqpDataType):
    def __init__(self, name, python_types, format_codes, format_spec):
        super().__init__(name, python_types, format_codes)

        self.format_string = "!" + format_spec
        self.format_width = _struct.calcsize(self.format_string)

    def encode(self, value):
        if self.python_types:
            assert isinstance(value, self.python_types)

        return _struct.pack(self.format_string, value)

    def decode(self, octets):
        assert len(octets) == self.format_width

        return _struct.unpack(self.format_string, octets)[0]

    def parse_data(self, buff, offset, format_code):
        assert offset + self.format_width <= len(buff)
        assert format_code in self.format_codes

        end = offset + self.format_width
        value = self.decode(buff[offset:end])

        return end, value

class _AmqpChar(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("char", (), (0x73,), "4s")

    def encode(self, value):
        return value.encode("utf-32-be")

    def decode(self, octets):
        return octets.decode("utf-32-be")

class _AmqpTimestamp(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("timestamp", (), (0x83,), "q")

    def encode(self, value):
        value = int(round(value * 1000, 3))
        return super().encode(value)

    def decode(self, octets):
        value = super().decode(octets)
        return round(value / 1000, 3)

class _AmqpVariableWidthType(_AmqpDataType):
    def __init__(self, name, python_types, short_format_code, long_format_code):
        super().__init__(name, python_types, (short_format_code, long_format_code))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        return value

    def decode(self, octets):
        return bytes(octets)

    def emit_constructor(self, buff, offset, size, count):
        if size < 256:
            _struct.pack_into("!B", buff, offset, self.short_format_code)
        else:
            _struct.pack_into("!B", buff, offset, self.long_format_code)

        return offset + 1

    def emit_data(self, buff, offset, size, count, octets):
        if size < 256:
            _struct.pack_into("!B", buff, offset, size)
            offset += 1
        else:
            _struct.pack_into("!I", buff, offset, size)
            offset += 4

        return super().emit_data(buff, offset, size, count, octets)

    def parse_data(self, buff, offset, format_code):
        assert format_code in self.format_codes

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
        super().__init__("binary", (bytes,), 0xa0, 0xb0)

class _AmqpString(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("string", (str,), 0xa1, 0xb1)

    def encode(self, value):
        return value.encode("utf-8")

    def decode(self, octets):
        return bytes(octets).decode("utf-8")

class _AmqpSymbol(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("string", (), 0xa3, 0xb3)

    def encode(self, value):
        return value.encode("ascii")

    def decode(self, octets):
        return bytes(octets).decode("ascii")

class _AmqpCompoundType(_AmqpDataType):
    def __init__(self, name, python_types, short_format_code, long_format_code):
        super().__init__(name, python_types, (short_format_code, long_format_code))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def encode(self, value):
        buff = memoryview(bytearray(10000)) # XXX buffers
        offset = 0

        for item in value:
            offset = emit_data(buff, offset, item)

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

    def emit(self, buff, offset, value):
        assert isinstance(value, self.python_types)

        octets = self.encode(value)

        size = len(octets)
        count = len(value)

        offset = self.emit_constructor(buff, offset, size, count)
        offset = self.emit_data(buff, offset, size, count, octets)

        return offset

    def emit_constructor(self, buff, offset, size, count):
        if size < 256 and count < 256:
            _struct.pack_into("!B", buff, offset, self.short_format_code)
        else:
            _struct.pack_into("!B", buff, offset, self.long_format_code)

        return offset + 1

    def emit_data(self, buff, offset, size, count, octets):
        if size < 256 and count < 256:
            _struct.pack_into("!BB", buff, offset, size, count)
            offset += 2
        else:
            _struct.pack_into("!II", buff, offset, size, count)
            offset += 8

        return super().emit_data(buff, offset, size, count, octets)

    def parse_data(self, buff, offset, format_code):
        assert format_code in self.format_codes

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
        super().__init__("list", (list,), 0xc0, 0xd0)

class _AmqpMap(_AmqpCompoundType):
    def __init__(self):
        super().__init__("map", (dict,), 0xc1, 0xd1)

    def encode(self, value):
        list_ = list()

        for item in value.items():
            list_.extend(item)

        return super().encode(list_)

    def decode(self, octets):
        list_ = super().decode(octets)
        dict_ = dict()

        for i in range(0, len(list_), 2):
            dict_[list_[i]] = list_[i + 1]

        return dict_

class _AmqpArray(_AmqpDataType):
    def __init__(self, element_python_type):
        super().__init__("array", (), (0xe0, 0xf0))

        self.short_format_code = 0xe0
        self.long_format_code = 0xf0

        self.element_data_type = get_data_type_for_python_type(element_python_type)

    def encode(self, value):
        buff = memoryview(bytearray(100000)) # XXX buffers
        offset = 0

        for item in value:
            octets = self.element_data_type.encode(item)
            size = len(octets)

            try:
                count = len(item)
            except TypeError:
                count = 1

            assert size < 0xffffffff
            assert count < 0xffffffff

            offset = self.element_data_type.emit_data(buff, offset, size, count, octets)

        return buff[:offset]

    def decode(self, octets):
        raise NotImplementedError()

    def emit_constructor(self, buff, offset, size, count):
        if size < 256 and count < 256:
            _struct.pack_into("!B", buff, offset, self.short_format_code)
        else:
            _struct.pack_into("!B", buff, offset, self.long_format_code)

        return offset + 1

    def emit_data(self, buff, offset, size, count, octets):
        if size < 256 and count < 256:
            _struct.pack_into("!BB", buff, offset, size, count)
            offset += 2
        else:
            _struct.pack_into("!II", buff, offset, size, count)
            offset += 8

        offset = self.element_data_type.emit_constructor(buff, offset, size, count)
        offset = super().emit_data(buff, offset, size, count, octets)

        return offset

    def parse_data(self, buff, offset, format_code):
        assert format_code in self.format_codes

        if format_code == self.short_format_code:
            size, count = _struct.unpack_from("!BB", buff, offset)
            offset += 2
        elif format_code == self.long_format_code:
            size, count = _struct.unpack_from("!II", buff, offset)
            offset += 8
        else:
            raise Exception()

        format_code = _struct.unpack_from("!B", buff, offset)[0]
        offset += 1

        data_type = get_data_type_for_format_code(format_code)

        value = list()
        end = offset + size

        while offset < end:
            offset, item = data_type.parse_data(buff, offset, data_type.primary_format_code)
            value.append(item)

        assert offset == end

        return offset, value

amqp_null = _AmqpNull()
amqp_boolean = _AmqpBoolean()

amqp_ubyte = _AmqpFixedWidthType("ubyte", (), (0x50,), "B")
amqp_ushort = _AmqpFixedWidthType("ushort", (), (0x60,), "H")
amqp_uint = _AmqpFixedWidthType("uint", (), (0x70,), "I")
amqp_ulong = _AmqpFixedWidthType("ulong", (), (0x80,), "Q")

amqp_byte = _AmqpFixedWidthType("byte", (), (0x51,), "b")
amqp_short = _AmqpFixedWidthType("short", (), (0x61,), "h")
amqp_int = _AmqpFixedWidthType("int", (), (0x71,), "i")
amqp_long = _AmqpFixedWidthType("long", (int,), (0x81,), "q")

amqp_float =_AmqpFixedWidthType("float", (), (0x72,), "f")
amqp_double = _AmqpFixedWidthType("double", (float,), (0x82,), "d")

amqp_char = _AmqpChar()
amqp_timestamp = _AmqpTimestamp()
amqp_uuid = _AmqpFixedWidthType("uuid", (), (0x98,), "16s")

amqp_binary = _AmqpBinary()
amqp_string = _AmqpString()
amqp_symbol = _AmqpSymbol()

amqp_list = _AmqpList()
amqp_map = _AmqpMap()

def get_data_type_for_python_type(python_type):
    try:
        return _data_types_by_python_type[python_type]
    except KeyError:
        raise Exception("No data type for python type {}".format(type(value)))

def get_data_type_for_format_code(format_code):
    try:
        return _data_types_by_format_code[format_code]
    except KeyError:
        raise Exception("No data type for format code 0x{:02X}".format(format_code))

def emit_data(buff, offset, value):
    data_type = get_data_type_for_python_type(type(value))
    return data_type.emit(buff, offset, value)

def parse_data(buff, offset):
    format_code = _struct.unpack_from("!B", buff, offset)[0]
    offset += 1

    data_type = get_data_type_for_format_code(format_code)

    return data_type.parse_data(buff, offset, format_code)

amqp_array = _AmqpArray(int) # XXX

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

    data = [
        (amqp_null, None),
        (amqp_boolean, True),
        (amqp_boolean, False),

        (amqp_ubyte, 0),
        (amqp_ubyte, 0xff),
        (amqp_ushort, 0),
        (amqp_ushort, 0xffff),
        (amqp_uint, 0),
        (amqp_uint, 0xffffffff),
        (amqp_ulong, 0),
        (amqp_ulong, 0xffffffffffffffff),

        (amqp_byte, 127),
        (amqp_byte, -128),
        (amqp_short, -32768),
        (amqp_short, 32767),
        (amqp_int, -2147483648),
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
        (amqp_symbol, "Hello!"),
        (amqp_symbol, "x" * 256),

        (amqp_list, [1, 2, "abc"]),
        (amqp_list, [1, 2, {"a": 1, "b": 2}]),
        (amqp_map, {"a": 1, "b": [1, 2, {"a": 1, "b": 2}]}),

        (amqp_array, [1, 2, 3]),
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
        lookahead = _hex(buff[offset:min(10, len(buff) - offset)])
        print("Parsing {}... for {} {}".format(lookahead, type_, input_value))

        start = offset
        offset, value = parse_data(buff, offset)

        print("Parsed {}".format(_hex(buff[start:offset])))

        assert value == input_value, "Expected {} but got {}".format(input_value, value)

        output_values.append(value)

    row = "{:5} {:10} {:>22} {:>22} {}"

    for i, item in enumerate(data):
        type_, input_value = item
        output_value = output_values[i]
        output_hex = output_hexes[i]

        print(row.format(i, type_.name, str(input_value), str(output_value), output_hex))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

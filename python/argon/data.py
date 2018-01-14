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

        for type_ in self.python_types:
            _data_types_by_python_type[type_] = self

        for code in self.format_codes:
            _data_types_by_format_code[code] = self

    def __repr__(self):
        return self.name

    def marshal(self, obj):
        return obj

    def unmarshal(self, value):
        return value

class _AmqpNull(_AmqpDataType):
    def __init__(self):
        super().__init__("null", (type(None),), (0x40,))

    def emit(self, buff, offset, value):
        assert type(value) in self.python_types

        return self.emit_constructor(buff, offset, value)

    def emit_constructor(self, buff, offset, value):
        _struct.pack_into("!B", buff, offset, 0x40)
        return offset + 1

    def emit_value(self, buff, offset, value):
        return offset

    def parse(self, buff, offset, format_code):
        assert format_code in self.format_codes
        return offset, None

class _AmqpBoolean(_AmqpDataType):
    def __init__(self):
        super().__init__("boolean", (bool,), (0x41, 0x42, 0x56))

    def emit(self, buff, offset, value):
        if value is True:
            _struct.pack_into("!B", buff, offset, 0x41)
        elif value is False:
            _struct.pack_into("!B", buff, offset, 0x42)
        else:
            raise Exception()

        return offset + 1

    def emit_constructor(self, buff, offset, value):
        _struct.pack_into("!B", buff, offset, 0x56)
        return offset + 1

    def emit_value(self, buff, offset, value):
        if value is True:
            _struct.pack_into("!B", buff, offset, 0x01)
        elif value is False:
            _struct.pack_into("!B", buff, offset, 0x00)
        else:
            raise Exception()

        return offset + 1

    def parse(self, buff, offset, format_code):
        assert format_code in self.format_codes

        if format_code == 0x41: return offset, True
        if format_code == 0x42: return offset, False

        if format_code == 0x56:
            (value,) = _struct.unpack_from("!B", buff, offset)
            offset += 1

            if value == 0x00: return offset, False
            if value == 0x01: return offset, True

            raise Exception()

        raise Exception()

class _AmqpFixedWidthType(_AmqpDataType):
    def __init__(self, name, python_types, format_codes, format_spec):
        super().__init__(name, python_types, format_codes)

        self.format_code = format_codes[0]
        self.format_string = "!" + format_spec
        self.format_width = _struct.calcsize(self.format_string)

    def emit(self, buff, offset, obj):
        assert offset + self.format_width < len(buff)
        #assert isinstance(obj, self.python_types) XXX

        value = self.marshal(obj)

        offset = self.emit_constructor(buff, offset, value)
        offset = self.emit_value(buff, offset, value)

        return offset

    def emit_constructor(self, buff, offset, value):
        _struct.pack_into("!B", buff, offset, self.format_code)
        return offset + 1

    def emit_value(self, buff, offset, value):
        _struct.pack_into(self.format_string, buff, offset, value)
        return offset + self.format_width

    def parse(self, buff, offset, format_code):
        assert offset + self.format_width < len(buff)
        assert format_code in self.format_codes

        (value,) = _struct.unpack_from(self.format_string, buff, offset)
        offset += self.format_width

        obj = self.unmarshal(value)

        return offset, obj

class _AmqpChar(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("char", (), (0x73,), "4s")

    def marshal(self, char):
        return char.encode("utf-32-be")

    def unmarshal(self, bytes_):
        return bytes_.decode("utf-32-be")

class _AmqpTimestamp(_AmqpFixedWidthType):
    def __init__(self):
        super().__init__("timestamp", (), (0x83,), "q")

    def marshal(self, time_):
        return int(round(time_ * 1000, 3))

    def unmarshal(self, value):
        return round(value / 1000, 3)

class _AmqpVariableWidthType(_AmqpDataType):
    def __init__(self, name, python_types, short_format_code, long_format_code):
        super().__init__(name, python_types, (short_format_code, long_format_code))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def emit(self, buff, offset, obj):
        #assert isinstance(obj, self.python_types) XXX

        bytes_ = self.marshal(obj)

        offset = self.emit_constructor(buff, offset, bytes_)
        offset = self.emit_value(buff, offset, bytes_)

        return offset

    def emit_constructor(self, buff, offset, bytes_):
        size = len(bytes_) # XXX I don't love passing bytes_ around here

        if size < 256:
            _struct.pack_into("!B", buff, offset, self.short_format_code)
        else:
            assert size < 0xffffffff

            _struct.pack_into("!B", buff, offset, self.long_format_code)

        return offset + 1

    def emit_value(self, buff, offset, bytes_):
        size = len(bytes_)

        if size < 256:
            _struct.pack_into("!B", buff, offset, size)
            offset += 1
        else:
            assert size < 0xffffffff

            _struct.pack_into("!I", buff, offset, size)
            offset += 4

        start = offset
        end = start + size

        buff[start:end] = bytes_

        return end

    def parse(self, buff, offset, format_code):
        assert format_code in self.format_codes

        if format_code == self.short_format_code:
            (size,) = _struct.unpack_from("!B", buff, offset)
            start = offset + 1
        elif format_code == self.long_format_code:
            (size,) = _struct.unpack_from("!I", buff, offset)
            start = offset + 4
        else:
            raise Exception()

        end = start + size
        string = self.unmarshal(bytes(buff[start:end]))

        return end, string

class _AmqpBinary(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("binary", (bytes,), 0xa0, 0xb0)

    def marshal(self, bytes_):
        return bytes_

    def unmarshal(self, bytes_):
        return bytes_

class _AmqpString(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("string", (str,), 0xa1, 0xb1)

    def marshal(self, string):
        return string.encode("utf-8")

    def unmarshal(self, bytes_):
        return bytes_.decode("utf-8")

class _AmqpSymbol(_AmqpVariableWidthType):
    def __init__(self):
        super().__init__("string", (), 0xa3, 0xb3)

    def marshal(self, string):
        return string.encode("ascii")

    def unmarshal(self, bytes_):
        return bytes_.decode("ascii")

class _AmqpCompoundType(_AmqpDataType):
    def __init__(self, name, python_types, short_format_code, long_format_code):
        super().__init__(name, python_types, (short_format_code, long_format_code))

        self.short_format_code = short_format_code
        self.long_format_code = long_format_code

    def emit(self, buff, offset, obj):
        assert isinstance(obj, self.python_types)

        values = self.marshal(obj)

        inner_buff = memoryview(bytearray(1000)) # XXX buffers
        inner_offset = 0

        for value in values:
            inner_offset = emit_data(inner_buff, inner_offset, value)

        size = inner_offset
        count = len(values)

        if size < 256 and count < 256:
            _struct.pack_into("!BBB", buff, offset, self.short_format_code, size, count)
            offset += 3
        else:
            assert size < 0xffffffff
            assert count < 0xffffffff

            _struct.pack_into("!BII", buff, offset, self.long_format_code, size, count)
            offset += 9

        start = offset
        end = start + size

        buff[start:end] = inner_buff[:inner_offset]

        return end

    def parse(self, buff, offset, format_code):
        assert format_code in self.format_codes

        if format_code == self.short_format_code:
            size, count = _struct.unpack_from("!BB", buff, offset)
            offset += 2
        elif format_code == self.long_format_code:
            size, count = _struct.unpack_from("!II", buff, offset)
            offset += 8
        else:
            raise Exception()

        start = offset
        end = start + size

        values = list()

        while offset < end:
            offset, value = parse_data(buff, offset)
            values.append(value)

        assert offset == end

        obj = self.unmarshal(values)

        return end, obj

class _AmqpList(_AmqpCompoundType):
    def __init__(self):
        super().__init__("list", (list,), 0xc0, 0xd0)

    def marshal(self, obj):
        return obj

    def unmarshal(self, values):
        return values

class _AmqpMap(_AmqpCompoundType):
    def __init__(self):
        super().__init__("map", (dict,), 0xc1, 0xd1)

    def marshal(self, obj):
        values = list()

        for item in obj.items():
            values.extend(item)

        return values

    def unmarshal(self, values):
        obj = dict()

        for i in range(0, len(values), 2):
            obj[values[i]] = values[i + 1]

        return obj

class _AmqpArray(_AmqpDataType):
    def __init__(self):
        super().__init__("array", (), (0xe0, 0xf0))

    def emit(self, buff, offset, objs):
        if len(objs) == 0:
            raise NotImplementedError() # XXX

        first_obj = objs[0]
        data_type = get_data_type_for_python_type(first_obj)

        inner_buff = memoryview(bytearray(1000)) # XXX buffers
        inner_offset = 0

        for obj in objs:
            assert type(obj) == type(first_obj)
            # XXX Need to ensure they're the same width as well

            value = data_type.marshal(obj)
            inner_offset = data_type.emit_value(inner_buff, inner_offset, value)

        size = inner_offset
        count = len(objs)

        if size < 256 and count < 256:
            _struct.pack_into("!BBB", buff, offset, 0xe0, size, count)
            offset += 3
        else:
            assert size < 0xffffffff
            assert count < 0xffffffff

            _struct.pack_into("!BII", buff, offset, 0xf0, size, count)
            offset += 9

        offset = data_type.emit_constructor(buff, offset, obj)

        start = offset
        end = start + size

        buff[start:end] = inner_buff[:inner_offset]

    def parse(self, buff, offset, format_code):
        assert format_code in self.format_codes

        if format_code == 0xe0:
            size, count = _struct.unpack_from("!BB", buff, offset)
            offset += 2
        elif format_code == 0xf0:
            size, count = _struct.unpack_from("!II", buff, offset)
            offset += 8
        else:
            raise Exception()

        # XXX parse_constructor

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

amqp_array = _AmqpArray()

def get_data_type_for_python_type(value):
    try:
        return _data_types_by_python_type[type(value)]
    except KeyError:
        raise Exception("No data type for python type {}".format(type(value)))

def get_data_type_for_format_code(format_code):
    try:
        return _data_types_by_format_code[format_code]
    except KeyError:
        raise Exception("No data type for format code 0x{:02X}".format(format_code))

def emit_data(buff, offset, value):
    data_type = get_data_type_for_python_type(value)
    return data_type.emit(buff, offset, value)

def parse_data(buff, offset):
    (format_code,) = _struct.unpack_from("!B", buff, offset)

    data_type = get_data_type_for_format_code(format_code)

    return data_type.parse(buff, offset + 1, format_code)

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

        # (amqp_array, [1, 2, 3]),
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
        print("Parsing {} {}".format(type_, input_value))

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

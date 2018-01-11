import struct as _struct

class _AmqpDataType:
    def __init__(self, format_code):
        self.format_code = format_code

class _AmqpNull(_AmqpDataType):
    def __init__(self):
        super().__init__(0x40)

    def emit(self, buff, offset):
        _struct.pack_into("!B", buff, offset, self.format_code)
        return offset + 1

    def parse(self, buff, offset):
        values = _struct.unpack_from("!B", buff, offset)

        assert values[0] == 0x40

        return offset + 1, None

class _AmqpBoolean(_AmqpDataType):
    def __init__(self):
        super().__init__(0x56)

    def emit(self, buff, offset, value):
        if value is True:
            _struct.pack_into("!B", buff, offset, 0x41)
        elif value is False:
            _struct.pack_into("!B", buff, offset, 0x42)
        else:
            raise Exception()

        return offset + 1

    def parse(self, buff, offset):
        values = _struct.unpack_from("!B", buff, offset)

        if values[0] == 0x41:
            return offset + 1, True
        elif values[0] == 0x42:
            return offset + 1, False
        elif values[0] == 0x56:
            raise NotImplementedError()
        else:
            raise Exception()

class _AmqpFixedSizeType(_AmqpDataType):
    def __init__(self, format_code, format_spec):
        super().__init__(format_code)

        self.format_struct = _struct.Struct("!B" + format_spec)

    def emit(self, buff, offset, value):
        assert offset + self.format_struct.size < len(buff)

        self.format_struct.pack_into(buff, offset, self.format_code, value)

        return offset + self.format_struct.size

    def parse(self, buff, offset):
        assert offset + self.format_struct.size < len(buff)

        values = self.format_struct.unpack_from(buff, offset)

        assert values[0] == self.format_code

        return offset + self.format_struct.size, values[1]

amqp_null = _AmqpNull()
amqp_boolean = _AmqpBoolean()

amqp_ubyte = _AmqpFixedSizeType(0x50, "B")
amqp_ushort = _AmqpFixedSizeType(0x60, "H")
amqp_uint = _AmqpFixedSizeType(0x70, "I")
amqp_ulong = _AmqpFixedSizeType(0x80, "Q")

amqp_byte = _AmqpFixedSizeType(0x51, "b")
amqp_short = _AmqpFixedSizeType(0x61, "h")
amqp_int = _AmqpFixedSizeType(0x71, "i")
amqp_long = _AmqpFixedSizeType(0x81, "q")

def _hex(buff):
    import binascii
    return binascii.hexlify(buff)

def _main():
    buff = bytearray(100)

    offset = amqp_null.emit(buff, 0)
    offset = amqp_boolean.emit(buff, offset, True)
    offset = amqp_boolean.emit(buff, offset, False)
    
    offset = amqp_ubyte.emit(buff, offset, 0)
    offset = amqp_ubyte.emit(buff, offset, 0xff)
    offset = amqp_ushort.emit(buff, offset, 0)
    offset = amqp_ushort.emit(buff, offset, 0xffff)
    offset = amqp_uint.emit(buff, offset, 0)
    offset = amqp_uint.emit(buff, offset, 0xffffffff)
    offset = amqp_ulong.emit(buff, offset, 0)
    offset = amqp_ulong.emit(buff, offset, 0xffffffffffffffff)
    
    offset = amqp_byte.emit(buff, offset, 127)
    offset = amqp_byte.emit(buff, offset, -128)
    offset = amqp_short.emit(buff, offset, -32768)
    offset = amqp_short.emit(buff, offset, 32767)
    offset = amqp_int.emit(buff, offset, -2147483648)
    offset = amqp_int.emit(buff, offset, 2147483647)
    offset = amqp_long.emit(buff, offset, -9223372036854775808)
    offset = amqp_long.emit(buff, offset, 9223372036854775807)

    print(_hex(buff))

    values = list()

    offset, value = amqp_null.parse(buff, 0)
    values.append(value)
    offset, value = amqp_boolean.parse(buff, offset)
    values.append(value)
    offset, value = amqp_boolean.parse(buff, offset)
    values.append(value)
    
    offset, value = amqp_ubyte.parse(buff, offset)
    values.append(value)
    offset, value = amqp_ubyte.parse(buff, offset)
    values.append(value)
    offset, value = amqp_ushort.parse(buff, offset)
    values.append(value)
    offset, value = amqp_ushort.parse(buff, offset)
    values.append(value)
    offset, value = amqp_uint.parse(buff, offset)
    values.append(value)
    offset, value = amqp_uint.parse(buff, offset)
    values.append(value)
    offset, value = amqp_ulong.parse(buff, offset)
    values.append(value)
    offset, value = amqp_ulong.parse(buff, offset)
    values.append(value)
    
    offset, value = amqp_byte.parse(buff, offset)
    values.append(value)
    offset, value = amqp_byte.parse(buff, offset)
    values.append(value)
    offset, value = amqp_short.parse(buff, offset)
    values.append(value)
    offset, value = amqp_short.parse(buff, offset)
    values.append(value)
    offset, value = amqp_int.parse(buff, offset)
    values.append(value)
    offset, value = amqp_int.parse(buff, offset)
    values.append(value)
    offset, value = amqp_long.parse(buff, offset)
    values.append(value)
    offset, value = amqp_long.parse(buff, offset)
    values.append(value)

    print(values)

if __name__ == "__main__":
    _main()

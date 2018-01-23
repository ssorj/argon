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
from argon.common import _hex, _shorten, _time, _uuid_bytes
from argon.data import *

_data = [
    (NullType(), None),
    (NullType(Symbol("a")), None),

    (BooleanType(), True),
    (BooleanType(), False),

    (UnsignedByteType(), 0),
    (UnsignedByteType(), 0xff),
    (UnsignedShortType(), 0),
    (UnsignedShortType(), 0xffff),
    (UnsignedIntType(), 0),
    (UnsignedIntType(), 128),
    (UnsignedIntType(), 0xffffffff),
    (UnsignedLongType(), 0),
    (UnsignedLongType(), 128),
    (UnsignedLongType(), 0xffffffffffffffff),
    #(UnsignedLongType(), UnsignedLong(12)),

    (ByteType(), 127),
    (ByteType(), -128),
    (ShortType(), -32768),
    (ShortType(), 32767),
    (IntType(), -2147483648),
    (IntType(), 0),
    (IntType(), 2147483647),
    (LongType(), -9223372036854775808),
    (LongType(), 9223372036854775807),

    (FloatType(), 1.0),
    (FloatType(), -1.0),
    (DoubleType(), 1.0),
    (DoubleType(), -1.0),

    # XXX Fails to decode to a single char on micropython
    # (_char_type, "a"),

    (TimestampType(), round(_time.time(), 3)),
    (UuidType(), _uuid_bytes()),

    (BinaryType(), b"123"),
    (BinaryType(), b"x" * 256),
    (StringType(), "Hello, \U0001F34B!"),
    (StringType(), "\U0001F34B" * 256),
    (SymbolType(), "hello"),
    (SymbolType(), "x" * 256),

    (ListType(), [None, 0, 1, "abc"]),
    (ListType(), [0, 1, ["a", "b", "c"]]),
    (ListType(), [0, 1, {"a": 0, "b": 1}]),
    (MapType(), {None: 0, "a": 1, "b": 2}),
    (MapType(), {"a": 0, "b": {0: "x", 1: "y"}}),
    (MapType(), {"a": 0, "b": [0, 1, {"a": 0, "b": 1}]}),

    (ArrayType(NullType()), [None, None, None]),
    (ArrayType(UnsignedByteType()), [0, 1, 2]),
    (ArrayType(UnsignedShortType()), [0, 1, 2]),
    (ArrayType(UnsignedIntType()), [0, 1, 2]),
    (ArrayType(LongType()), [0, 1, 2]),
    (ArrayType(FloatType()), [0.0, 1.5, 3.0]),

    (ArrayType(DoubleType()), [0.0, 1.5, 3.0]),

    (ArrayType(TimestampType()), [0.0, round(_time.time(), 3), -1.0]),
    (ArrayType(UuidType()), [_uuid_bytes(), _uuid_bytes(), _uuid_bytes()]),

    (ArrayType(ListType()), [[0, 1, "abc"], [0, 1, "abc"], [0, 1, "abc"]]),
    (ArrayType(MapType()), [{"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}]),
    (ArrayType(ArrayType(BooleanType())), [[True, False], [True, False], [True, False]]),
]

def _main():
    debug = False

    buff = Buffer()
    offset = 0
    output_hexes = list()

    for i in range(1):
        for data_type, input_value in _data:
            if debug:
                print("Emitting {} {}".format(data_type, input_value))

            start = offset
            offset = data_type.emit(buff, offset, input_value)

            hex_ = _hex(buff[start:offset])
            output_hexes.append(hex_)

            if debug:
                print("Emitted {}".format(hex_))

        offset = 0
        output_values = list()

        for data_type, input_value in _data:
            if debug:
                lookahead = _hex(buff[offset:offset + 10])
                print("Parsing {}... for {} {}".format(lookahead, data_type, input_value))

            start = offset
            offset, value = parse_data(buff, offset)

            if debug:
                print("Parsed {}".format(_hex(buff[start:offset])))

            assert value == input_value, "Expected {} but got {}".format(input_value, value)

            output_values.append(value)

    row = "{:4} {:22} {:>22} {:>22} {}"

    for i, item in enumerate(_data):
        data_type, input_value = item
        output_value = output_values[i]
        output_hex = output_hexes[i]

        print(row.format(i, repr(data_type), _shorten(str(input_value)), _shorten(str(output_value)), output_hex))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

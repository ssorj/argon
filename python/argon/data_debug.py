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
from argon.common import _micropython, _shorten, _time, _uuid_bytes
from argon.data import *
from argon.data import _data_hex, _hex

_input_values = [
    None,
    DescribedValue("a", None),

    True,
    False,
]

if not _micropython:
    _input_values += [
        UnsignedByte(0),
        UnsignedByte(0xff),

        UnsignedShort(0),
        UnsignedShort(0xffff),
        UnsignedInt(0),
        UnsignedInt(128),
        UnsignedInt(0xffffffff),
        UnsignedLong(0),
        UnsignedLong(128),
        UnsignedLong(0xffffffffffffffff),

        Byte(-128),
        Byte(127),

        Short(-32768),
        Short(32767),

        Int(-2147483648),
        Int(2147483647),

        Float(1.0),
        Float(-1.0),

        Decimal32(b"1111"),
        Decimal64(b"22222222"),
        Decimal128(b"4444444444444444"),

        Char("a"),
        Timestamp(round(_time.time(), 3)),

        Symbol("hello"),
        Symbol("x" * 256),
    ]

_input_values += [
    -128,
    127,

    -32768,
    32767,

    -2147483648,
    2147483647,

    -9223372036854775808,
    9223372036854775807,

    -1.0,
    1.0,

    b"123",
    b"x" * 256,

    "Hello, \U0001F34B!",
    "\U0001F34B" * 256,

    Uuid(_uuid_bytes()),

    [None, 0, 1, "abc"],
    [0, 1, ["a", "b", "c"]],
    [0, 1, {"a": 0, "b": 1}],
    [None] * 256,

    {None: 0, "a": 1, "b": 2},
    {"a": 0, "b": {0: "x", 1: "y"}},
    {"a": 0, "b": [0, 1, {"a": 0, "b": 1}]},

    Array(type(None), [None, None, None]),
    Array(type(None), [None] * 256),
    Array(UnsignedByte, [0, 1, 2]),
    Array(int, [0, 1, 2]),
    Array(float, [0.0, 1.5, 3.0]),
    Array(Uuid, [_uuid_bytes(), _uuid_bytes(), _uuid_bytes()]),

    Array(list, [[0, 1, "abc"], [0, 1, "abc"], [0, 1, "abc"]]),
    Array(dict, [{"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}, {"a": 0, "b": 1, "c": 2}]),
    Array(Array, [Array(bool, [True, False]), Array(bool, [True, False])]),
]

def _main():
    debug = True

    buff = Buffer()
    offset = 0

    output_octets = list()

    for value in _input_values:
        if debug:
            print("Emitting data for {}".format(value))

        start = offset
        offset = emit_data(buff, offset, value)

        octets = _data_hex(buff[start:offset])

        output_octets.append(octets)

        if debug:
            print("Emitted {}".format(octets))

    offset = 0

    output_values = list()

    for value in _input_values:
        if debug:
            lookahead = _hex(buff[offset:offset + 20])
            print("Parsing {}... for {}".format(lookahead, value))

        start = offset
        offset, output_value = parse_data(buff, offset)

        if debug:
            print("Parsed {}".format(_data_hex(buff[start:offset])))

        msg = "Expected {} {} but got {} {}".format(type(value), value, type(output_value), output_value)
        assert output_value == value, msg

        output_values.append(output_value)

    row = "{:4}  {:>24}  {:16}  {:>24}  {:16}  {}"

    for i, value in enumerate(_input_values):
        output_value = output_values[i]
        octets = output_octets[i]

        args = (
            i,
            _shorten(str(value), 24),
            type(value).__name__,
            _shorten(str(output_value), 24),
            type(output_value).__name__,
            octets,
        )

        print(row.format(*args))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

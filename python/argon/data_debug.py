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
from argon.common import _hex, _micropython, _shorten, _time, _uuid_bytes
from argon.data import *

def _data_hex(octets):
    o = _hex(octets)
    return "{} {}".format(o[0:2], o[2:])

class _DescribedValue:
    def __init__(self, descriptor, value):
        self.descriptor = descriptor
        self.value = value

    def __repr__(self):
        return "{}:{}".format(self.descriptor, self.value)

    def __eq__(self, other):
        return self.descriptor == other.descriptor and self.value == other.value

_input_values = [
    None,
    _DescribedValue("a", None),

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

        Char("a"),

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

    Timestamp(round(_time.time(), 3)),
    Uuid(_uuid_bytes()),

    [None, 0, 1, "abc"],
    [0, 1, ["a", "b", "c"]],
    [0, 1, {"a": 0, "b": 1}],

    {None: 0, "a": 1, "b": 2},
    {"a": 0, "b": {0: "x", 1: "y"}},
    {"a": 0, "b": [0, 1, {"a": 0, "b": 1}]},

    Array(type(None), [None, None, None]),
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
        descriptor = None

        if isinstance(value, _DescribedValue):
            descriptor = value.descriptor
            value = value.value

        if debug:
            print("Emitting data for {}".format(value))

        start = offset
        offset = emit_data(buff, offset, value, descriptor)

        octets = _data_hex(buff[start:offset])

        output_octets.append(octets)

        if debug:
            print("Emitted {}".format(octets))

    offset = 0

    output_values = list()

    for value in _input_values:
        descriptor = None

        if isinstance(value, _DescribedValue):
            descriptor = value.descriptor
            value = value.value

        if debug:
            lookahead = _hex(buff[offset:offset + 20])
            print("Parsing {}... for {}".format(lookahead, value))

        start = offset
        offset, output_value, output_descriptor = parse_data(buff, offset)

        if debug:
            print("Parsed {}".format(_data_hex(buff[start:offset])))

        #print(111, repr(output_value), type(output_value), "==", repr(value), type(output_value))
        
        assert output_value == value, "Expected {} but got {}".format(value, output_value)

        output_values.append(output_value)

    row = "{:4}  {:>24}  {:16}  {:>24}  {:16}  {}"

    for i, value in enumerate(_input_values):
        descriptor = None

        if isinstance(value, _DescribedValue):
            descriptor = value.descriptor
            value = value.value

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

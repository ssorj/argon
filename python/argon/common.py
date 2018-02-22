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

import sys as _sys

_micropython = _sys.implementation.name == "micropython"

if _micropython:
    import gc as _gc
    import uos as _os
    import urandom as _random
    import uselect as _select
    import usocket as _socket
    import ustruct as _struct
    import utime as _time
else:
    _gc = None
    import os as _os
    import random as _random
    import select as _select
    import socket as _socket
    import struct as _struct
    import time as _time

try:
    _DEBUG = _os.getenv("ARGON_DEBUG") is not None
except AttributeError:
    _DEBUG = False

class Buffer:
    def __init__(self):
        self._octets = bytearray(256)
        self._view = memoryview(self._octets)

    def skip(self, offset, size):
        return offset + size, offset

    def read(self, offset, size):
        end = offset + size
        return end, self._view[offset:end]

    def write(self, offset, octets):
        end = offset + len(octets)

        self.ensure(end)
        self._octets[offset:end] = octets

        return end

    def unpack(self, offset, size, format_string):
        assert len(self) > offset + size

        values = _struct.unpack_from(format_string, self._view, offset)

        return (offset + size,) + values

    def __getitem__(self, index):
        return self._view[index]

    def __setitem__(self, index, value):
        self._view[index] = value

    def __len__(self):
        return len(self._octets)

    def ensure(self, size):
        if len(self._octets) < size:
            new_size = max(size, 2 * len(self._octets))

            self._octets = self._octets + bytearray([0] * (new_size - len(self._octets)))
            self._view = memoryview(self._octets)

    if _micropython:
        # XXX Hideous bad hack
        def pack(self, offset, size, format_string, *values):
            self.ensure(offset + size)

            from argon.data import UnsignedByte, UnsignedShort, UnsignedInt, UnsignedLong

            values = list(values)

            for i, value in enumerate(values):
                if isinstance(value, UnsignedByte):
                    values[i] = int.from_bytes(value.to_bytes(1, "big"), "big")
                elif isinstance(value, UnsignedShort):
                    values[i] = int.from_bytes(value.to_bytes(2, "big"), "big")
                elif isinstance(value, UnsignedInt):
                    values[i] = int.from_bytes(value.to_bytes(4, "big"), "big")
                elif isinstance(value, UnsignedLong):
                    values[i] = int.from_bytes(value.to_bytes(8, "big"), "big")

            _struct.pack_into(format_string, self._octets, offset, *values)

            return offset + size
    else:
        def pack(self, offset, size, format_string, *values):
            self.ensure(offset + size)

            _struct.pack_into(format_string, self._octets, offset, *values)

            return offset + size

def _uuid_bytes():
    _random.seed(round(_time.time() * 1000))

    values = (
        _random.getrandbits(32),
        _random.getrandbits(32),
        _random.getrandbits(32),
        _random.getrandbits(32),
    )

    return _struct.pack("IIII", *values)

def _hex(data):
    return "".join(["{:02x}".format(x) for x in data])

def _shorten(string, max_=20):
    if string is None:
        return string

    return string[:min(max_, len(string))]

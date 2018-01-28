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
    import urandom as _random
    import uselect as _select
    import usocket as _socket
    import ustruct as _struct
    import utime as _time
else:
    import random as _random
    import select as _select
    import socket as _socket
    import struct as _struct
    import time as _time

class Buffer:
    def __init__(self):
        self.octets = bytearray(256)
        self.view = memoryview(self.octets)

    def skip(self, offset, size):
        return offset + size, offset

    def read(self, offset, size):
        end = offset + size
        return end, self.view[offset:end]

    def write(self, offset, octets):
        end = offset + len(octets)

        self.ensure(end)
        self.octets[offset:end] = octets

        return end

    def unpack(self, offset, size, format_string):
        assert len(self) > offset + size

        values = _struct.unpack_from(format_string, self.view, offset)

        return (offset + size,) + values

    def __getitem__(self, index):
        return self.view[index]

    def __setitem__(self, index, value):
        self.view[index] = value

    def __len__(self):
        return len(self.octets)

    if _micropython:
        # XXX Hideous bad hack
        def pack(self, offset, size, format_string, *values):
            self.ensure(offset + size)

            from argon.data import UnsignedLong

            values = list(values)

            for i, value in enumerate(values):
                if isinstance(value, UnsignedLong):
                    values[i] = int.from_bytes(value.to_bytes(8, "big"), "big")

            _struct.pack_into(format_string, self.octets, offset, *values)

            return offset + size

        def ensure(self, size):
            if len(self.octets) < size:
                new_size = max(size, 2 * len(self.octets))

                self.octets = self.octets + bytearray([0] * new_size)
                self.view = memoryview(self.octets)
    else:
        def pack(self, offset, size, format_string, *values):
            self.ensure(offset + size)

            _struct.pack_into(format_string, self.octets, offset, *values)

            return offset + size

        def ensure(self, size):
            if len(self.octets) < size:
                new_size = max(size, 2 * len(self.octets))

                self.view.release()
                self.octets.extend([0] * max(size, 2 * len(self.octets)))
                self.view = memoryview(self.octets)

def _hex(data):
    return "".join(["{:02x}".format(x) for x in data])

def _uuid_bytes():
    _random.seed(round(_time.time() * 1000))

    values = (
        _random.getrandbits(32),
        _random.getrandbits(32),
        _random.getrandbits(32),
        _random.getrandbits(32),
    )

    return _struct.pack("IIII", *values)

def _shorten(string, max_=20):
    return string[:min(max_, len(string))]

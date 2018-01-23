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

try:
    from collections import namedtuple as _namedtuple
except ImportError:
    from ucollections import namedtuple as _namedtuple

try:
    import ustruct as _struct
except ImportError:
    import struct as _struct

try:
    import utime as _time
except ImportError:
    import time as _time

try:
    import urandom as random
except ImportError:
    import random

class Buffer:
    def __init__(self):
        self.octets = bytearray(256)
        self.view = memoryview(self.octets)

    def ensure(self, size):
        if len(self.octets) < size:
            new_size = max(size, 2 * len(self.octets))

            # XXX
            if _micropython:
                self.octets = self.octets + bytearray([0] * new_size)
            else:
                self.view.release()
                self.octets.extend([0] * max(size, 2 * len(self.octets)))
                self.view = memoryview(self.octets)

    def skip(self, offset, size):
        return offset + size, offset

    if _micropython:
        def read(self, offset, size):
            end = offset + size
            return end, self.octets[offset:end]
    else:
        def read(self, offset, size):
            end = offset + size
            return end, self.view[offset:end]

    def write(self, offset, octets):
        end = offset + len(octets)

        self.ensure(end)
        self.octets[offset:end] = octets

        return end

    def pack(self, offset, size, format_string, *values):
        self.ensure(offset + size)

        _struct.pack_into(format_string, self.octets, offset, *values)

        return offset + size

    def unpack(self, offset, size, format_string):
        assert len(self) > offset + size

        values = _struct.unpack_from(format_string, self.octets, offset)

        return (offset + size,) + values

    def send(self, offset, size, sock):
        offset, data = self.read(offset, size)
        return offset

    def __getitem__(self, index):
        return self.view[index]

    def __setitem__(self, index, value):
        self.octets[index] = value # XXX view

    def __len__(self):
        return len(self.octets)

def _hex(data):
    return "".join(["{:02x}".format(x) for x in data])

def _uuid_bytes():
    random.seed(round(_time.time() * 1000))

    values = (
        random.getrandbits(32),
        random.getrandbits(32),
        random.getrandbits(32),
        random.getrandbits(32),
    )

    return _struct.pack("IIII", *values)

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

from argon.common import _hex
from argon.data import *
from argon.data import _data_hex

class _Frame:
    __slots__ = "channel", "_field_values", "payload"

    def __init__(self, channel, field_values=None, payload=None):
        self.channel = channel
        self._field_values = field_values
        self.payload = payload

        if self._field_values is None:
            self._field_values = []

        if self.payload is None:
            self.payload = b""

    def __hash__(self):
        return hash(self._field_values)

    def __eq__(self, other):
        return self._field_values == other._field_values

    def __repr__(self):
        args = self.__class__.__name__, self.channel, self._field_values, len(self.payload)
        return "{}({}, {}, {})".format(*args)

    def _emit(self, buff, offset):
        offset, size_offset = buff.skip(offset, 4)

        performative = DescribedValue(self._descriptor, self._field_values)

        offset = buff.pack(offset, 4, "!BBH", 2, 0, self.channel)
        offset = emit_data(buff, offset, performative)
        offset = buff.write(offset, self.payload)

        size = offset - size_offset
        buff.pack(size_offset, 4, "!I", size)

        return offset

def _field_property(index):
    def get(obj):
        try:
            return obj._field_values[index]
        except IndexError:
            return None

    def set_(obj, value):
        try:
            obj._field_values[index] = value
        except IndexError:
            obj._field_values += ([None] * (index - len(obj._field_values))) + [value]

    return property(get, set_)

class OpenFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000010)

    container_id = _field_property(0)
    hostname = _field_property(1)
    max_frame_size = _field_property(2)
    channel_max = _field_property(3)
    idle_timeout = _field_property(4)
    outgoing_locales = _field_property(5)
    incoming_locales = _field_property(6)
    offered_capabilities = _field_property(7)
    desired_capabilities = _field_property(8)
    properties = _field_property(9)

class BeginFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000011)

    remote_channel = _field_property(0)
    next_outgoing_id = _field_property(1)
    incoming_window = _field_property(2)
    outgoing_window = _field_property(3)
    handle_max = _field_property(4)
    offered_capabilities = _field_property(5)
    desired_capabilities = _field_property(6)
    properties = _field_property(7)

class AttachFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000012)

    name = _field_property(0)
    handle = _field_property(1)
    role = _field_property(2)
    snd_settle_mode = _field_property(3)
    rcv_settle_mode = _field_property(4)
    source = _field_property(5)
    target = _field_property(6)
    unsettled = _field_property(7)
    incoming_unsettled = _field_property(8)
    initial_delivery_count = _field_property(9)
    max_message_size = _field_property(10)
    offered_capabilities = _field_property(11)
    desired_capabilities = _field_property(12)
    properties = _field_property(13)

class FlowFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000013)

    next_incomping_id = _field_property(0)
    incoming_window = _field_property(1)
    next_outgoing_id = _field_property(2)
    outgoing_window = _field_property(3)
    handle = _field_property(4)
    delivery_count = _field_property(5)
    link_credit = _field_property(6)
    available = _field_property(7)
    drain = _field_property(8)
    echo = _field_property(9)
    properties = _field_property(10)

class TransferFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000014)

    handle = _field_property(0)
    delivery_id = _field_property(1)
    delivery_tag = _field_property(2)
    message_format = _field_property(3)
    settled = _field_property(4)
    more = _field_property(5)
    rcv_settle_mode = _field_property(6)
    state = _field_property(7)
    resume = _field_property(8)
    aborted = _field_property(9)
    batchable = _field_property(10)

class DispositionFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000015)

    role = _field_property(0)
    first = _field_property(1)
    last = _field_property(2)
    settled = _field_property(3)
    batchable = _field_property(4)

class DetachFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000016)

    handle = _field_property(0)
    closed = _field_property(1)
    error = _field_property(2)

class EndFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000017)

    error = _field_property(0)

class CloseFrame(_Frame):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000018)

    error = _field_property(0)

_frame_classes_by_descriptor = {
    UnsignedLong(0 << 32 | 0x00000010): OpenFrame,
    UnsignedLong(0 << 32 | 0x00000011): BeginFrame,
    UnsignedLong(0 << 32 | 0x00000012): AttachFrame,
    UnsignedLong(0 << 32 | 0x00000013): FlowFrame,
    UnsignedLong(0 << 32 | 0x00000014): TransferFrame,
    UnsignedLong(0 << 32 | 0x00000015): DispositionFrame,
    UnsignedLong(0 << 32 | 0x00000016): DetachFrame,
    UnsignedLong(0 << 32 | 0x00000017): EndFrame,
    UnsignedLong(0 << 32 | 0x00000018): CloseFrame,
}

def emit_frame(buff, offset, frame):
    return frame._emit(buff, offset)

def parse_frame(buff, offset):
    start = offset
    offset, size, channel = parse_frame_header(buff, offset)
    end = start + size

    return parse_frame_body(buff, offset, end, channel)

def parse_frame_header(buff, offset):
    #print("parse_frame_header", _frame_hex(buff[offset:offset + 20]), "...")
    offset, size, _, _, channel = buff.unpack(offset, 8, "!IBBH")
    return offset, size, channel

def parse_frame_body(buff, offset, end, channel):
    #print("parse_frame_body", _data_hex(buff[offset:offset + 20]), "...")

    offset, performative = parse_data(buff, offset)
    offset, payload = buff.read(offset, end - offset)

    assert isinstance(performative, DescribedValue)

    descriptor = performative.descriptor
    values = performative.value

    try:
        frame_class = _frame_classes_by_descriptor[descriptor]
    except KeyError:
        raise Exception("No frame for descriptor 0x{:02X}".format(descriptor))

    return offset, frame_class(channel, values, payload)

def _frame_hex(octets):
    o = _hex(octets)
    args = o[0:8], o[8:12], o[12:16], o[16:18], o[18:22], o[22:24], o[24:]
    return "{} {} {} {} {} {} {}".format(*args)

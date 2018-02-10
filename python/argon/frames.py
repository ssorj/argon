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

from argon.data import *
from argon.data import _data_hex, _field, _hex

OPEN = UnsignedLong(0x00000010)
BEGIN = UnsignedLong(0x00000011)
ATTACH = UnsignedLong(0x00000012)
FLOW = UnsignedLong(0x00000013)
TRANSFER = UnsignedLong(0x00000014)
DISPOSITION = UnsignedLong(0x00000015)
DETACH = UnsignedLong(0x00000016)
END = UnsignedLong(0x00000017)
CLOSE = UnsignedLong(0x00000018)

_performative_names = {
    OPEN: "Open",
    BEGIN: "Begin",
    ATTACH: "Attach",
    FLOW: "Flow",
    TRANSFER: "Transfer",
    DISPOSITION: "Disposition",
    DETACH: "Detach",
    END: "End",
    CLOSE: "Close",
}

class AmqpFrame:
    __slots__ = "channel", "performative", "payload"

    def __init__(self, channel, performative, payload=None):
        self.channel = channel
        self.performative = performative
        self.payload = payload

        # XXX Drop this
        if self.payload is None:
            self.payload = b""

    def __hash__(self):
        return hash(self.channel), hash(self.performative), hash(self.payload)

    def __eq__(self, other):
        return (self.channel, self.performative, self.payload) == \
            (other.channel, other.performative, other.payload)

    def __repr__(self):
        name = _performative_names[self.performative._descriptor]
        args = name, self.channel, self.performative, len(self.payload)
        return "{}({}, {}, {})".format(*args)

    def _emit(self, buff, offset):
        offset, size_offset = buff.skip(offset, 4)

        offset = buff.pack(offset, 4, "!BBH", 2, 0, self.channel)
        offset = emit_data(buff, offset, self.performative)
        offset = buff.write(offset, self.payload)

        size = offset - size_offset
        buff.pack(size_offset, 4, "!I", size)

        return offset

class _Performative(DescribedValue):
    __slots__ = ()

    def __init__(self, descriptor, values=None):
        super().__init__(descriptor, values)

        if self._value is None:
            self._value = list()

class OpenPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(OPEN, values)

    container_id = _field(0)
    hostname = _field(1)
    max_frame_size = _field(2)
    channel_max = _field(3)
    idle_timeout = _field(4)
    outgoing_locales = _field(5)
    incoming_locales = _field(6)
    offered_capabilities = _field(7)
    desired_capabilities = _field(8)
    properties = _field(9)

class BeginPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(BEGIN, values)

    remote_channel = _field(0)
    next_outgoing_id = _field(1)
    incoming_window = _field(2)
    outgoing_window = _field(3)
    handle_max = _field(4)
    offered_capabilities = _field(5)
    desired_capabilities = _field(6)
    properties = _field(7)

class AttachPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(ATTACH, values)

    name = _field(0)
    handle = _field(1)
    role = _field(2)
    snd_settle_mode = _field(3)
    rcv_settle_mode = _field(4)
    source = _field(5)
    target = _field(6)
    unsettled = _field(7)
    incoming_unsettled = _field(8)
    initial_delivery_count = _field(9)
    max_message_size = _field(10)
    offered_capabilities = _field(11)
    desired_capabilities = _field(12)
    properties = _field(13)

class FlowPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(FLOW, values)

    next_incomping_id = _field(0)
    incoming_window = _field(1)
    next_outgoing_id = _field(2)
    outgoing_window = _field(3)
    handle = _field(4)
    delivery_count = _field(5)
    link_credit = _field(6)
    available = _field(7)
    drain = _field(8)
    echo = _field(9)
    properties = _field(10)

class TransferPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(TRANSFER, values)

    handle = _field(0)
    delivery_id = _field(1)
    delivery_tag = _field(2)
    message_format = _field(3)
    settled = _field(4)
    more = _field(5)
    rcv_settle_mode = _field(6)
    state = _field(7)
    resume = _field(8)
    aborted = _field(9)
    batchable = _field(10)

class DispositionPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(DISPOSITION, values)

    role = _field(0)
    first = _field(1)
    last = _field(2)
    settled = _field(3)
    batchable = _field(4)

class DetachPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(DETACH, values)

    handle = _field(0)
    closed = _field(1)
    error = _field(2)

class EndPerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(END, values)

    error = _field(0)

class ClosePerformative(_Performative):
    __slots__ = ()

    def __init__(self, values=None):
        super().__init__(CLOSE, values)

    error = _field(0)

register_value_class(OPEN, OpenPerformative)
register_value_class(BEGIN, BeginPerformative)
register_value_class(ATTACH, AttachPerformative)
register_value_class(FLOW, FlowPerformative)
register_value_class(TRANSFER, TransferPerformative)
register_value_class(DISPOSITION, DispositionPerformative)
register_value_class(DETACH, DetachPerformative)
register_value_class(END, EndPerformative)
register_value_class(CLOSE, ClosePerformative)

def emit_frame(buff, offset, frame):
    return frame._emit(buff, offset)

def parse_frame(buff, offset):
    start = offset
    offset, size, channel = parse_frame_header(buff, offset)
    end = start + size

    return parse_frame_body(buff, offset, end, channel)

def parse_frame_header(buff, offset):
    offset, size, _, _, channel = buff.unpack(offset, 8, "!IBBH")
    return offset, size, channel

def parse_frame_body(buff, offset, end, channel):
    offset, performative = parse_data(buff, offset)
    offset, payload = buff.read(offset, end - offset)

    assert isinstance(performative, _Performative), (type(performative), performative)

    return offset, AmqpFrame(channel, performative, payload)

def _frame_hex(octets):
    o = _hex(octets)
    args = o[0:8], o[8:12], o[12:16], o[16:18], o[18:22], o[22:24], o[24:]
    return "{} {} {} {} {} {} {}".format(*args)

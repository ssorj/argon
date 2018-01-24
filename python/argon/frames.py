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
from argon.common import _namedtuple, _struct
from argon.data import *

class _Frame:
    def __repr__(self):
        return self.__class__.__name__

    def emit(self, buff, offset, channel, fields):
        offset, size_offset = buff.skip(offset, 4)
        offset = buff.pack(offset, 4, "!BBH", 2, 0, channel)
        offset = self.emit_body(buff, offset, fields)

        size = offset - size_offset
        buff.pack(size_offset, 4, "!I", size)

        return offset

    def emit_body(self, buff, offset, fields):
        raise NotImplementedError()

    def parse(self, buff, offset):
        offset, size, _, _, channel = buff.unpack(offset, 8, "!IBBH")
        offset, fields = self.parse_body(buff, offset)

        return offset, channel, fields

    def parse_body(self, buff, offset):
        raise NotImplementedError()

class OpenFrame(_Frame):
    def __init__(self):
        self._performative = ListType(UnsignedLong(0 << 32 | 0x00000010))

    def emit_body(self, buff, offset, fields):
        return self._performative.emit(buff, offset, fields._values)

    def parse_body(self, buff, offset):
        #offset, values = self._performative.parse(buff, offset)
        offset, values, descriptor = parse_data(buff, offset)
        return offset, OpenFrameFields(*values)

class CloseFrame(_Frame):
    def __init__(self):
        self._performative = ListType(UnsignedLong(0 << 32 | 0x00000018))

    def emit_body(self, buff, offset, fields):
        return self._performative.emit(buff, offset, fields._values)

    def parse_body(self, buff, offset):
        #offset, values = self._performative.parse(buff, offset)
        offset, values, descriptor = parse_data(buff, offset)
        return offset, CloseFrameFields(*values)

_open_frame = OpenFrame()
_close_frame = CloseFrame()

_frames_by_performative_code = {
    UnsignedLong(0 << 32 | 0x00000010): _open_frame,
    UnsignedLong(0 << 32 | 0x00000018): _close_frame,
}

def _get_frame_for_performative_code(code):
    try:
        return _frames_by_performative_code[code]
    except KeyError:
        raise Exception("No frame for performative code 0x{:02X}".format(code))

class _FrameFields:
    __slots__ = ("_values",)

    def __init__(self, size, *args, **kwargs):
        self._values = [None] * size

        for i, arg in enumerate(args):
            self._values[i] = arg

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __hash__(self):
        return hash(self._values)

    def __eq__(self, other):
        return self._values == other._values

    def __repr__(self):
        return self._values.__repr__()

def _field(index):
    def get(obj):
        return obj._values[index]

    def set_(obj, value):
        obj._values[index] = value

    return property(get, set_)

class OpenFrameFields(_FrameFields):
    __slots__ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(10, *args, **kwargs)

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

class CloseFrameFields(_FrameFields):
    __slots__ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(1, *args, **kwargs)

    error = _field(0)

def parse_frame(buff, offset):
    offset, size, channel = self._parse_header(buff, offset)
    offset, fields = self.parse_body(buff, offset)

    # size XXX !

    return offset, channel, fields

def _parse_header(buff, offset):
    offset, size, _, _, channel = buff.unpack(offset, 8, "!IBBH")
    return offset, channel

# BeginFields = _namedtuple("BeginFields",
#                           ("remote_channel", "next_outgoing_id", "incoming_window",
#                            "outgoing_window", "handle_max", "offered_capabilities",
#                            "desired_capabilities", "properties"))

# AttachFields = _namedtuple("AttachFields",
#                            ("name", "handle", "role", "snd_settle_mode", "rcv_settle_mode",
#                             "source", "target", "unsettled", "incomplete_unsettled",
#                             "initial_delivery_count", "max_message_size",
#                             "offered_capabilities", "desired_capabilities", "properties"))

# FlowFields = _namedtuple("FlowFields",
#                          ("next_incoming_id", "incoming_window", "next_outgoing_id",
#                           "outgoing_window", "handle", "delivery_count", "link_credit",
#                           "available", "drain", "echo", "properties"))

# TransferFields = _namedtuple("TransferFields",
#                              ("handle", "delivery_id", "delivery_tag", "message_format",
#                               "settled", "more", "rcv_settle_mode", "state",
#                               "resume", "aborted", "batchable"))

# DispositionFields = _namedtuple("DispositionFields",
#                                 ("role", "first", "last", "settled", "batchable"))

# DetachFields = _namedtuple("DetachFields", ("handle", "closed", "error"))

# EndFields = _namedtuple("EndFields", ("error",))

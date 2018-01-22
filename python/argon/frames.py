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

from argon.common import _Buffer, _struct
from argon.data import *

# XXX These don't allow for defaults, but the frame type objects could set them

_Open = _namedtuple("_Open", ("container_id", "hostname", "max_frame_size", "channel_max",
                              "idle_timeout", "outgoing_locales", "incoming_locales",
                              "offered_capabilities", "desired_capabilities", "properties"))

_Begin = _namedtuple("_Begin", ("remote_channel", "next_outgoing_id", "incoming_window",
                                "outgoing_window", "handle_max", "offered_capabilities",
                                "desired_capabilities", "properties"))

_Attach = _namedtuple("_Attach", ("name", "handle", "role", "snd_settle_mode", "rcv_settle_mode",
                                  "source", "target", "unsettled", "incomplete_unsettled",
                                  "initial_delivery_count", "max_message_size",
                                  "offered_capabilities", "desired_capabilities", "properties"))

_Flow = _namedtuple("_Flow", ("next_incoming_id", "incoming_window", "next_outgoing_id",
                              "outgoing_window", "handle", "delivery_count", "link_credit",
                              "available", "drain", "echo", "properties"))

_Transfer = _namedtyple("_Transfer", ("handle", "delivery_id", "delivery_tag", "message_format",
                                      "settled", "more", "rcv_settle_mode", "state",
                                      "resume", "aborted", "batchable"))

_Disposition = _namedtuple("Disposition", ("role", "first", "last", "settled", "batchable"))
_Detach = _namedtuple("_Detach", ("handle", "closed", "error"))
_End = _namedtuple("_End", ("error",))
_Close = _namedtuple("_Close", ("error",))

class _Frame:
    def emit(self, buff, offset, channel, *args, **kwargs):
        size_offset = offset
        offset += 4

        offset = buff.pack(offset, 4, "!BBH", 2, 0, channel)
        offset = self.emit_body(buff, offset, channel, *args, **kwargs)

        size = offset - size_offset
        buff.pack(size_offset, 4, "!I", size)

        return offset

    def emit_body(self, buff, offset, channel, *args, **kwargs):
        raise NotImplementedError()

    def parse(self, buff, offset):
        pass

class OpenFrame(_Frame):
    def __init__(self):
        self.performative = ListType(UnsignedLong(0 << 32 | 0x00000010))

    def emit_body(self, buff, offset, channel, container_id, hostname=None):
        fields = [
            container_id,
            hostname,
        ]

        return self.performative.emit(buff, offset, fields)

class CloseFrame(_Frame):
    def __init__(self):
        self.performative = ListType(UnsignedLong(0 << 32 | 0x00000018))

    def emit_body(self, buff, offset, channel):
        return self.performative.emit(buff, offset, [])

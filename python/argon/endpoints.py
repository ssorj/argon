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

from argon.common import _hex, _uuid_bytes
from argon.frames import _field
from argon.transport import *

class Connection:
    def __init__(self, container_id=None):
        self.transport = None

        if container_id is None:
            container_id = _hex(_uuid_bytes())

        self._open = OpenPerformative()
        self._open.container_id = container_id

        self._close = ClosePerformative()

        self._opened = False
        self._closed = False

        self._channel_ids = _Sequence()

        self.sessions = list()
        self.sessions_by_channel = dict()

    @property
    def container_id(self):
        return self._open.container_id

    def bind(self, transport):
        self.transport = transport
        self.transport.on_start = self.on_transport_start
        self.transport.on_frame = self.on_transport_frame
        self.transport.on_stop = self.on_transport_stop

    def on_transport_start(self):
        self.on_start()

    def on_transport_frame(self, frame):
        assert isinstance(frame, AmqpFrame)

        descriptor = frame.performative._descriptor

        if descriptor == OPEN_DESCRIPTOR:
            assert self._opened is False and self._closed is False

            self._opened = True
            self.on_open()
            return

        if descriptor == CLOSE_DESCRIPTOR:
            assert self._opened is True and self._closed is False

            self._closed = True
            self.on_close()
            return

        session = self.sessions_by_channel[frame.channel]

        if descriptor == BEGIN_DESCRIPTOR:
            session._handle_begin(frame)
            return

        if descriptor == ATTACH_DESCRIPTOR:
            link = session.links_by_name[frame.performative.name]
            link._handle_attach(frame)
            return

        if descriptor == FLOW_DESCRIPTOR:
            if frame.performative.handle is None:
                return # XXX Handle flow for sessions

            link = session.links_by_handle[frame.performative.handle]
            link._handle_flow(frame)
            return

        if descriptor == TRANSFER_DESCRIPTOR:
            return # XXX Only sending for now

        if descriptor == DISPOSITION_DESCRIPTOR:
            return # XXX All presettled for now

        if descriptor == DETACH_DESCRIPTOR:
            link = session.links_by_handle[frame.performative.handle]
            link._handle_detach(frame)
            return

        if descriptor == END_DESCRIPTOR:
            session._handle_end(frame)
            return

        raise Exception()

    def on_transport_stop(self, error):
        raise Exception(error) # XXX

    def on_start(self):
        pass

    def open(self):
        self.transport.emit_amqp_frame(0, self._open)

    def on_open(self):
        pass

    def close(self, error=None):
        # self._close.error = ...
        self.transport.emit_amqp_frame(0, self._close)

    def on_close(self):
        pass

    def on_stop(self):
        pass

class _Endpoint:
    def __init__(self, connection, channel):
        self.connection = connection
        self.channel = channel

        self._opened = False
        self._closed = False

    @property
    def transport(self):
        return self.connection.transport

    def open(self):
        raise NotImplementedError()

    def on_open(self):
        pass

    def close(self, error=None):
        raise NotImplementedError()

    def on_close(self):
        pass

class Session(_Endpoint):
    def __init__(self, connection):
        super().__init__(connection, connection._channel_ids.next())

        self._begin = BeginPerformative()
        self._begin.next_outgoing_id = UnsignedInt(0)
        self._begin.incoming_window = UnsignedInt(0xffffffff)
        self._begin.outgoing_window = UnsignedInt(0xffffffff)

        self._end = EndPerformative()

        self._link_handles = _Sequence()

        self.links_by_name = dict()
        self.links_by_handle = dict()

        self.connection.sessions.append(self)
        self.connection.sessions_by_channel[self.channel] = self

    def open(self):
        self.transport.emit_amqp_frame(self.channel, self._begin)

    def _handle_begin(self, frame):
        self._remote_channel = frame.performative.remote_channel
        self.on_open()

    def close(self, error=None):
        # self._end.error = ...
        self.transport.emit_amqp_frame(self.channel, self._end)

    def _handle_end(self, frame):
        self.on_close()

class _Link(_Endpoint):
    def __init__(self, session, role, name=None):
        super().__init__(session.connection, session.channel)

        self.session = session

        handle = UnsignedInt(self.session._link_handles.next())

        if name is None:
            name = "{}-{}".format(self.connection.container_id, handle)

        self._attach = AttachPerformative()
        self._attach.name = name
        self._attach.handle = handle
        self._attach.role = role
        self._attach.snd_settle_mode = UnsignedByte(1) # XXX Presettled
        self._attach.source = None
        self._attach.target = None

        self._detach = DetachPerformative()
        self._detach.handle = handle
        self._detach.closed = True

        self._delivery_ids = _Sequence()

        self.session.links_by_name[self._attach.name] = self
        self.session.links_by_handle[self._attach.handle] = self

    def open(self):
        self.transport.emit_amqp_frame(self.channel, self._attach)

    def _handle_attach(self, frame):
        self.on_open()

    def _handle_flow(self, frame):
        self.credit = frame.performative.link_credit
        self.on_flow()

    def on_flow(self):
        pass

    def send(self, message):
        performative = TransferPerformative()
        performative.handle = self._attach.handle
        performative.delivery_id = UnsignedInt(self._delivery_ids.next())
        performative.settled = True

        tag = "delivery-{}".format(performative.delivery_id).encode("ascii")
        performative.delivery_tag = tag

        self.transport.emit_amqp_frame(self.channel, performative, None, message)

    def close(self, error=None):
        # self._detach.error = ...
        self.transport.emit_amqp_frame(self.channel, self._detach)

    def _handle_detach(self, frame):
        self.on_close()

class Sender(_Link):
    def __init__(self, session, address, name=None):
        super().__init__(session, False, name)

        self._attach.target = Target()
        self._attach.target.address = address

class Receiver(_Link):
    def __init__(self, session, address, name=None):
        super().__init__(session, True, name)

        self._attach.source = Source()
        self._attach.source.address = address

_SOURCE_DESCRIPTOR = UnsignedLong(0x00000028)
_TARGET_DESCRIPTOR = UnsignedLong(0x00000029)

class _Terminus(DescribedValue):
    def __init__(self, descriptor, values):
        super().__init__(descriptor, values)

        if self._value is None:
            self._value = list()

    address = _field(0)
    durable = _field(1)
    expiry_policy = _field(2)
    timeout = _field(3)
    dynamic = _field(4)
    dynamic_node_properties = _field(5)

class Source(_Terminus):
    def __init__(self, values=None):
        super().__init__(_SOURCE_DESCRIPTOR, values)

    distribution_mode = _field(6)
    filter = _field(7)
    default_outcome = _field(8)
    outcomes = _field(9)
    capabilities = _field(10)

class Target(_Terminus):
    def __init__(self, values=None):
        super().__init__(_TARGET_DESCRIPTOR, values)

    capabilities = _field(6)

register_value_class(_SOURCE_DESCRIPTOR, Source)
register_value_class(_TARGET_DESCRIPTOR, Target)

class _Sequence:
    __slots__ = ("value",)

    def __init__(self):
        self.value = -1 # XXX Things break when this is 0

    def next(self):
        self.value += 1
        return self.value

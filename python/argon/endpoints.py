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
from argon.io import *
from argon.frames import _field

class Connection(TcpConnection):
    def __init__(self, host, port, container_id=None):
        super().__init__(host, port)

        if container_id is None:
            container_id = _hex(_uuid_bytes())

        self._open = OpenPerformative()
        self._open.container_id = container_id

        self._opened = False
        self._closed = False

        self._channel_ids = _Sequence()

        self.sessions = list()
        self.sessions_by_channel = dict()

    @property
    def container_id(self):
        return self._open.container_id

    def on_start(self):
        self.send_open()

    def on_frame(self, frame):
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
            session._receive_open(frame)
            return

        if descriptor == ATTACH_DESCRIPTOR:
            link = session.links_by_name[frame.performative.name]
            link._receive_open(frame)
            return

        if descriptor == FLOW_DESCRIPTOR:
            if frame.performative.handle is None:
                return # XXX Handle flow for sessions

            link = session.links_by_handle[frame.performative.handle]
            link._receive_flow(frame)
            return

        if descriptor == TRANSFER_DESCRIPTOR:
            return # XXX Only sending for now

        if descriptor == DISPOSITION_DESCRIPTOR:
            return # XXX All presettled for now

        if descriptor == DETACH_DESCRIPTOR:
            link = session.links_by_handle[frame.performative.handle]
            link._receive_close(frame)
            return

        if descriptor == END_DESCRIPTOR:
            session._receive_close(frame)
            return

        raise Exception()

    def send_open(self):
        self.enqueue_output(AmqpFrame(0, self._open))

    def on_open(self):
        pass

    def send_close(self, error=None):
        performative = ClosePerformative()
        self.enqueue_output(AmqpFrame(0, performative))

    def on_close(self):
        pass

class _Endpoint:
    def __init__(self, connection, channel):
        self.connection = connection
        self.channel = channel

        self._opened = False
        self._closed = False

    def send_open(self):
        raise NotImplementedError()

    def _receive_open(self, frame):
        raise NotImplementedError()

    def on_open(self):
        pass

    def send_close(self, error=None):
        raise NotImplementedError()

    def _receive_close(self, frame):
        self.on_close()

class Session(_Endpoint):
    def __init__(self, connection):
        super().__init__(connection, connection._channel_ids.next())

        self._begin = BeginPerformative()
        self._begin.next_outgoing_id = UnsignedInt(0)
        self._begin.incoming_window = UnsignedInt(0xffffffff)
        self._begin.outgoing_window = UnsignedInt(0xffffffff)

        self._link_handles = _Sequence()

        self.links = list()
        self.links_by_name = dict()
        self.links_by_handle = dict()

        self.connection.sessions.append(self)
        self.connection.sessions_by_channel[self.channel] = self

    def send_open(self):
        self.connection.enqueue_output(AmqpFrame(self.channel, self._begin))

    def _receive_open(self, frame):
        self._remote_channel = frame.performative.remote_channel
        self.on_open()

    def send_close(self, error=None):
        performative = EndPerformative()
        self.connection.enqueue_output(AmqpFrame(self.channel, performative))

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

        self._delivery_ids = _Sequence()

        self.session.links.append(self)
        self.session.links_by_name[self._attach.name] = self
        self.session.links_by_handle[self._attach.handle] = self

    def send_open(self):
        self.connection.enqueue_output(AmqpFrame(self.channel, self._attach))

    def _receive_open(self, frame):
        self.on_open()

    def _receive_flow(self, frame):
        self.credit = frame.performative.link_credit
        self.on_flow()

    def on_flow(self):
        pass

    def send_transfer(self, message):
        performative = TransferPerformative()
        performative.handle = self._attach.handle
        performative.delivery_id = UnsignedInt(self._delivery_ids.next())
        performative.settled = True

        tag = "delivery-{}".format(performative.delivery_id).encode("ascii")
        performative.delivery_tag = tag

        self.connection.enqueue_output(AmqpFrame(self.channel, performative, None, message))

    def send_close(self, error=None):
        performative = DetachPerformative()
        performative.handle = self._attach.handle
        performative.closed = True

        self.connection.enqueue_output(AmqpFrame(self.channel, performative))

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

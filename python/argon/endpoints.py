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

from argon.io import *
from argon.frames import _field

class Connection(TcpConnection):
    def __init__(self, host, port, container_id):
        super().__init__(host, port)

        self.container_id = container_id

        self._opened = False
        self._closed = False

        # XXX self._incoming_channel_ids = set()

        self.sessions = list()
        self.sessions_by_channel = dict()

    def on_start(self):
        self.send_open()

    def on_frame(self, frame):
        assert isinstance(frame, AmqpFrame)

        type_ = type(frame.performative)

        if type_ is OpenPerformative:
            assert self._opened is False and self._closed is False

            self._opened = True
            self.on_open()

            return

        if type_ is ClosePerformative:
            assert self._opened is True and self._closed is False

            self._closed = True
            self.on_close()

            return

        session = self.sessions_by_channel[frame.channel]

        if type_ is BeginPerformative:
            session._receive_open(frame)
            return

        if type_ is AttachPerformative:
            link = session.links_by_name[frame.performative.name]
            link._receive_open(frame)
            return

        if type_ is FlowPerformative:
            if frame.performative.handle is None:
                return # XXX Handle flow for sessions

            link = session.links_by_handle[frame.performative.handle]
            link._receive_flow(frame)
            return

        if type_ is TransferPerformative:
            return # XXX Only sending for now

        if type_ is DispositionPerformative:
            return # XXX All presettled for now

        if type_ is DetachPerformative:
            link = session.links_by_handle[frame.performative.handle]
            link._receive_close(frame)
            return

        if type_ is EndPerformative:
            session._receive_close(frame)
            return

        raise Exception()

    def send_open(self):
        frame = AmqpFrame(0, OpenPerformative())
        frame.performative.container_id = self.container_id

        self.send_frame(frame)

    def on_open(self):
        pass

    def send_close(self, error=None):
        frame = AmqpFrame(0, ClosePerformative())
        self.send_frame(frame)

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
    def __init__(self, connection, channel):
        super().__init__(connection, channel)

        self._remote_channel = None
        self._next_outgoing_id = 0
        self._incoming_window = 0xffff
        self._outgoing_window = 0xffff

        self._link_handles = _Sequence()

        self.links = list()
        self.links_by_name = dict()
        self.links_by_handle = dict()

        self.connection.sessions.append(self)
        self.connection.sessions_by_channel[self.channel] = self

    def send_open(self):
        frame = AmqpFrame(self.channel, BeginPerformative())
        frame.performative.next_outgoing_id = UnsignedInt(self._next_outgoing_id)
        frame.performative.incoming_window = UnsignedInt(self._incoming_window)
        frame.performative.outgoing_window = UnsignedInt(self._outgoing_window)

        self.connection.send_frame(frame)

    def _receive_open(self, frame):
        self._remote_channel = frame.performative.remote_channel
        self.on_open()

    def send_close(self, error=None):
        frame = AmqpFrame(self.channel, EndPerformative())
        self.connection.send_frame(frame)

class Link(_Endpoint):
    def __init__(self, session, name=None):
        super().__init__(session.connection, session.channel)

        self.session = session

        self._name = name
        self._handle = self.session._link_handles.next()
        self._role = False # Sender

        if self._name is None:
            self._name = "{}-{}".format(self.connection.container_id, self._handle)

        self._delivery_ids = _Sequence()

        self.session.links.append(self)
        self.session.links_by_name[self._name] = self
        self.session.links_by_handle[self._handle] = self

    def send_open(self, target=None):
        frame = AmqpFrame(self.channel, AttachPerformative())
        frame.performative.name = self._name
        frame.performative.handle = UnsignedInt(self._handle)
        frame.performative.role = self._role
        frame.performative.snd_settle_mode = UnsignedByte(1) # XXX Presettled

        frame.performative.target = target

        self.connection.send_frame(frame)

    def _receive_open(self, frame):
        self.on_open()

    def _receive_flow(self, frame):
        self.credit = frame.performative.link_credit
        self.on_flow()

    def on_flow(self):
        pass

    def send_transfer(self, payload):
        frame = AmqpFrame(self.channel, TransferPerformative(), payload)
        frame.performative.handle = self._handle
        frame.performative.delivery_id = UnsignedInt(self._delivery_ids.next())
        frame.performative.message_format = UnsignedInt(0)
        frame.performative.settled = True

        tag = "delivery-{}".format(frame.performative.delivery_id).encode("ascii") # XXX
        frame.performative.delivery_tag = tag

        self.connection.send_frame(frame)

    def send_close(self, error=None):
        frame = AmqpFrame(self.channel, DetachPerformative())
        frame.performative.handle = self._handle
        frame.performative.closed = True

        self.connection.send_frame(frame)

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
        super().__init__(UnsignedLong(0 << 32 | 0x00000028), values)

    distribution_mode = _field(6)
    filter = _field(7)
    default_outcome = _field(8)
    outcomes = _field(9)
    capabilities = _field(10)

class Target(_Terminus):
    def __init__(self, values=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000029), values)

    capabilities = _field(6)

class _Sequence:
    __slots__ = ("value",)

    def __init__(self):
        self.value = -1 # XXX Things break when this is 0

    def next(self):
        self.value += 1
        return self.value

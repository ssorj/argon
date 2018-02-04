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
        if isinstance(frame, OpenFrame):
            assert self._opened is False and self._closed is False

            self._opened = True
            self.on_open()

            return

        if isinstance(frame, BeginFrame):
            session = self.sessions_by_channel[frame.channel]

            session._receive_open(frame)

            return

        if isinstance(frame, AttachFrame):
            session = self.sessions_by_channel[frame.channel]
            link = session.links_by_name[frame.name]

            link._receive_open(frame)

            return

        if isinstance(frame, FlowFrame):
            return # XXX

        if isinstance(frame, DetachFrame):
            session = self.sessions_by_channel[frame.channel]
            link = session.links_by_name[frame.name]

            link._receive_close(frame)

            return

        if isinstance(frame, EndFrame):
            session = self.sessions_by_channel[frame.channel]

            session._receive_close(frame)

            return

        if isinstance(frame, CloseFrame):
            assert self._opened is True and self._closed is False

            self._closed = True
            self.on_close()

            return

        raise Exception()

    def send_open(self):
        frame = OpenFrame(0)
        frame.container_id = self.container_id

        self.send_frame(frame)

    def on_open(self):
        pass

    def send_close(self, error=None):
        frame = CloseFrame(0)
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

        self.links = list()
        self.links_by_name = dict()

        self.connection.sessions.append(self)
        self.connection.sessions_by_channel[self.channel] = self

    def send_open(self):
        frame = BeginFrame(self.channel)
        frame.next_outgoing_id = UnsignedInt(self._next_outgoing_id)
        frame.incoming_window = UnsignedInt(self._incoming_window)
        frame.outgoing_window = UnsignedInt(self._outgoing_window)

        self.connection.send_frame(frame)

    def _receive_open(self, frame):
        self._remote_channel = frame.remote_channel
        self.on_open()

    def send_close(self, error=None):
        frame = EndFrame(self.channel)
        self.connection.send_frame(frame)

class Link(_Endpoint):
    def __init__(self, session):
        super().__init__(session.connection, session.channel)

        self.session = session

        self._name = "hello"
        self._handle = 0
        self._role = False # Sender

        self.session.links.append(self)
        self.session.links_by_name[self._name] = self

    def send_open(self):
        frame = AttachFrame(self.channel)
        frame.name = self._name
        frame.handle = UnsignedInt(self._handle)
        frame.role = False

        self.connection.send_frame(frame)

    def _receive_open(self, frame):
        self.on_open()

    def send_close(self, error=None):
        frame = DetachFrame(self.channel)
        self.connection.send_frame(frame)

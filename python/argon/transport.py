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

from argon.common import *
from argon.common import _micropython, _time, _select, _socket, _struct
from argon.frames import *
from argon.frames import _frame_hex, _hex

class SocketTransport:
    def __init__(self, socket, address):
        self.socket = socket
        self.address = address
        self.debug = True

        self._input_buffer = Buffer()
        self._output_buffer = Buffer()
        self._emit_offset = 0

    def _log_output(self, octets, frame, message=None):
        if self.debug:
            print("S", octets)
            print(" ", frame, message)

    def _log_input(self, octets, frame):
        if self.debug:
            print("R", octets)
            print(" ", frame)

    def run(self):
        read_offset = 0
        parse_offset = 0
        write_offset = 0

        try:
            self.socket.connect(self.address)

            self._shake_hands()

            self.socket.setblocking(False)

            self.on_start()

            poller = _select.poll()
            poller.register(self.socket)

            while True:
                events = poller.poll(1000)
                flags = events[0][1]

                #_time.sleep(0.2)
                #print("tick", "r", read_offset, "p", parse_offset, "e", self._emit_offset, "w", write_offset)
                #print("    ", flags & _select.POLLIN, flags & _select.POLLOUT)

                if flags & _select.POLLERR:
                    raise Exception("POLLERR!")

                if flags & _select.POLLHUP:
                    raise Exception("POLLHUP!")

                if flags & _select.POLLIN:
                    read_offset = self._read_socket(read_offset)

                parse_offset = self._parse_frames(parse_offset, read_offset)

                if write_offset < self._emit_offset and flags & _select.POLLOUT:
                    write_offset = self._write_socket(write_offset, self._emit_offset)

                if parse_offset == read_offset:
                    read_offset = 0
                    parse_offset = 0

                if self._emit_offset == write_offset:
                    self._emit_offset = 0
                    write_offset = 0

            self.on_stop()
        finally:
            self.socket.close()

    def on_start(self):
        pass

    def on_frame(self, frame):
        pass

    def on_stop(self, error):
        pass

    def emit_amqp_frame(self, channel, performative, payload=None, message=None):
        start = self._emit_offset
        offset = emit_amqp_frame(self._output_buffer, start, channel, performative, payload, message)

        if self.debug:
            frame = AmqpFrame(channel, performative, payload)
            self._log_output(_frame_hex(self._output_buffer[start:offset]), frame, message)

        self._emit_offset = offset

    def _shake_hands(self):
        protocol_header = _struct.pack("!4sBBBB", b"AMQP", 0, 1, 0, 0)

        if self.debug:
            print("S", _hex(protocol_header))
            print(" ", str(protocol_header))

        if _micropython:
            self.socket.write(protocol_header)
            response = self.socket.read(8)
        else:
            self.socket.sendall(protocol_header)
            response = self.socket.recv(8, _socket.MSG_WAITALL)

        if self.debug:
            print("R", _hex(response))
            print(" ", str(response))

        assert response == protocol_header

    if _micropython:
        def _read_socket(self, offset):
            start = offset
            self._input_buffer.ensure(offset + 64)
            return offset + self.socket.readinto(self._input_buffer[offset:], 64)

        def _write_socket(self, write_offset, emit_offset):
            octets = bytes(self._output_buffer[write_offset:emit_offset])
            return write_offset + self.socket.send(octets)
    else:
        def _read_socket(self, offset):
            start = offset
            self._input_buffer.ensure(offset + 64)
            return offset + self.socket.recv_into(self._input_buffer[offset:], 64)

        def _write_socket(self, write_offset, emit_offset):
            octets = self._output_buffer[write_offset:emit_offset]
            return write_offset + self.socket.send(octets)

    def _parse_frames(self, offset, limit):
        while offset < limit:
            start = offset

            if offset + 8 > limit:
                return start

            offset, size, channel = parse_frame_header(self._input_buffer, offset)
            end = start + size

            if end > limit:
                return start

            offset, frame = parse_frame_body(self._input_buffer, offset, end, channel)

            self._log_input(_frame_hex(self._input_buffer[start:offset]), frame)

            self.on_frame(frame)

        return offset

class TcpTransport(SocketTransport):
    def __init__(self, host, port):
        self.host = host
        self.port = port

        socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        address = _socket.getaddrinfo(self.host, self.port)[0][-1]

        super().__init__(socket, address)

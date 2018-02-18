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

class TcpConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.debug = True

        self._output_frames = list()

    def _log_send(self, octets, obj):
        if self.debug:
            print("S", octets)
            print(" ", obj)

    def _log_receive(self, octets, obj):
        if self.debug:
            print("R", octets)
            print(" ", obj)

    def run(self):
        address = _socket.getaddrinfo(self.host, self.port)[0][-1]
        sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)

        input_buff = Buffer()
        read_offset = 0
        parse_offset = 0

        output_buff = Buffer()
        emit_offset = 0
        write_offset = 0

        self.on_start()

        try:
            sock.connect(address)

            self._shake_hands(sock)

            sock.setblocking(False)

            poller = _select.poll()
            poller.register(sock)

            while True:
                events = poller.poll(1000)
                flags = events[0][1]

                if flags & _select.POLLERR:
                    raise Exception("POLLERR!")

                if flags & _select.POLLHUP:
                    raise Exception("POLLHUP!")

                if flags & _select.POLLIN:
                    read_offset = self._read_socket(input_buff, read_offset, sock)

                parse_offset = self._parse_frames(input_buff, parse_offset, read_offset)
                emit_offset = self._emit_frames(output_buff, emit_offset)

                if write_offset < emit_offset and flags & _select.POLLOUT:
                    write_offset = self._write_socket(output_buff, write_offset, emit_offset, sock)

                if parse_offset == read_offset:
                    input_buff.reset()
                    read_offset = 0
                    parse_offset = 0

                if emit_offset == write_offset:
                    output_buff.reset()
                    emit_offset = 0
                    write_offset = 0
        finally:
            sock.close()

        self.on_stop()

    def on_start(self):
        pass

    def on_frame(self, frame):
        pass

    def on_stop(self):
        pass

    def send_frame(self, frame):
        self._output_frames.append(frame)

    def _shake_hands(self, sock):
        protocol_header = _struct.pack("!4sBBBB", b"AMQP", 0, 1, 0, 0)

        self._log_send(_hex(protocol_header), str(protocol_header))

        if _micropython:
            sock.write(protocol_header)
            response = sock.read(8)
        else:
            sock.sendall(protocol_header)
            response = sock.recv(8, _socket.MSG_WAITALL)

        self._log_receive(_hex(response), str(response))

        assert response == protocol_header

    if _micropython:
        def _read_socket(self, buff, offset, sock):
            start = offset
            buff.ensure(offset + 32)
            return offset + sock.readinto(buff[offset:], 32)

        def _write_socket(self, buff, write_offset, emit_offset, sock):
            return write_offset + sock.send(bytes(buff[write_offset:emit_offset]))
    else:
        def _read_socket(self, buff, offset, sock):
            start = offset
            buff.ensure(offset + 32)
            return offset + sock.recv_into(buff[offset:], 32)

        def _write_socket(self, buff, write_offset, emit_offset, sock):
            return write_offset + sock.send(buff[write_offset:emit_offset])

    def _parse_frames(self, buff, offset, limit):
        while offset < limit:
            start = offset

            if offset + 8 > limit:
                return start

            offset, size, channel = parse_frame_header(buff, offset)
            end = start + size

            if end > limit:
                return start

            offset, frame = parse_frame_body(buff, offset, end, channel)

            self._log_receive(_frame_hex(buff[start:offset]), frame)

            self.on_frame(frame)

        return offset

    def _emit_frames(self, buff, offset):
        while len(self._output_frames) > 0:
            frame = self._output_frames.pop(0)

            start = offset
            offset = emit_frame(buff, offset, frame)

            self._log_send(_frame_hex(buff[start:offset]), frame)

        return offset

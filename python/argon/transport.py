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
from argon.common import _hex, _micropython, _time, _select, _socket, _struct
from argon.frames import *
from argon.frames import _frame_hex

def _log_send(octets, obj):
    print("S", octets)
    print(" ", obj)

def _log_receive(octets, obj):
    print("R", octets)
    print(" ", obj)

def _shake_hands(sock):
    protocol_header = _struct.pack("!4sBBBB", b"AMQP", 0, 1, 0, 0)

    _log_send(_hex(protocol_header), str(protocol_header))

    if _micropython:
        sock.write(protocol_header)
        response = sock.read(8)
    else:
        sock.sendall(protocol_header)
        response = sock.recv(8, _socket.MSG_WAITALL)

    _log_receive(_hex(response), str(response))

    assert response == protocol_header

def connect_and_run(host, port, input_frames, output_frames):
    address = _socket.getaddrinfo(host, port)[0][-1]

    input_buff = Buffer()
    read_offset = 0
    parse_offset = 0

    output_buff = Buffer()
    emit_offset = 0
    write_offset = 0

    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)

    try:
        sock.connect(address)

        _shake_hands(sock)

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
                read_offset = _read_socket(input_buff, read_offset, sock)

            parse_offset = _parse_frames(input_buff, parse_offset, read_offset)
            emit_offset = _emit_frames(output_buff, emit_offset, output_frames)

            if write_offset < emit_offset and flags & _select.POLLOUT:
                write_offset = _write_socket(output_buff, write_offset, emit_offset, sock)
    finally:
        sock.close()

def _read_socket(buff, offset, sock):
    #print("_read_socket")

    start = offset

    buff.ensure(offset + 1024)

    if _micropython:
        offset = offset + sock.readinto(buff[offset:], 1024)
    else:
        offset = offset + sock.recv_into(buff[offset:], 1024)

    return offset

def _write_socket(buff, write_offset, emit_offset, sock):
    #print("_write_socket")
    return write_offset + sock.send(buff[write_offset:emit_offset])

def _parse_frames(buff, offset, limit):
    #print("_parse_frames")

    while offset < limit:
        start = offset

        if offset + 8 > limit:
            return start

        offset, size, channel = parse_frame_header(buff, offset)

        if start + size > limit:
            return start

        offset, frame = parse_frame_body(buff, offset, channel)

        _log_receive(_frame_hex(buff[start:offset]), frame)

        if isinstance(frame, CloseFrame):
            print("SUCCESS")
            raise KeyboardInterrupt()

    return offset

def _emit_frames(buff, offset, output_frames):
    #print("_emit_frames")

    while len(output_frames) > 0:
        frame = output_frames.pop(0)

        start = offset
        offset = emit_frame(buff, offset, frame)

        _log_send(_frame_hex(buff[start:offset]), frame)

    return offset

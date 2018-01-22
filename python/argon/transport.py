import socket as _socket
import sys as _sys

from argon.common import *
from argon.common import _hex, _time, _struct
from argon.frames import *

def send_protocol_header(buff, offset, sock):
    start = offset
    offset = buff.pack(offset, 8, "!4sBBBB", b"AMQP", 0, 1, 0, 0)

    return buff.send(start, 8, sock)

def recv_protocol_header(buff, offset, sock):
    return buff.recv(offset, 8, sock)

def _main():
    open_frame = OpenFrameType()
    close_frame = CloseFrameType()

    container_id = "test"

    address = "localhost", 5672
    channel = 1

    input_buff = _Buffer()
    input_offset = 0

    output_buff = _Buffer()
    output_offset = 0

    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
        sock.connect(address)

        output_offset = send_protocol_header(output_buff, output_offset)
        input_offset, data = recv_protocol_header(input_buff, input_offset)

        print("H", _hex(data))

        start = output_offset
        output_offset = open_frame.emit(output_buff, output_offset, channel, container_id)
        size = output_offset - start

        data = output_buff[start:output_offset]
        print("S", _hex(data))

        output_buff.send(start, size, sock)

        start = input_offset
        input_offset += sock.recv_into(input_buff, 2048)

        data = input_buff[start:input_offset]
        print("R", _hex(data))

        # offset = close_frame.emit(buff, offset, channel)

        # data = buff[start:offset]

        # print("S", _hex(data))
        # sock.sendall(data)

        # data = sock.recv(2048)
        # print("R", _hex(data))

        print("SUCCESS")

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

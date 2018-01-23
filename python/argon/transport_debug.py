import socket as _socket
import sys as _sys

from argon.common import *
from argon.common import _hex, _time, _struct
from argon.frames import *

def _log_send(data):
    print("S", _hex(data), data)

def _log_receive(data):
    print("R", _hex(data), data)

def _shake_hands(sock):
    protocol_header = _struct.pack("!4sBBBB", b"AMQP", 0, 1, 0, 0)

    _log_send(protocol_header)

    sock.sendall(protocol_header)

    response = sock.recv(8, _socket.MSG_WAITALL)

    _log_receive(response)

    assert response == protocol_header

def _main():
    open_frame = OpenFrame()
    close_frame = CloseFrame()

    container_id = "test"

    address = "localhost", 5672
    channel = 1

    input_buff = Buffer()
    input_offset = 0

    output_buff = Buffer()
    output_offset = 0

    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
        sock.connect(address)

        _shake_hands(sock)

        for i in range(2):
            input_start = input_offset
            output_start = output_offset

            output_offset = open_frame.emit(output_buff, output_offset, channel, container_id)
            output_offset = close_frame.emit(output_buff, output_offset, channel)

            output_data = output_buff[output_start:output_offset]

            _log_send(output_data)

            sock.sendall(output_data)

            input_buff.ensure(4096)
            input_offset += sock.recv_into(input_buff.view, 4096)
            input_data = input_buff[input_start:input_offset]

            _log_receive(input_data)

            if i == 1:
                print("SUCCESS")
                return


if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

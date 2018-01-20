import socket as _socket
import struct as _struct
import sys as _sys
import time as _time

from argon.common import _hex, _Buffer
from argon.frames import _AmqpFrame

def _main():
    address = "localhost", 5672

    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
        sock.connect(address)

        protocol_header = _struct.pack("!4sBBBB", b"AMQP", 0, 1, 0, 0)
        
        sock.sendall(protocol_header)

        response = sock.recv(8 * 8)

        assert response == protocol_header

        frame = _AmqpFrame(None)

        buff = _Buffer()
        offset = 0

        offset = frame.emit(buff, offset)

        open_ = buff[:offset]
        
        sock.sendall(open_)

        print(111, _hex(open_))
        
        data = sock.recv(2048)
        
        print(_hex(data))
        
        #size = _struct.unpack_from("!I")
        
        #sock.sendall(XXX)

        # data = sock.recv(...)
        
        print("SUCCESS")
    
if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

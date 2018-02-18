import socket, sys

HOST = 'localhost'        # The remote host
PORT = 50007              # The same port as used by the server

read_buff = bytearray(16)
read_view = memoryview(read_buff)

size = 0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

while True:
    nbytes = s.recv_into(read_view)

    if not nbytes:
        break

    size += nbytes
    read_view = read_view[nbytes:]

    if not len(read_view):
        print "filled a chunk", read_buff
        read_view = memoryview(read_buff)

print("End of data", read_buff[:len(read_view)], size)

s.close()

---

class _Integer:
    def __init__(self, value):
        self._value = value

    def __hash__(self):
        return hash(self._value)

    def __complex__(self):
        return complex(self._value)

    def __int__(self):
        return int(self._value)

    def __float__(self):
        return float(self._value)

    def __round__(self, n=None):
        return round(self._value, n)

    def __index__(self):
        return self._value

    def __lt__(self, other):
        return self._value < other

    def __le__(self, other):
        return self._value <= other

    def __eq__(self, other):
        return self._value == other

    def __gt__(self, other):
        return self._value < other

    def __ge__(self, other):
        return self._value >= other

    def __add__(self, other):
        return self._value + other

    def __sub__(self, other):
        return self._value - other

    def __mul__(self, other):
        return self._value * other

    def __truediv__(self, other):
        return self._value / other

    def __floordiv__(self, other):
        return self._value // other

    def __mod__(self, other):
        return self._value % other

    def __divmod__(self, other):
        return divmod(self._value % other)

    def __pow__(self, other, modulo=None):
        return pow(self._value, other, modulo)

    def __lshift__(self, other):
        return self._value << other

    def __rshift__(self, other):
        return self._value >> other

    def __and__(self, other):
        return self._value & other

    def __xor__(self, other):
        return self._value ^ other

    def __or__(self, other):
        return self._value | other

    def __neg__(self):
        return -self._value

    def __pos__(self):
        return +self._value

    def __abs__(self):
        return abs(self._value)

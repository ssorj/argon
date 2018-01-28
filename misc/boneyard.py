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

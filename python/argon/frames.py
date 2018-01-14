import struct as _struct

from .datatypes import *

class _AmqpFrame:
    def __init__(self, performative):
        self.performative = performative

    def parse(buff):
        pass
        
    def emit(buff, channel, payload):
        # Compute size
        # doff = 0x02
        # type = 0x00
        # channel = 0000
        pass

class _OpenFrame(_AmqpFrame):
    def __init__(self, size, data_offset):
        pass

from .datatypes import *

class _AmqpFrame:
    __slots__ = "data_offset", "performative"
    
    def __init__(self, data_offset, performative):
        self.data_offset = data_offset
        self.performtive = performative

    def parse(buff):
        pass
        
    def emit(buff, channel, payload):
        # Compute size
        pass

class _OpenFrame(_AmqpFrame):
    def __init__(self, size, data_offset)

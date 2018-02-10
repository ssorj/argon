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

from argon.io import *
from argon.io import _hex

class _DebugConnection(TcpConnection):
    def on_start(self):
        frame = AmqpFrame(0, OpenPerformative())
        frame.performative.container_id = "abc123"
        self.send_frame(frame)

        frame = AmqpFrame(0, BeginPerformative())
        frame.performative.next_outgoing_id = UnsignedInt(0)
        frame.performative.incoming_window = UnsignedInt(0xffff)
        frame.performative.outgoing_window = UnsignedInt(0xffff)
        self.send_frame(frame)

        frame = AmqpFrame(0, EndPerformative())
        self.send_frame(frame)

        frame = AmqpFrame(0, AttachPerformative())
        frame.performative.name = "abc"
        frame.performative.handle = UnsignedInt(0)
        frame.performative.role = False # sender
        self.send_frame(frame)

        frame = AmqpFrame(0, ClosePerformative())
        self.send_frame(frame)

    def on_frame(self, frame):
        pass

def _main():
    conn = _DebugConnection("127.0.0.1", 5672)
    conn.run()

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

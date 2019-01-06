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

from argon.transport import *
from argon.transport import _hex

class _DebugTransport(TcpTransport):
    def on_start(self):
        performative = OpenPerformative()
        performative.container_id = "abc123"
        self.emit_amqp_frame(0, performative)

        performative = BeginPerformative()
        performative.next_outgoing_id = UnsignedInt(0)
        performative.incoming_window = UnsignedInt(0xffff)
        performative.outgoing_window = UnsignedInt(0xffff)
        self.emit_amqp_frame(0, performative)

        performative = EndPerformative()
        self.emit_amqp_frame(0, performative)

        performative = AttachPerformative()
        performative.name = "abc"
        performative.handle = UnsignedInt(0)
        performative.role = False # sender
        self.emit_amqp_frame(0, performative)

        performative = ClosePerformative()
        self.emit_amqp_frame(0, performative)

    def on_frame(self, frame):
        pass

def _main():
    conn = _DebugTransport("127.0.0.1", 5672)
    conn.run()

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

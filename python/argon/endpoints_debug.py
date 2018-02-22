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

from argon.endpoints import *

class _DebugConnection(Connection):
    def __init__(self):
        super().__init__()

        self.session = _DebugSession(self)

    def on_start(self):
        self.send_open()
        
    def on_open(self):
        print("CONNECTION OPENED")

        self.session.send_open()

    def on_close(self):
        print("CONNECTION CLOSED")
        raise KeyboardInterrupt() # XXX

class _DebugSession(Session):
    def __init__(self, connection):
        super().__init__(connection)

        self.sender = _DebugSender(self, "q0")

    def on_open(self):
        print("SESSION OPENED")

        target = Target()
        target.address = "q0"

        self.sender.send_open()

    def on_close(self):
        print("SESSION CLOSED")

        self.connection.send_close()

class _DebugSender(Sender):
    def on_open(self):
        print("LINK OPENED")

    def on_flow(self):
        print("LINK FLOW")

        from argon.message import Message, emit_message

        message = Message()
        message.id = 123
        message.durable = True
        message.body = [1, 2, 3]
        message.properties["a"] = 1

        self.send_transfer(message)
        self.send_close()

    def on_close(self):
        print("LINK CLOSED")

        self.session.send_close()

def _main():
    transport = TcpTransport("127.0.0.1", 5672)

    conn = _DebugConnection()
    conn.bind(transport)

    transport.run()

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

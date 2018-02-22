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

        self.session = Session(self)
        self.sender = _DebugSender(self.session, "q0")

    def on_start(self):
        self.open()
        self.session.open()
        self.sender.open()
        
class _DebugSender(Sender):
    def on_flow(self):
        from argon.message import Message, emit_message

        message = Message()
        message.id = 123
        message.durable = True
        message.body = [1, 2, 3]
        message.properties["a"] = 1

        self.send(message)
        self.connection.close()
        self.transport.stop()

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

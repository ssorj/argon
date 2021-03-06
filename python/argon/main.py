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

from argon import *

class _MainConnection(Connection):
    def __init__(self, address, message):
        super().__init__()

        self.address = address
        self.message = message

        self.session = Session(self)
        self.sender = _MainSender(self.session, self.address)

    def on_start(self):
        self.open()
        self.session.open()
        self.sender.open()

    def on_close(self, error=None):
        self.transport.stop()

class _MainSender(Sender):
    def on_flow(self):
        self.send(self.session.connection.message)
        self.connection.close()

def send(host, port, address, message):
    transport = TcpTransport(host, port)

    conn = _MainConnection(address, message)
    conn.bind(transport)

    transport.run()

def main():
    operation, host, port, address, message_body = _sys.argv[1:]
    port = int(port)

    if operation == "send":
        message = Message()
        message.body = message_body

        send(host, port, address, message)
    else:
        raise Exception()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

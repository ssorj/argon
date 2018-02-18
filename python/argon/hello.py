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
from argon.message import *

class _HelloConnection(Connection):
    def __init__(self, host, port, container_id=None):
        super().__init__(host, port, container_id)

        self.session = _HelloSession(self)

    def on_open(self):
        self.session.send_open()

    def on_close(self):
        raise KeyboardInterrupt() # XXX

class _HelloSession(Session):
    def __init__(self, connection):
        super().__init__(connection)

        self.sender = _HelloSender(self, "hello")

    def on_open(self):
        target = Target()
        target.address = "hello"

        self.sender.send_open()

    def on_close(self):
        self.connection.send_close()

class _HelloSender(Sender):
    def on_flow(self):
        message = Message()
        message.id = 123
        message.durable = True
        message.body = [1, 2, 3]
        message.properties["a"] = 1

        buff = Buffer()
        offset = emit_message(buff, 0, message)

        self.send_transfer(buff[0:offset])
        self.send_close()

    def on_close(self):
        self.session.send_close()

def main():
    # import tracemalloc
    # tracemalloc.start()

    conn = _HelloConnection("amqp.zone", 5672)

    try:
        conn.run()
    except KeyboardInterrupt:
        pass

    # snapshot = tracemalloc.take_snapshot()
    # top_stats = snapshot.statistics('lineno')

    # print("[Top 20]")

    # for stat in top_stats[:20]:
    #     print(stat)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

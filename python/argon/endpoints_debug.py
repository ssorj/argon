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
    def __init__(self, host, port, container_id):
        super().__init__(host, port, container_id)

        self.session = _DebugSession(self, 0)

    def on_open(self):
        print("CONNECTION OPENED")

        self.session.send_open()

    def on_close(self):
        print("CONNECTION CLOSED")
        raise KeyboardInterrupt() # XXX

class _DebugSession(Session):
    def on_open(self):
        print("SESSION  OPENED")
        self.send_close()

    def on_close(self):
        print("SESSION CLOSED")
        self.connection.send_close()

def _main():
    conn = _DebugConnection("127.0.0.1", 5672, "abc")
    sess = _DebugSession(conn, 0)

    conn.run()

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

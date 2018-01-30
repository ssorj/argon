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

from argon.common import *
from argon.transport import *

def _log_send(octets, obj):
    print("S", octets)
    print(" ", obj)

def _log_receive(octets, obj):
    print("R", octets)
    print(" ", obj)

class _DebugConnection(TcpConnection):
    def on_start(self):
        frame = OpenFrame(0)
        frame.container_id = "abc123"
        self.send_frame(frame)

        frame = CloseFrame(0)
        self.send_frame(frame)

    def on_frame(self, frame):
        print(111, frame)

def _main():
    conn = _DebugConnection("127.0.0.1", 5672)
    conn.run()

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

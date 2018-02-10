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

from argon.common import _shorten, _time
from argon.data import _hex
from argon.message import *

def _main():
    debug = True

    buff = Buffer()
    offset = 0

    message = Message()
    message.id = 123
    message.durable = True
    message.body = b"x" * 8

    start = offset
    offset = emit_message(buff, offset, message)

    print(_hex(buff[start:offset]))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

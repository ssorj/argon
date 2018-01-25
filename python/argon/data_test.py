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

from argon.common import *
from argon.common import _time
from argon.data import *

_strings = [
    "",
    "\U0001F34B",
    "x" * 256,
]

_ints = [
    1,
    127,
    10 ** 10,
]

_floats = [
    1.0,
    -127.127,
    10.1 ** 10,
]

_lists = [
    [1, 2, 3],
    ["a", "b", "c"],
    [[1, 2, 3], ["a", "b", "c"]],
]

def _main():
    start = _time.time()
    
    buff = Buffer()
    offset = 0

    for coll in (_strings, _ints, _floats, _lists):
        for i in range(100 * 1000):
            for value in coll:
                offset = emit_data(buff, offset, value)

    duration = _time.time() - start
    print("Encoded {:,} megabytes/second".format(round(offset / duration / (1000 * 1000), 2)))

    start = _time.time()

    end = offset
    offset = 0
    
    while offset < end:
        offset, value, descriptor = parse_data(buff, offset)

    assert offset == end
        
    duration = _time.time() - start
    print("Decoded {:,} megabytes/second".format(round(offset / duration / (1000 * 1000), 2)))
            
if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

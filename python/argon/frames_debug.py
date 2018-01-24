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
from argon.common import _hex, _shorten, _time
from argon.frames import *

_frames = [
    (OpenFrame(), OpenFrameFields(container_id="test0")),
    (OpenFrame(), OpenFrameFields(container_id="test1", hostname="example.org")),
    (CloseFrame(), CloseFrameFields()),
]

def _main():
    debug = True

    buff = Buffer()
    offset = 0
    output_data = list()

    for frame, input_fields in _frames:
        if debug:
            print("Emitting {} {}".format(frame, input_fields))

        start = offset
        offset = frame.emit(buff, offset, 0, input_fields)

        data = _hex(buff[start:offset])
        output_data.append(data)

        if debug:
            print("Emitted {}".format(data))

    offset = 0
    output_fields = list()

    for frame, input_fields in _frames:
        if debug:
            lookahead = _hex(buff[offset:offset + 10])
            print("Parsing {}... for {} {}".format(lookahead, frame, input_fields))

        start = offset
        #offset, fields = parse_frame(buff, offset)
        offset, channel, fields = frame.parse(buff, offset)

        if debug:
            print("Parsed {}".format(_hex(buff[start:offset])))

        assert fields == input_fields, "Expected {} but got {}".format(input_fields, fields)

        output_fields.append(fields)

    row = "{:4}  {:12}  {:30}  {:30}  {}"

    for i, item in enumerate(_frames):
        frame, input_fields = item
        fields = output_fields[i]
        data = output_data[i]

        print(row.format(i, repr(frame), _shorten(str(input_fields), 30), _shorten(str(fields), 30), data))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

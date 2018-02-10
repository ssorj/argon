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
from argon.common import _shorten, _time
from argon.frames import *
from argon.frames import _frame_hex, _hex

_input_frames = [
    AmqpFrame(0, OpenPerformative(["test0"])),
    AmqpFrame(1, OpenPerformative(["test1", "example.org"])),
    AmqpFrame(0, BeginPerformative([None, 0, 100, 100])),
    AmqpFrame(0, AttachPerformative()),
    AmqpFrame(0, FlowPerformative()),
    AmqpFrame(0, TransferPerformative(), b"x" * 32),
    AmqpFrame(0, DispositionPerformative()),
    AmqpFrame(0, DetachPerformative()),
    AmqpFrame(0, EndPerformative()),
    AmqpFrame(0, ClosePerformative()),
]

def _main():
    debug = True

    buff = Buffer()
    offset = 0

    output_octets = list()

    for frame in _input_frames:
        if debug:
            print("Emitting {}".format(frame))

        start = offset
        offset = emit_frame(buff, offset, frame)

        octets = _frame_hex(buff[start:offset])
        output_octets.append(octets)

        if debug:
            print("Emitted {}".format(octets))

    offset = 0

    output_frames = list()

    for frame in _input_frames:
        if debug:
            lookahead = _hex(buff[offset:offset + 20])
            print("Parsing {}... for {}".format(lookahead, frame))

        start = offset
        offset, output_frame = parse_frame(buff, offset)

        if debug:
            print("Parsed {}".format(_frame_hex(buff[start:offset])))

        assert output_frame == frame, "Expected {} but got {}".format(frame, output_frame)

        output_frames.append(frame)

    row = "{:4}  {:30}  {:30}  {}"

    for i, frame in enumerate(_input_frames):
        output_frame = output_frames[i]
        octets = output_octets[i]

        args = (
            i,
            _shorten(str(frame), 30),
            _shorten(str(output_frame), 30),
            octets,
        )

        print(row.format(*args))

if __name__ == "__main__":
    try:
        _main()
    except KeyboardInterrupt:
        pass

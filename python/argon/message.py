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

from argon.common import _field_property

class _Section:
    __slots__ = ("_field_values",)
    
    def __init__(self, field_values=None):
        self._field_values = field_values

        if self._field_values is None:
            self._field_values = []

class _Header(_Section):
    _descriptor = UnsignedLong(0 << 32 | 0x00000070)
    
    durable = _field_property(0)
    priority = _field_property(1)
    ttl = _field_property(2)
    first_acquirer = _field_property(3)
    delivery_count = _field_property(4)

class _Properties(_Section):
    __slots__ = ()
    _descriptor = UnsignedLong(0 << 32 | 0x00000073)

    message_id = _field_property(0)
    user_id = _field_property(1)
    to = _field_property(2)
    subject = _field_property(3)
    reply_to = _field_property(4)
    correlation_id = _field_property(5)
    content_type = _field_property(6)
    content_encoding = _field_property(7)
    absolute_expiry_time = _field_property(8)
    creation_time = _field_property(9)
    group_id = _field_property(10)
    group_sequence = _field_property(11)
    reply_to_group_id = _field_property(12)

class Message:
    pass

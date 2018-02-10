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

from argon.data import *
from argon.data import _field

class _Header(DescribedValue):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000070), value)

        if self._value is None:
            self._value = list()

    durable = _field(0)
    priority = _field(1)
    ttl = _field(2)
    first_acquirer = _field(3)
    delivery_count = _field(4)

class _Attributes(DescribedValue):
    __slots__ = ()

    def __init__(self, descriptor, value=None):
        super().__init__(descriptor, value)

        if self._value is None:
            self._value = dict()

class _DeliveryAnnotations(_Attributes):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000071), value)

class _MessageAnnotations(_Attributes):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000072), value)

class _Properties(DescribedValue):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000073), value)

    message_id = _field(0)
    user_id = _field(1)
    to = _field(2)
    subject = _field(3)
    reply_to = _field(4)
    correlation_id = _field(5)
    content_type = _field(6)
    content_encoding = _field(7)
    absolute_expiry_time = _field(8)
    creation_time = _field(9)
    group_id = _field(10)
    group_sequence = _field(11)
    reply_to_group_id = _field(12)

class _ApplicationProperties(_Attributes):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000074), value)

class _ApplicationData(DescribedValue):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000077), value)

class _Footer(_Attributes):
    __slots__ = ()

    def __init__(self, value=None):
        super().__init__(UnsignedLong(0 << 32 | 0x00000078), value)

class Message:
    __slots__ = ("_header", "_delivery_annotations", "_message_annotations", "_properties",
                 "_application_properties", "_application_data", "_footer")

    def __init__(self):
        self._header = None
        self._delivery_annotations = None
        self._message_annotations = None
        self._properties = None
        self._application_properties = None
        self._application_data = None
        self._footer = None

    def _get_header(self):
        if self._header is None:
            self._header = _Header()

    def _get_properties(self):
        if self._properties is None:
            self._properties = _Properties()

    def _get_application_data(self):
        if self._application_data is None:
            self._application_data = _ApplicationData()

        return self._application_data

    @property
    def id(self):
        return self._get_properties().message_id

    @id.setter
    def id(self, value):
        self._get_properties().message_id = value

    @property
    def user_id(self):
        return self._get_properties().user_id

    @user_id.setter
    def user_id(self, value):
        self._get_properties().user_id = value

    @property
    def to(self):
        return self._get_properties().to

    @to.setter
    def to(self, value):
        self._get_properties().to = value

    @property
    def body(self):
        return self._get_application_data()._value

    @body.setter
    def body(self, value):
        self._get_application_data()._value = value

    @property
    def durable(self):
        return self._get_header().durable

    @durable.setter
    def durable(self, durable):
        self._get_header().durable = durable

    @property
    def properties(self):
        if self._application_properties is none:
            self._application_properties = _ApplicationProperties()

        return self._application_properties._value

    @property
    def delivery_annotations(self):
        if self._delivery_annotations is none:
            self._delivery_annotations = _deliveryannotations()

        return self._delivery_annotations._value

    @property
    def message_annotations(self):
        if self._message_annotations is none:
            self._message_annotations = _messageannotations()

        return self._message_annotations._value

    @property
    def footer(self):
        if self._footer is None:
            self._footer = _Footer()

        return self._footer._value

    def _emit(self, buff, offset):
        if self._header is not None:
            offset = self._header.emit(buff, offset)

        if self._delivery_annotations is not None:
            offset = emit_data(buff, offset, self._delivery_annotations)

        if self._message_annotations is not None:
            offset = emit_data(buff, offset, self._message_annotations)

        if self._properties is not None:
            offset = self._properties.emit(buff, offset)

        if self._application_properties is not None:
            offset = emit_data(buff, offset, self._application_properties)

        if self._application_data is not None:
            offset = emit_data(buff, offset, self._application_data)

        if self._footer is not None:
            offset = emit_data(buff, offset, self._footer)

        return offset

def emit_message(buff, offset, message):
    return message._emit(buff, offset)

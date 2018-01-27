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

export PYTHONPATH := python
export MICROPYPATH := python

.PHONY: default
default: frames-debug

.PHONY: data-debug
data-debug: data-debug-cpython data-debug-micropython

.PHONY: data-test
data-test: data-test-cpython data-test-micropython

.PHONY: frames-debug
frames-debug: frames-debug-cpython frames-debug-micropython

.PHONY: transport-debug
transport-debug: transport-debug-cpython transport-debug-micropython

.PHONY: %-debug-cpython
%-debug-cpython:
	python3 -m argon.$*_debug

.PHONY: %-debug-micropython
%-debug-micropython:
	micropython python/argon/$*_debug.py

.PHONY: %-test-cpython
%-test-cpython:
	python3 -m argon.$*_test

.PHONY: %-test-micropython
%-test-micropython:
	micropython python/argon/$*_test.py

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

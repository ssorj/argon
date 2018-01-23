export PYTHONPATH := python
export MICROPYPATH := python

.PHONY: default
default: data-debug

.PHONY: data-debug
data-debug: data-debug-cpython data-debug-micropython

.PHONY: data-debug-cpython
data-debug-cpython:
	python3 -m argon.data_debug

.PHONY: data-debug-micropython
data-debug-micropython:
	micropython python/argon/data_debug.py

.PHONY: data-test
data-test: data-test-cpython data-test-micropython

.PHONY: data-test-cpython
data-test-cpython:
	python3 -m argon.data_test.py

.PHONY: data-test-micropython
data-test-micropython:
	micropython python/argon/data_test.py

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

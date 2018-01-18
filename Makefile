export PYTHONPATH := python
export MICROPYPATH := python

.PHONY: test
test: test-cpython test-micropython

.PHONY: test-cpython
test-cpython:
	python3 -m argon.types

.PHONY: test-micropython
test-micropython:
	micropython python/argon/types.py

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

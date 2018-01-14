export PYTHONPATH := python

.PHONY: test
test: test-micropython

.PHONY: test-micropython
test-micropython:
	micropython python/argon/datatypes.py

.PHONY: test-cpython
test-cpython:
	python3 -m argon.datatypes

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

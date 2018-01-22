export PYTHONPATH := python
export MICROPYPATH := python

.PHONY: test
test: test-cpython test-micropython

.PHONY: test-cpython
test-cpython:
	python3 -m argon.data

.PHONY: test-micropython
test-micropython:
	micropython python/argon/data.py

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

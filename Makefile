export PYTHONPATH := python

.PHONY: test
test: test-micropython

.PHONY: big-test
big-test: test-micropython test-cpython

.PHONY: test-micropython
test-micropython:
	micropython python/argon/data.py

.PHONY: test-cpython
test-cpython:
	python3 -m argon.data

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

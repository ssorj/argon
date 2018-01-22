export PYTHONPATH := python
export MICROPYPATH := python

.PHONY: devel
devel: devel-cpython devel-micropython

.PHONY: devel-cpython
devel-cpython:
	python3 -m argon.datadevel

.PHONY: devel-micropython
devel-micropython:
	micropython python/argon/datadevel.py

.PHONY: test
test: test-cpython test-micropython

.PHONY: test-cpython
test-cpython:
	python3 -m argon.datatests

.PHONY: test-micropython
test-micropython:
	micropython python/argon/datatests.py

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

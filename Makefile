export PYTHONPATH := python

.PHONY: test
test:
	python3 -m argon.datatypes

.PHONY: clean
clean:
	find python -type f -name \*.pyc -delete
	rm -rf python/argon/__pycache__

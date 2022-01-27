clean:
	find . -name \*.pyc -delete
	find . -name __pycache__ -delete
	rm -rf dist/

.PHONY: test_unit
test_unit:
	poetry run python -bb -m pytest tests

.PHONY: test
test: test_unit

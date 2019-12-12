.DEFAULT: all
.PHONY: all develop test coverage

all:
	echo "nothing here, use one of the following targets:"
	echo "develop, test, clean"

develop:
	pip install -r requirements.txt
	python setup.py develop

test: develop
	pip install -r test-requirements.txt
	flake8 .

clean:
	rm -Rf *.egg-info htmlcov coverage.xml .coverage __pycache__ .pytest_cache mflog/__pycache__ tests/__pycache__

SHELL := /usr/bin/env bash

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -hE '^([a-zA-Z_-]+:)?[^\?]+?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {if ($$2 == "") { printf("\n\n\033[32m%s\033[0m\n\n", $$1) } else { printf("\033[36m%-15s\033[0m %s\n", $$1, $$2) }}'


## Build

.PHONY: build
build: ## Build api image
	docker-compose -f docker-compose.test.yml build

.PHONY: vendorize
vendorize: ## Update vendorized dependencies
	pip3 install --isolated --upgrade -t ./vendor -r requirements.txt

.PHONY: venv
venv: ## Update virtualenv
	virtualenv -p $$(which python3.8) venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install --upgrade -r requirements-test.txt


## Test

.PHONY: test
test: ## Run tests
	docker-compose -f docker-compose.test.yml build
	docker-compose -f docker-compose.test.yml up jql

.PHONY: lint
lint: ## Lint code
	venv/bin/flake8 --extend-exclude 'vendor'
	venv/bin/mypy -p jql


## Run

.PHONY: repl
repl: ## Run REPL
	PYTHONPATH="./vendor:${PYTHONPATH}" venv/bin/python -m jql.repl


-include Makefile.local

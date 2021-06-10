SHELL := /bin/bash

.DEFAULT_GOAL := help
.PHONY: help build test repl lint venv

help:
	@grep -E '^([a-zA-Z_-]+:)?[^\?]+?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {if ($$2 == "") { printf("\n\n\033[32m%s\033[0m\n\n", $$1) } else { printf("\033[36m%-15s\033[0m %s\n", $$1, $$2) }}'

## Build

build: ## Build api image
	docker-compose -f docker-compose.test.yml build

test: build ## Run tests
	docker-compose -f docker-compose.test.yml up -d db
	docker-compose -f docker-compose.test.yml up jql

repl: ## Run REPL
	venv/bin/python -m jql.repl

lint: ## Lint code
	venv/bin/flake8
	venv/bin/mypy -p jql

venv: ## Update virtualenv
	virtualenv -p $$(which python3.8) venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install --upgrade -r requirements-test.txt


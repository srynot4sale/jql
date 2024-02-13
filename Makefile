SHELL := /usr/bin/env bash

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -hE '^([a-zA-Z_-]+:)?[^\?]+?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {if ($$2 == "") { printf("\n\n\033[32m%s\033[0m\n\n", $$1) } else { printf("\033[36m%-15s\033[0m %s\n", $$1, $$2) }}'


## Build

.PHONY: build
build: ## Build api image
	docker compose -f docker-compose.test.yml build

.PHONY: vendorize
vendorize: ## Update vendorized dependencies
	rm -Rf ./vendor
	venv/bin/pip install --isolated -t ./vendor -r requirements.txt

.PHONY: venv
venv: ## Update virtualenv
	$$(which python3.10) -m venv venv
	venv/bin/pip install --upgrade pip wheel
	venv/bin/pip install --upgrade -r requirements.txt
	venv/bin/pip install --upgrade -r requirements-test.txt

.PHONY: js
js: ## Generate JS parser
	venv/bin/lark-js --start action jql/jql.lark -o parser.js

## Test

.PHONY: test
onlytest: test ## Run tests
	docker compose -f docker-compose.test.yml up --exit-code-from jql jql

.PHONY: dev
test: lint build onlytest ## Run linter and tests

.PHONY: lint
lint: ## Lint code
	RC=0
	venv/bin/flake8 || RC=1
	venv/bin/mypy -p jql || RC=1
	exit $$RC


## Run

.PHONY: replbuild
replbuild: ## Build REPL image
	docker build -f Dockerfile.repl -t jql-repl .

.PHONY: repl
repl: replbuild ## Run REPL
	docker run --rm --env-file secrets.env -ti -v jql-repl:/data jql-repl

.PHONY: repldb
repldb: replbuild ## Open Sqlite for REPL
	docker run --rm -ti -v jql-repl:/data jql-repl sqlite3 /data/repl.jdb

.PHONY: replmigrate
replmigrate: replbuild ## Run REPL migration
	docker run --rm --env-file secrets.env -t -v jql-repl:/data jql-repl python jql/store/sqlite_migration.py /data/repl.jdb

-include Makefile.local

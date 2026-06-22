#########
# BUILD #
#########
.PHONY: develop-py develop-rs develop
develop-py:
	uv pip install -e .[develop]

develop-rs:
	make -C rust develop

develop: develop-rs develop-py  ## setup project for development

.PHONY: requirements-py requirements-rs requirements
requirements-py:  ## install prerequisite python build requirements
	python -m pip install --upgrade pip toml
	python -m pip install `python -c 'import toml; c = toml.load("pyproject.toml"); print("\n".join(c["build-system"]["requires"]))'`
	python -m pip install `python -c 'import toml; c = toml.load("pyproject.toml"); print(" ".join(c["project"]["optional-dependencies"]["develop"]))'`

requirements-rs:  ## install prerequisite rust build requirements
	make -C rust requirements

requirements: requirements-rs requirements-py  ## setup project for development

.PHONY: build-py build-rs build
build-py:
	python -m build -w -n

build-rs:
	make -C rust build

build: build-rs build-py  ## build the project

.PHONY: install
install:  ## install python library
	uv pip install .

#########
# LINTS #
#########
.PHONY: lint-py lint-rs lint-docs lint lints
lint-py:  ## run python linter with ruff
	python -m ruff check fsspec_db
	python -m ruff format --check fsspec_db

lint-rs:  ## run rust linter
	make -C rust lint

lint-docs:  ## lint docs with mdformat and codespell
	python -m mdformat --check README.md
	python -m codespell_lib README.md

lint: lint-rs lint-py lint-docs  ## run project linters

# alias
lints: lint

.PHONY: fix-py fix-rs fix-docs fix format
fix-py:  ## fix python formatting with ruff
	python -m ruff check --fix fsspec_db
	python -m ruff format fsspec_db

fix-rs:  ## fix rust formatting
	make -C rust fix

fix-docs:  ## autoformat docs with mdformat and codespell
	python -m mdformat README.md
	python -m codespell_lib --write README.md

fix: fix-rs fix-py fix-docs  ## run project autoformatters

# alias
format: fix

################
# Other Checks #
################
.PHONY: check-dist check-types checks check

check-dist:  ## check python sdist and wheel with check-dist
	check-dist -v

check-types:  ## check python types with ty
	ty check --python $$(which python)

checks: check-dist

# alias
check: checks

#########
# TESTS #
#########
.PHONY: test-py tests-py coverage-py
test-py:  ## run python tests
	python -m pytest -v fsspec_db/tests

# alias
tests-py: test-py

coverage-py:  ## run python tests and collect test coverage
	python -m pytest -v fsspec_db/tests --cov=fsspec_db --cov-report term-missing --cov-report xml

.PHONY: test-rs tests-rs coverage-rs
test-rs:  ## run rust tests
	make -C rust test

# alias
tests-rs: test-rs

coverage-rs:  ## run rust tests and collect test coverage
	make -C rust coverage

.PHONY: test coverage tests
test: test-py test-rs  ## run all tests
coverage: coverage-py coverage-rs  ## run all tests and collect test coverage

# alias
tests: test

###############
# INTEGRATION #
###############
# Live Postgres/MySQL integration tests, gated on FSSPEC_DB_*_URL env vars.
# Uses podman-compose by default; override with `make COMPOSE="docker compose" ...`.
COMPOSE ?= podman-compose
COMPOSE_FILE ?= ci/docker-compose.yml
FSSPEC_DB_POSTGRES_URL ?= postgresql://fsspec:fsspec@127.0.0.1:55432/fsspec
FSSPEC_DB_MYSQL_URL ?= mysql://fsspec:fsspec@127.0.0.1:53306/fsspec
# NOTE: do not `export` these globally. They are passed only to the integration
# recipes below, so a plain `make test`/`make coverage` does not put them in the
# environment and trigger the env-gated Postgres/MySQL integration tests (which
# would fail without a live database).

.PHONY: dbs-up dbs-wait dbs-down dbs-logs test-integration test-integration-rs test-integration-py

dbs-up:  ## start the Postgres/MySQL fixtures (podman-compose)
	$(COMPOSE) -f $(COMPOSE_FILE) up -d

dbs-wait:  ## block until the database fixtures accept connections
	@echo "waiting for postgres + mysql to become ready..."
	@for i in $$(seq 1 60); do \
		if $(COMPOSE) -f $(COMPOSE_FILE) exec -T postgres pg_isready -U fsspec -d fsspec >/dev/null 2>&1 \
			&& $(COMPOSE) -f $(COMPOSE_FILE) exec -T mysql mysql -ufsspec -pfsspec fsspec -e 'SELECT 1' >/dev/null 2>&1; then \
			echo "databases ready"; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "timed out waiting for databases"; $(COMPOSE) -f $(COMPOSE_FILE) ps; exit 1

dbs-down:  ## stop the database fixtures and remove their volumes
	$(COMPOSE) -f $(COMPOSE_FILE) down -v

dbs-logs:  ## tail the database fixture logs
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f

test-integration-rs:  ## run the env-gated rust integration tests
	FSSPEC_DB_POSTGRES_URL="$(FSSPEC_DB_POSTGRES_URL)" FSSPEC_DB_MYSQL_URL="$(FSSPEC_DB_MYSQL_URL)" $(MAKE) -C rust test

test-integration-py:  ## run the env-gated python integration tests
	FSSPEC_DB_POSTGRES_URL="$(FSSPEC_DB_POSTGRES_URL)" FSSPEC_DB_MYSQL_URL="$(FSSPEC_DB_MYSQL_URL)" python -m pytest -v fsspec_db/tests/test_integration.py

test-integration:  ## bring up dbs, run all integration tests, and always tear them down
	@status=0; \
	{ $(MAKE) dbs-up \
		&& $(MAKE) dbs-wait \
		&& $(MAKE) test-integration-rs \
		&& $(MAKE) test-integration-py; } || status=$$?; \
	$(MAKE) dbs-down; \
	exit $$status

###########
# VERSION #
###########
.PHONY: show-version patch minor major

show-version:  ## show current library version
	@bump-my-version show current_version

patch:  ## bump a patch version
	@bump-my-version bump patch

minor:  ## bump a minor version
	@bump-my-version bump minor

major:  ## bump a major version
	@bump-my-version bump major

########
# DIST #
########
.PHONY: dist-py-wheel dist-py-sdist dist-rs dist-check dist publish

dist-py-wheel:  ## build python wheel
	python -m cibuildwheel --output-dir dist

dist-py-sdist:  ## build python sdist
	python -m build --sdist -o dist

dist-rs:  ## build rust dists
	make -C rust dist

dist-check:  ## run python dist checker with twine
	python -m twine check dist/*

dist: clean build dist-rs dist-py-wheel dist-py-sdist dist-check  ## build all dists

publish: dist  ## publish python assets

#########
# CLEAN #
#########
.PHONY: deep-clean clean

deep-clean: ## clean everything from the repository
	git clean -fdx

clean: ## clean the repository
	rm -rf .coverage coverage cover htmlcov logs build dist *.egg-info

############################################################################################

.PHONY: help

# Thanks to Francoise at marmelab.com for this
.DEFAULT_GOAL := help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

print-%:
	@echo '$*=$($*)'

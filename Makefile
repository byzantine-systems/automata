.DEFAULT_GOAL := build
.DELETE_ON_ERROR:

PROJECT_NAME ?= bs-automata
DOTNET ?= dotnet
NIX ?= nix
FANTOMAS ?= fantomas
DB_CONNECTION_STRING ?= Host=127.0.0.1;Port=5432;Database=$(PROJECT_NAME);Username=$(PROJECT_NAME);Password=$(PROJECT_NAME)
DB_URL ?= postgresql://$(PROJECT_NAME):$(PROJECT_NAME)@127.0.0.1:5432/$(PROJECT_NAME)

SOLUTION := bs-automata.slnx
POSTGRES_PROJECT := src/ByzantineSystems.Automata.Storage.Postgres/ByzantineSystems.Automata.Storage.Postgres.fsproj
SQLHYDRA_CONFIG := sqlhydra-npgsql.toml
SQLITE_PROJECT := src/ByzantineSystems.Automata.Storage.Sqlite/ByzantineSystems.Automata.Storage.Sqlite.fsproj
SQLHYDRA_SQLITE_CONFIG := sqlhydra-sqlite.toml
# A scratch database migrated from scratch on every codegen run; nothing else reads it.
SQLITE_SCHEMA_DB := out/sqlhydra/schema.db
MIGRATE_PROJECT := tools/ByzantineSystems.Automata.Migrate/ByzantineSystems.Automata.Migrate.fsproj
EXAMPLE_PAYMENT_PROJECT := examples/ByzantineSystems.Automata.Examples.PaymentProcessor/ByzantineSystems.Automata.Examples.PaymentProcessor.fsproj
EXAMPLE_SUPERVISION_PROJECT := examples/ByzantineSystems.Automata.Examples.Supervision/ByzantineSystems.Automata.Examples.Supervision.fsproj
EXAMPLE_HOSTED_PROJECT := examples/ByzantineSystems.Automata.Examples.Hosted/ByzantineSystems.Automata.Examples.Hosted.fsproj
EXAMPLE_PROJECTS := $(EXAMPLE_PAYMENT_PROJECT) $(EXAMPLE_SUPERVISION_PROJECT) $(EXAMPLE_HOSTED_PROJECT)

SRC_PROJECTS := $(wildcard src/*/*.fsproj)
UNIT_TEST_PROJECTS := \
	tests/ByzantineSystems.Automata.Core.Tests/ByzantineSystems.Automata.Core.Tests.fsproj \
	tests/ByzantineSystems.Automata.Resilience.Tests/ByzantineSystems.Automata.Resilience.Tests.fsproj \
	tests/ByzantineSystems.Automata.Runtime.Tests/ByzantineSystems.Automata.Runtime.Tests.fsproj \
	tests/ByzantineSystems.Automata.DependencyInjection.Tests/ByzantineSystems.Automata.DependencyInjection.Tests.fsproj \
	tests/ByzantineSystems.Automata.Storage.Sqlite.Tests/ByzantineSystems.Automata.Storage.Sqlite.Tests.fsproj
INTEGRATION_TEST_PROJECTS := \
	tests/ByzantineSystems.Automata.Storage.Postgres.Tests/ByzantineSystems.Automata.Storage.Postgres.Tests.fsproj
TEST_PROJECTS := $(UNIT_TEST_PROJECTS) $(INTEGRATION_TEST_PROJECTS)

NUGET_SOURCE := https://api.nuget.org/v3/index.json
NUGET_API_KEY ?= None
NUGET_PACKAGES_DIR := out/nix-lock
NUGET_TO_JSON ?= nixpkgs\#nuget-to-json
DOCS_OUTPUT := out/docs
PACKAGE_OUTPUT := out/packages
COVERAGE_DIR := $(CURDIR)/coverage
COVERAGE_RAW_DIR := $(COVERAGE_DIR)/raw

release := $(shell git tag -l --sort=-creatordate | head -n 1)
# Untagged builds take the version Directory.Build.props declares, which is also what the flake
# reads, so a local pack and a Nix build agree on what they are building.
declared := $(shell sed -n 's:.*<Version>\(.*\)</Version>.*:\1:p' Directory.Build.props | head -n 1)
VERSION ?= $(if $(release),$(patsubst v%,%,$(release)),$(declared))

PROJECT_FILES := $(wildcard src/*/*.fsproj tests/*/*.fsproj tools/*/*.fsproj examples/*/*.fsproj)
RESTORE_INPUTS := Makefile $(SOLUTION) global.json nuget.config $(PROJECT_FILES) \
	$(wildcard Directory.Build.* Directory.Packages.*)

.PHONY: build test test-unit test-integration coverage migrate schema-generate schema-check migrate-sqlite-schema schema-generate-sqlite schema-check-sqlite run-example run-example-supervision run-example-hosted db db-reset fmt docs nix-lock pack package-smoke push

build:
	$(DOTNET) build $(SOLUTION) -m:1

test: test-unit

test-unit: build
	@for project in $(UNIT_TEST_PROJECTS); do \
		$(DOTNET) run --project $$project --no-build || exit 1; \
	done

# Requires a running PostgreSQL; gated on AUTOMATA_TEST_DB inside the test host.
test-integration: build
	@for project in $(INTEGRATION_TEST_PROJECTS); do \
		$(DOTNET) run --project $$project --no-build || exit 1; \
	done

coverage:
	@test -n "$${AUTOMATA_TEST_DB:-}" || { echo "AUTOMATA_TEST_DB must be set to run all coverage tests." >&2; exit 1; }
	$(RM) -r $(COVERAGE_DIR)
	$(DOTNET) tool restore
	@set -eu; \
	for project in $(TEST_PROJECTS); do \
		name=$$(basename "$$(dirname "$$project")"); \
		mkdir -p "$(COVERAGE_RAW_DIR)/$$name"; \
		$(DOTNET) test "$$project" \
			/p:CollectCoverage=true \
			/p:CoverletOutputFormat=cobertura \
			/p:CoverletOutput="$(COVERAGE_RAW_DIR)/$$name/"; \
	done
	$(DOTNET) reportgenerator \
		-reports:"$(COVERAGE_RAW_DIR)/**/coverage.cobertura.xml" \
		-targetdir:"$(COVERAGE_DIR)" \
		-reporttypes:Cobertura
	$(DOTNET) reportgenerator \
		-reports:"$(COVERAGE_RAW_DIR)/**/coverage.cobertura.xml" \
		-targetdir:"$(COVERAGE_DIR)/html" \
		-reporttypes:Html
	@echo "Cobertura coverage report: $(COVERAGE_DIR)/Cobertura.xml"
	@echo "HTML coverage report: $(COVERAGE_DIR)/html/index.html"

migrate:
	BS_AUTOMATA_CONN='$(DB_CONNECTION_STRING)' $(DOTNET) run --project $(MIGRATE_PROJECT)

# The generated schema types come from the migrated database, so both targets migrate first.
# Regenerate after any migration that changes a table or view the stores read.
schema-generate: migrate
	$(DOTNET) tool restore
	$(DOTNET) sqlhydra npgsql -t $(SQLHYDRA_CONFIG) -p $(POSTGRES_PROJECT)
	$(FANTOMAS) src/ByzantineSystems.Automata.Storage.Postgres/Schema.Generated.fs

# Fails when the committed types and the schema disagree. CI runs it after migrating.
schema-check: migrate
	$(DOTNET) tool restore
	DOTNET='$(DOTNET)' FANTOMAS='$(FANTOMAS)' bash scripts/verify-sqlhydra.sh

# The SQLite store's generated types come from a scratch database migrated from nothing, so
# the generated file depends on the migrations alone and never on a developer's local data.
migrate-sqlite-schema:
	mkdir -p $(dir $(SQLITE_SCHEMA_DB))
	$(RM) $(SQLITE_SCHEMA_DB) $(SQLITE_SCHEMA_DB)-wal $(SQLITE_SCHEMA_DB)-shm
	BS_AUTOMATA_SQLITE_PATH='$(SQLITE_SCHEMA_DB)' $(DOTNET) run --project $(MIGRATE_PROJECT)

schema-generate-sqlite: migrate-sqlite-schema
	$(DOTNET) tool restore
	$(DOTNET) sqlhydra sqlite -t $(SQLHYDRA_SQLITE_CONFIG) -p $(SQLITE_PROJECT)
	$(FANTOMAS) src/ByzantineSystems.Automata.Storage.Sqlite/Schema.Generated.fs

schema-check-sqlite: migrate-sqlite-schema
	$(DOTNET) tool restore
	DOTNET='$(DOTNET)' FANTOMAS='$(FANTOMAS)' SQLHYDRA_PROVIDER=sqlite SQLHYDRA_CONFIG='$(SQLHYDRA_SQLITE_CONFIG)' \
		SQLHYDRA_SCHEMA_FILE=src/ByzantineSystems.Automata.Storage.Sqlite/Schema.Generated.fs \
		SQLHYDRA_PROJECT_FILE='$(SQLITE_PROJECT)' SQLHYDRA_GENERATE_TARGET=schema-generate-sqlite \
		bash scripts/verify-sqlhydra.sh

run-example:
	$(DOTNET) run --project $(EXAMPLE_PAYMENT_PROJECT)

run-example-supervision:
	$(DOTNET) run --project $(EXAMPLE_SUPERVISION_PROJECT)

# The production shape: supervised processing and delivery, maintenance, and a caller. Needs
# BS_AUTOMATA_CONN, like run-example.
run-example-hosted:
	$(DOTNET) run --project $(EXAMPLE_HOSTED_PROJECT)

db:
	psql '$(DB_URL)'

# cron.job rows outlive DROP SCHEMA, so the pg_cron ticks are removed first; the DO block
# makes that a no-op on a database where the routine was never installed.
db-reset:
	psql '$(DB_URL)' -v ON_ERROR_STOP=1 -c "DO \$$\$$ BEGIN IF to_regproc('fsm.unschedule_maintenance') IS NOT NULL THEN PERFORM fsm.unschedule_maintenance(); END IF; END \$$\$$;" -c 'DROP SCHEMA IF EXISTS fsm CASCADE; DROP TABLE IF EXISTS public.schemaversions;'
	$(MAKE) migrate DB_CONNECTION_STRING='$(DB_CONNECTION_STRING)'

fmt:
	$(NIX) fmt

docs:
	$(DOTNET) tool restore
	$(RM) -r $(DOCS_OUTPUT)
	$(DOTNET) docfx docfx.json

# SOURCE:
# https://github.com/NixOS/nixpkgs/blob/master/doc/languages-frameworks/dotnet.section.md#generating-and-updating-nuget-dependencies-generating-and-updating-nuget-dependencies
nix-lock: deps.json

deps.json: $(RESTORE_INPUTS)
	$(RM) -r $(NUGET_PACKAGES_DIR)
	$(DOTNET) restore $(SOLUTION) --packages $(NUGET_PACKAGES_DIR) -m:1
	@for project in $(EXAMPLE_PROJECTS); do \
		$(DOTNET) restore "$$project" --packages $(NUGET_PACKAGES_DIR) -m:1 || exit 1; \
	done
	$(NIX) run '$(NUGET_TO_JSON)' -- $(NUGET_PACKAGES_DIR) > $@.tmp
	mv $@.tmp $@

# Packages VERSION, defaulting to the newest git tag or the repository version.
pack:
	@echo "PACKING RELEASE: $(VERSION)"
	$(RM) -r $(PACKAGE_OUTPUT)
	@for project in $(SRC_PROJECTS); do \
		$(DOTNET) pack "$$project" -c Release /p:Version=$(VERSION) /p:PackageOutputPath=$(CURDIR)/$(PACKAGE_OUTPUT) || exit 1; \
	done

# Restores every locally packed library into a clean F# consumer and compiles the README's code
# against them, so the documented API is proven to be the packed API.
package-smoke: pack
	DOTNET='$(DOTNET)' bash scripts/package-smoke.sh "$(VERSION)" "$(CURDIR)/$(PACKAGE_OUTPUT)" "$(NUGET_SOURCE)" \
		$(notdir $(basename $(SRC_PROJECTS)))

# Pushes the packed release to NuGet. Symbols are their own packages and are never carried by
# the .nupkg, so both globs are pushed or debuggers get the library without its sources.
push:
	@echo "Pushing release '$(VERSION)' to $(NUGET_SOURCE)"
	$(DOTNET) nuget push '$(PACKAGE_OUTPUT)/*.nupkg' -k "$(NUGET_API_KEY)" -s $(NUGET_SOURCE) --skip-duplicate
	$(DOTNET) nuget push '$(PACKAGE_OUTPUT)/*.snupkg' -k "$(NUGET_API_KEY)" -s $(NUGET_SOURCE) --skip-duplicate

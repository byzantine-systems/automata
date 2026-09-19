.DEFAULT_GOAL := build
.DELETE_ON_ERROR:

PROJECT_NAME ?= bs-automata
DOTNET ?= dotnet
NIX ?= nix
DB_CONNECTION_STRING ?= Host=127.0.0.1;Port=5432;Database=$(PROJECT_NAME);Username=$(PROJECT_NAME);Password=$(PROJECT_NAME)
DB_URL ?= postgresql://$(PROJECT_NAME):$(PROJECT_NAME)@127.0.0.1:5432/$(PROJECT_NAME)

SOLUTION := bs-automata.slnx
MIGRATE_PROJECT := tools/ByzantineSystems.Automata.Migrate/ByzantineSystems.Automata.Migrate.fsproj
EXAMPLE_PAYMENT_PROJECT := examples/ByzantineSystems.Automata.Examples.PaymentProcessor/ByzantineSystems.Automata.Examples.PaymentProcessor.fsproj
EXAMPLE_SUPERVISION_PROJECT := examples/ByzantineSystems.Automata.Examples.Supervision/ByzantineSystems.Automata.Examples.Supervision.fsproj

SRC_PROJECTS := $(wildcard src/*/*.fsproj)
UNIT_TEST_PROJECTS := \
	tests/ByzantineSystems.Automata.Core.Tests/ByzantineSystems.Automata.Core.Tests.fsproj \
	tests/ByzantineSystems.Automata.Resilience.Tests/ByzantineSystems.Automata.Resilience.Tests.fsproj \
	tests/ByzantineSystems.Automata.Runtime.Tests/ByzantineSystems.Automata.Runtime.Tests.fsproj \
	tests/ByzantineSystems.Automata.DependencyInjection.Tests/ByzantineSystems.Automata.DependencyInjection.Tests.fsproj
INTEGRATION_TEST_PROJECTS := \
	tests/ByzantineSystems.Automata.Storage.Postgres.Tests/ByzantineSystems.Automata.Storage.Postgres.Tests.fsproj

NUGET_SOURCE := https://api.nuget.org/v3/index.json
NUGET_API_KEY ?= None
NUGET_PACKAGES_DIR := out/nix-lock
NUGET_TO_JSON ?= nixpkgs\#nuget-to-json
DOCS_OUTPUT := out/docs
PACKAGE_OUTPUT := out/packages

release := $(shell git tag -l --sort=-creatordate | head -n 1)
VERSION ?= $(if $(release),$(patsubst v%,%,$(release)),0.1.0)

PROJECT_FILES := $(wildcard src/*/*.fsproj tests/*/*.fsproj tools/*/*.fsproj examples/*/*.fsproj)
RESTORE_INPUTS := Makefile $(SOLUTION) global.json nuget.config $(PROJECT_FILES) \
	$(wildcard Directory.Build.* Directory.Packages.*)

.PHONY: build test test-unit test-integration migrate run-example run-example-supervision db db-reset fmt docs nix-lock pack package-smoke push

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

migrate:
	BS_AUTOMATA_CONN='$(DB_CONNECTION_STRING)' $(DOTNET) run --project $(MIGRATE_PROJECT)

run-example:
	$(DOTNET) run --project $(EXAMPLE_PAYMENT_PROJECT)

run-example-supervision:
	$(DOTNET) run --project $(EXAMPLE_SUPERVISION_PROJECT)

db:
	psql '$(DB_URL)'

db-reset:
	psql '$(DB_URL)' -v ON_ERROR_STOP=1 -c 'DROP SCHEMA IF EXISTS fsm CASCADE; DROP TABLE IF EXISTS public.schemaversions;'
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
	$(NIX) run '$(NUGET_TO_JSON)' -- $(NUGET_PACKAGES_DIR) > $@.tmp
	mv $@.tmp $@

# Packages VERSION, defaulting to the newest git tag or the repository version.
pack:
	@echo "PACKING RELEASE: $(VERSION)"
	$(RM) -r $(PACKAGE_OUTPUT)
	@for project in $(SRC_PROJECTS); do \
		$(DOTNET) pack "$$project" -c Release /p:Version=$(VERSION) /p:PackageOutputPath=$(CURDIR)/$(PACKAGE_OUTPUT) || exit 1; \
	done

# Restores every locally packed library into a clean F# consumer and compiles it.
package-smoke: pack
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
		$(DOTNET) new classlib --language F# --framework net10.0 --output "$$tmp" --no-restore; \
		for package in $(notdir $(basename $(SRC_PROJECTS))); do \
			$(DOTNET) add "$$tmp" package "$$package" --version "$(VERSION)" --no-restore || exit 1; \
		done; \
		$(DOTNET) restore "$$tmp" --source "$(CURDIR)/$(PACKAGE_OUTPUT)" --source "$(NUGET_SOURCE)" || exit 1; \
		$(DOTNET) build "$$tmp" --no-restore

# Pushes the packed release to NuGet
push:
	@echo "Pushing release '$(VERSION)' to $(NUGET_SOURCE)"
	$(DOTNET) nuget push '$(PACKAGE_OUTPUT)/*.nupkg' -k "$(NUGET_API_KEY)" -s $(NUGET_SOURCE) --skip-duplicate

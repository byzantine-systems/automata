.DEFAULT_GOAL := build
.DELETE_ON_ERROR:

PROJECT_NAME ?= bs-automata
DOTNET ?= dotnet
NIX ?= nix

SOLUTION := bs-automata.slnx
MIGRATE_PROJECT := tools/ByzantineSystems.Automata.Migrate/ByzantineSystems.Automata.Migrate.fsproj
SAMPLE_PROJECT := samples/ByzantineSystems.Automata.Samples.PaymentProcessor/ByzantineSystems.Automata.Samples.PaymentProcessor.fsproj

SRC_PROJECTS := $(wildcard src/*/*.fsproj)
UNIT_TEST_PROJECTS := \
	tests/ByzantineSystems.Automata.Core.Tests/ByzantineSystems.Automata.Core.Tests.fsproj \
	tests/ByzantineSystems.Automata.Resilience.Tests/ByzantineSystems.Automata.Resilience.Tests.fsproj \
	tests/ByzantineSystems.Automata.Runtime.Tests/ByzantineSystems.Automata.Runtime.Tests.fsproj
INTEGRATION_TEST_PROJECTS := \
	tests/ByzantineSystems.Automata.Storage.Postgres.Tests/ByzantineSystems.Automata.Storage.Postgres.Tests.fsproj

NUGET_SOURCE := https://api.nuget.org/v3/index.json
NUGET_API_KEY ?= None
NUGET_PACKAGES_DIR := out/nix-lock
NUGET_TO_JSON ?= nixpkgs\#nuget-to-json

release := $(shell git tag -l --sort=-creatordate | head -n 1)

PROJECT_FILES := $(wildcard src/*/*.fsproj tests/*/*.fsproj tools/*/*.fsproj samples/*/*.fsproj)
RESTORE_INPUTS := Makefile $(SOLUTION) global.json nuget.config $(PROJECT_FILES) \
	$(wildcard Directory.Build.* Directory.Packages.*)

.PHONY: build test test-unit test-integration migrate run-sample db fmt nix-lock pack push

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
	$(DOTNET) run --project $(MIGRATE_PROJECT)

run-sample:
	$(DOTNET) run --project $(SAMPLE_PROJECT)

db:
	psql postgresql://$(PROJECT_NAME):$(PROJECT_NAME)@127.0.0.1:5432/$(PROJECT_NAME)

fmt:
	$(NIX) fmt

# SOURCE:
# https://github.com/NixOS/nixpkgs/blob/master/doc/languages-frameworks/dotnet.section.md#generating-and-updating-nuget-dependencies-generating-and-updating-nuget-dependencies
nix-lock: deps.json

deps.json: $(RESTORE_INPUTS)
	$(RM) -r $(NUGET_PACKAGES_DIR)
	$(DOTNET) restore $(SOLUTION) --packages $(NUGET_PACKAGES_DIR) -m:1
	$(NIX) run '$(NUGET_TO_JSON)' -- $(NUGET_PACKAGES_DIR) > $@.tmp
	mv $@.tmp $@

# Packages the newest git tag as a .NET release
pack:
	@echo "PACKING RELEASE: $(release)"
	rm -f src/*/bin/Release/*.nupkg src/*/bin/Release/*.snupkg
	$(DOTNET) pack -c Release /p:Version=$(release:v%=%) $(SRC_PROJECTS)

# Pushes the packed release to NuGet
push:
	@echo "Pushing release '$(release)' to $(NUGET_SOURCE)"
	$(DOTNET) nuget push 'src/*/bin/Release/*.nupkg' -k "$(NUGET_API_KEY)" -s $(NUGET_SOURCE) --skip-duplicate

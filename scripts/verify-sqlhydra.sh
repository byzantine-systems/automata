#!/usr/bin/env bash
# Fails when the committed generated schema types and the migrated schema disagree.
#
# Regenerates in place, diffs against what was committed, and always restores the committed
# files, so a check never leaves the tree changed. The project file is guarded too, because the
# generator rewrites it whenever it thinks the generated file is missing from it.
#
# Serves both stores. The defaults are the PostgreSQL store's; the SQLite store's make target
# overrides all five.

set -euo pipefail

provider="${SQLHYDRA_PROVIDER:-npgsql}"
config="${SQLHYDRA_CONFIG:-sqlhydra-npgsql.toml}"
schema_file="${SQLHYDRA_SCHEMA_FILE:-src/ByzantineSystems.Automata.Storage.Postgres/Schema.Generated.fs}"
project_file="${SQLHYDRA_PROJECT_FILE:-src/ByzantineSystems.Automata.Storage.Postgres/ByzantineSystems.Automata.Storage.Postgres.fsproj}"
generate_target="${SQLHYDRA_GENERATE_TARGET:-schema-generate}"
schema_backup="$(mktemp)"
project_backup="$(mktemp)"

restore() {
    cp "$schema_backup" "$schema_file"
    cp "$project_backup" "$project_file"
    rm -f "$schema_backup" "$project_backup"
}

cp "$schema_file" "$schema_backup"
cp "$project_file" "$project_backup"
trap restore EXIT

"${DOTNET:-dotnet}" sqlhydra "$provider" -t "$config" -p "$project_file" >/dev/null
"${FANTOMAS:-fantomas}" "$schema_file" >/dev/null

drift=0
diff -u "$schema_backup" "$schema_file" || drift=1
diff -u "$project_backup" "$project_file" || drift=1

if [[ $drift -ne 0 ]]; then
    echo "The generated schema types are out of date: run make $generate_target and commit the result." >&2
    exit 1
fi

echo "Generated schema types match the migrated schema."

#!/usr/bin/env bash
# Keeps handwritten library tasks independent of a caller's synchronization context.

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
    printf '%s\n' 'verify-async-context: rg is required' >&2
    exit 1
fi

matches=''

if matches="$(
    rg --line-number --color never '\btask(Result)?\s*\{' src \
        --glob '*.fs'
)"; then
    :
else
    status=$?

    if (( status != 1 )); then
        exit "$status"
    fi
fi

violations=()
postgres_generated=0
sqlite_generated=0

if [[ -n "$matches" ]]; then
    while IFS= read -r match; do
        case "$match" in
            src/ByzantineSystems.Automata.Storage.Postgres/Schema.Generated.fs:*)
                ((postgres_generated += 1))
                ;;
            src/ByzantineSystems.Automata.Storage.Sqlite/Schema.Generated.fs:*)
                ((sqlite_generated += 1))
                ;;
            *) violations+=("$match") ;;
        esac
    done <<< "$matches"
fi

if (( ${#violations[@]} > 0 )); then
    printf '%s\n' 'Handwritten library tasks must use backgroundTask or backgroundTaskResult:' >&2
    printf '  %s\n' "${violations[@]}" >&2
    exit 1
fi

if (( postgres_generated != 1 || sqlite_generated != 1 )); then
    printf '%s\n' 'Expected exactly one generated task factory in each provider schema file.' >&2
    printf '  PostgreSQL: %d\n' "$postgres_generated" >&2
    printf '  SQLite: %d\n' "$sqlite_generated" >&2
    exit 1
fi

#!/usr/bin/env bash
# Builds the README's F# against the freshly packed packages, in a consumer that knows nothing
# of this repository.
#
# Restoring every package proves the packages exist and resolve. Compiling the README proves the
# documented API is the packed API: the chart and the hosted setup the README shows are what a
# user copies first, and a README that no longer compiles is found here rather than by them.
#
# Usage: package-smoke.sh <version> <local package directory> <nuget source> <package ids...>

set -euo pipefail

version="$1"
packages="$2"
nuget="$3"
shift 3

dotnet="${DOTNET:-dotnet}"
readme="$(pwd)/README.md"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

"$dotnet" new classlib --language F# --framework net10.0 --output "$tmp" --no-restore >/dev/null

for package in "$@"; do
    "$dotnet" add "$tmp" package "$package" --version "$version" --no-restore >/dev/null
done

# The nth ```fsharp block of the README, verbatim.
block() {
    awk -v wanted="$1" '
        /^```fsharp$/ { inside = 1; seen++; next }
        /^```$/ && inside { inside = 0; next }
        inside && seen == wanted { print }
    ' "$readme"
}

# The two blocks are one program: the chart, then the host that runs it. A library file needs a
# module declaration, which the README leaves out for readability, so it is added here.
{ echo "module Readme.Chart"; echo; block 1; } > "$tmp/Chart.fs"
{ echo "module Readme.Run"; echo; echo "open Readme.Chart"; block 2; } > "$tmp/Run.fs"

rm "$tmp/Library.fs"
sed -i 's|<Compile Include="Library.fs" />|<Compile Include="Chart.fs" /><Compile Include="Run.fs" />|' "$tmp"/*.fsproj

"$dotnet" restore "$tmp" --source "$packages" --source "$nuget"
"$dotnet" build "$tmp" --no-restore
echo "The README compiles against the packed packages."

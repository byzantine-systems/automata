---
sut_path: /home/mbenevides/Code/Personal/byzantine-systems/automata
commit: 9bdc555187e735cdf31431de3515832d3d0fc9d3
updated: 2026-11-12
external_references: []
---

# Existing Antithesis SDK Assertions

Scan performed 2026-11-12 over the whole repository (`src/`, `tests/`,
`examples/`, `tools/`, `*.csproj/*.fsproj`, workflows, docs) for imports of an
Antithesis SDK and calls to `assert_always` / `assert_sometimes` /
`assert_reachable` / `assert_unreachable` or non-macro equivalents.

**Result: none found.** The codebase has no Antithesis instrumentation of any
kind; no package reference to an Antithesis SDK exists.

Consequences for the workflow:

- Every property in `property-catalog.md` is currently an *unimplemented*
  assertion; instrumentation points listed in evidence files describe what is
  **missing**, never what already exists.
- `antithesis-mutation-testing` cannot start until the harness, workload and
  assertions exist (its prerequisites gate it out today).

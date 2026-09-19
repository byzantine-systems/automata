# ByzantineSystems.Automata

[![Built with Nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)

A typed hierarchical state-machine toolkit for F# and .NET 10. Pure chart resolution is separated from actor-based execution, resilience policy, and optional durable storage.

The name "statechart" comes from David Harel's 1987 paper *Statecharts: A Visual Formalism for Complex Systems*, which introduced the visual notation this toolkit implements as a typed F# computation expression.

## Packages

| Package | Implemented surface |
| --- | --- |
| `ByzantineSystems.Automata.Core` | Typed identifiers, transition rules, validated hierarchical charts, and pure event resolution |
| `ByzantineSystems.Automata.Resilience` | Polly retry, timeout, and circuit-breaker configuration plus final failure dispositions |
| `ByzantineSystems.Automata.Storage` | State, history, retry queue, dead-letter, and action-outbox contracts |
| `ByzantineSystems.Automata.Storage.InMemory` | Thread-safe in-memory implementation of the storage contracts |
| `ByzantineSystems.Automata.Storage.Postgres` | PostgreSQL-backed storage, JSON codecs, state-path projection, and embedded DbUp migrations |
| `ByzantineSystems.Automata.Runtime` | Per-entity actor execution, idempotent sends, durable retry and outbox pumps, observers, and machine lifecycle |
| `ByzantineSystems.Automata.DependencyInjection` | Hosted `BackgroundService` supervision with keyed Polly pipelines, scoped action handlers, and audit persistence |

## Public API

Charts can be created with `Chart.create` or the `statechart` computation expression. The computation expression exposes `root`, `classify`, `state`, `compound`, `initial`, `terminal`, `onEntry`, `onExit`, `on`, `attempt`, `goto`, and `internalOn`; construction returns `Result<Chart<_,_,_,_>, ChartError list>`.

Machines are created with the `machine` computation expression from a validated chart, an initial state, an `IMachineStore`, and a `RetryConfig`. Optional configuration includes a durable retry policy, transition observation, mailbox capacity, idle timeout, and a `TimeProvider`. Construction returns accumulated `MachineConfigError` values. Use `Machine.send`, `Machine.state`, `Machine.machineId`, and `Machine.stopAsync` to interact with a machine.

Storage is available through the narrow `IStateStore`, `IRetryQueue`, `IDeadLetterStore`, and `IActionOutbox` interfaces or the combined `IMachineStore`. `InMemoryStore` implements the combined contract. `PostgresStore` accepts `StoreOptions`, and `Migrator.migrate` applies its embedded schema.

`AddAutomata` registers a supervised hosted service from typed `AutomataOptions`: a keyed Polly pipeline built once, a root supervisor owning machine generations, an optional scoped `IActionHandler` for the transactional outbox, and an `ISupervisionEventStore` audit sink (`InMemorySupervisionStore` for tests, `PostgresSupervisionStore` for durability).

See the [documentation](docs/index.md), [public API guide](docs/public-api.md), and [schema evolution guide](docs/schema-evolution.md). Runnable demonstrations live under [`examples/`](examples/); links are intentionally directory-level so example names can grow without changing this overview.

## Development

The repository pins the .NET 10 SDK and provides Nix development shells.

```console
make build
make test
make run-example
make run-example-supervision
make docs
make package-smoke
```

`make test-integration` runs PostgreSQL tests when `AUTOMATA_TEST_DB` is configured. `make migrate` applies migrations using `BS_AUTOMATA_CONN`, and `make db-reset` resets the local disposable schema.

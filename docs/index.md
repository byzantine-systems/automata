# ByzantineSystems.Automata documentation

`ByzantineSystems.Automata` is a typed hierarchical **statechart** toolkit for F# and .NET. It separates pure chart resolution from durable execution: a chart decides, and PostgreSQL orders, records and delivers what it decided. The name **"statechart"** comes from David Harel's 1987 paper _Statecharts: A Visual Formalism for Complex Systems_, which introduced the visual notation this toolkit implements as a typed F# computation expression.

> **Statecharts** constitute a visual formalism for describing states and transitions in a modular fashion, enabling clustering, orthogonality (i.e., concurrency) and refinement, and encouraging 'zoom' capabilities for moving easily back and forth between levels of abstraction.

## How it fits together

- **The chart is pure.** `Chart.resolve` takes a state and an event and returns the next state and the actions to run, or a typed error. Nothing in it touches a clock, a database or a thread.
- **The inbox orders the work.** `Machine.enqueue` records an event in `fsm.command` and returns. At most one command per entity is claimable at a time, so claiming one excludes that entity on every host, while unrelated entities make progress concurrently.
- **A commit is one transaction.** The processor claims a command, resolves it against the chart, and finalizes it: the transition is appended, the belief advanced, the actions queued on pgmq and the command closed, all or nothing.
- **Actions are delivered at least once**, by a dispatcher that fences every acknowledgement against the delivery that claimed it.
- **Beliefs have two time axes.** What was true and when it was believed are recorded separately, so the past can be read as it was believed at any moment, and corrected without erasing the opinion it replaces.
- **Maintenance is part of the library.** A boot check refuses to start against a schema that cannot serve, and retention, lease reaping, drift reports and cross-process wake-ups run on pg_cron or in the host.

## Start here

- The repository README has the install steps, a chart and a complete hosted setup.
- [Public API guide](public-api.md) covers the implemented public types and modules.
- [Chart fragments](fragments.md) shows how to build charts from reusable pieces.
- [Schema evolution](schema-evolution.md) explains the PostgreSQL schema, its routines, and how maintenance works.

Runnable demonstrations live under `examples/`:

- `Hosted`: the production shape. A Generic Host with supervised processing and delivery, maintenance, and a caller, around a chart built from one fragment used twice. `make run-example-hosted`.
- `PaymentProcessor`: the same pieces wired by hand, without dependency injection. `make run-example`.
- `Supervision`: the supervisor on its own, restarting a child that crashes. `make run-example-supervision`.
- `readme.fsx`: the README's chart, resolved in a script with nothing but `Core`.

The projects target .NET 10 and PostgreSQL 19, with the `btree_gist` and `pgmq` extensions. `pg_cron` is optional.

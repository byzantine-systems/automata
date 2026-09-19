# ByzantineSystems.Automata documentation

`ByzantineSystems.Automata` is a typed hierarchical state-machine toolkit for F# and .NET. It separates pure chart resolution from actor-based execution, resilience policy, and optional durable storage.

The name "statechart" comes from David Harel's 1987 paper *Statecharts: A Visual Formalism for Complex Systems*, which introduced the visual formalism this toolkit models.

## Start here

- [Public API guide](public-api.md) covers the currently implemented public types and modules.
- [Schema evolution](schema-evolution.md) explains how the PostgreSQL schema and embedded migrations are maintained.
- The repository README summarizes packages, supported behavior, and development commands.

Runnable demonstrations live under `examples/`: a hierarchical payment chart (in-memory or PostgreSQL) and supervised crash-and-restart.

The projects target .NET 10. The in-memory store is suitable for tests and local use; the PostgreSQL store provides durable state, transition history, retries, dead letters, and an action outbox.

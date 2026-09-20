# ByzantineSystems.Automata

[![Built with Nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)
[![NuGet](https://img.shields.io/nuget/v/ByzantineSystems.Automata.Core)](https://www.nuget.org/packages/ByzantineSystems.Automata.Core)
![License](https://img.shields.io/github/license/byzantine-systems/automata)

[![Build](https://github.com/byzantine-systems/automata/actions/workflows/build.yml/badge.svg)](https://github.com/byzantine-systems/automata/actions/workflows/build.yml)
![Coverage](https://byzantine-systems.github.io/automata/coverage.svg)

`ByzantineSystems.Automata` is a strongly typed [statechart](https://en.wikipedia.org/wiki/State_diagram#Harel_statechart) toolkit for F# and .NET 10, whose main goal is to allow you to:

- Model states, events, actions, and domain errors with ordinary F# types.
- Validate the statechart once.
- Resolve transitions with a pure core and execute the same model through isolated per-entity actors.

This library is designed to keep domain behavior independent from runtime and infrastructure concerns:

- **Typed and validated charts**: hierarchical and terminal states, guarded transitions, entry and exit actions, event bubbling, and accumulated construction errors.
- **Deterministic core**: `Chart.resolve` is a pure function, so transition behavior can be tested without actors, databases, clocks, or dependency injection.
- **Entity-level concurrency**: each entity has a serialized mailbox while unrelated entities can make progress concurrently; idempotency keys and optimistic epochs protect committed transitions.
- **Composable durability**: storage contracts cover state, transition history, durable retries, dead letters, and a transactional action outbox, with in-memory and PostgreSQL implementations.
- **Operational resilience**: Polly pipelines handle short-lived failures, durable retry handles longer delays, and supervision policies manage machine restarts and escalation.

Use only the pure chart library, assemble a custom runtime from the smaller packages, or host a complete supervised machine with PostgreSQL persistence and .NET dependency injection.

## Install

Install only the layers your application needs. For pure chart construction and resolution:

```shell
dotnet add package ByzantineSystems.Automata.Core
```

To execute charts with the in-memory store:

```shell
dotnet add package ByzantineSystems.Automata.Runtime
dotnet add package ByzantineSystems.Automata.Storage.InMemory
```

## Example

### Define a chart

This is the basic two-state light switch from the [statecharts.dev on/off example](https://statecharts.dev/on-off-statechart.html): every `Flick` toggles the state, entering `On` emits `TurnLightOn`, and leaving it emits `TurnLightOff`.

```fsharp
open ByzantineSystems.Automata.Core

type SwitchState =
    | Off
    | On

type SwitchEvent = Flick

type SwitchAction =
    | TurnLightOn
    | TurnLightOff

let switchChart =
    statechart<SwitchState, SwitchEvent, SwitchAction, string> {
        root "switch"

        classify (function
            | Off -> stateId "off"
            | On -> stateId "on")

        state "off" {
            on (fun _ event -> event = Flick) (fun _ _ -> [], On)
        }

        state "on" {
            onEntry (fun _ _ -> [ TurnLightOn ])
            onExit (fun _ _ -> [ TurnLightOff ])
            on (fun _ event -> event = Flick) (fun _ _ -> [], Off)
        }
    }
    |> function
        | Ok chart -> chart
        | Error errors -> invalidOp $"Invalid switch chart: %A{errors}"
```

The computation expression returns `Result<Chart<_,_,_,_>, ChartError list>` and supports leaf and compound states, initial and terminal states, entry/exit actions, ordinary or fallible transitions, compound targets, and internal transitions. `Chart.create` provides the equivalent lower-level API.

Run the chart-only example directly from the repository with `dotnet fsi examples/readme.fsx`; the complete script is at [`examples/readme.fsx`](examples/readme.fsx).

### Run the chart

Compose the validated chart with storage and resilience policy using the `machine` computation expression:

```fsharp
open System.Threading
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory

type LightSwitch = class end
type SwitchId = EntityId<LightSwitch>

let machineStore =
    InMemoryStore<SwitchId, SwitchState, SwitchEvent, SwitchAction>()
    :> IMachineStore<SwitchId, SwitchState, SwitchEvent, SwitchAction>

let switchMachine =
    machine<SwitchId, SwitchState, SwitchEvent, SwitchAction, string> (machineId "light-switches") {
        chart switchChart
        initialState Off
        store machineStore
        retry RetryConfig.defaults<string>
    }
    |> function
        | Ok machine -> machine
        | Error errors -> invalidOp $"Invalid machine configuration: %A{errors}"

let flickSwitch () =
    task {
        let cancellation = CancellationToken.None
        let hallway: SwitchId = entityId "hallway"

        do! Machine.startAsync switchMachine cancellation

        let! outcome =
            Machine.send
                switchMachine
                hallway
                (EventEnvelope.create "hallway-flick-1" Flick)
                cancellation

        let! current = Machine.state switchMachine hallway cancellation
        do! Machine.stopAsync switchMachine cancellation

        return outcome, current
    }
```

See the [documentation](docs/index.md), [public API guide](docs/public-api.md), and [schema evolution guide](docs/schema-evolution.md). Runnable demonstrations live under [`examples/`](examples/).

## Packages

The toolkit is published as focused building blocks. Start with `Core` for pure modeling, add the runtime and the storage implementation your application needs, and opt into Polly resilience, PostgreSQL durability, or hosted-service integration independently. The abstractions remain public at each boundary, so applications can replace infrastructure without rewriting their charts.

| Package | Implemented surface |
| --- | --- |
| `ByzantineSystems.Automata.Core` | Typed identifiers, transition rules, validated hierarchical charts, and pure event resolution |
| `ByzantineSystems.Automata.Resilience` | Polly retry, timeout, and circuit-breaker configuration plus final failure dispositions |
| `ByzantineSystems.Automata.Storage` | State, history, retry queue, dead-letter, and action-outbox contracts |
| `ByzantineSystems.Automata.Storage.InMemory` | Thread-safe in-memory implementation of the storage contracts |
| `ByzantineSystems.Automata.Storage.Postgres` | PostgreSQL-backed storage, JSON codecs, state-path projection, and embedded DbUp migrations |
| `ByzantineSystems.Automata.Runtime` | Per-entity actor execution, idempotent sends, durable retry and outbox pumps, observers, and machine lifecycle |
| `ByzantineSystems.Automata.DependencyInjection` | Hosted `BackgroundService` supervision with keyed Polly pipelines, scoped action handlers, and audit persistence |

## Development

The project uses [devenv.sh](https://devenv.sh/), so you don't need a local .NET installation. To start a development shell:

```shell
nix develop --impure
# or
direnv allow .
```

To build the examples purely with Nix:

```shell
nix build
```

There is also a `Makefile` to control most of the development/testing workflows:

```console
make build
make test
make run-example
make run-example-supervision
make coverage
make docs
make package-smoke
```

If you have a local PostgreSQL server running:

- `make test-integration` runs PostgreSQL tests when `AUTOMATA_TEST_DB` is configured.
- `make coverage` runs every test project and generates merged Cobertura and HTML reports.
- `make migrate` applies migrations using `BS_AUTOMATA_CONN`.
- `make db-reset` resets the local disposable schema.

## References

- The name "statechart" comes from David Harel's 1987 paper *Statecharts: A Visual Formalism for Complex Systems*, which introduced the visual notation this toolkit implements as a typed F# computation expression.

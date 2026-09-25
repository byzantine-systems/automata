module ByzantineSystems.Automata.Examples.PaymentProcessor.Program

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

/// Phantom marker tying entity ids to the payment domain.
type Payment = class end
type Entity = EntityId<Payment>

type PaymentState =
    | Idle
    | Processing of amount: decimal * customer: string
    | AwaitingConfirmation of reference: string
    | Captured of reference: string
    | Failed of reason: string
    | Cancelled of reason: string

type PaymentEvent =
    | InitiatePayment of amount: decimal * customer: string
    | GatewayProcessed of reference: string
    | ConfirmationReceived of reference: string
    | GatewayFailed of reason: string
    | Cancel

type PaymentAction =
    | ReserveGatewaySlot
    | ReleaseGatewaySlot
    | NotifyCustomer of message: string

/// The payment chart: a compound `active` region with inherited `Cancel`
/// and `GatewayFailed` handling, and a nested `settled` compound reached via `captured`.
let private paymentChart: Result<Chart<PaymentState, PaymentEvent, PaymentAction, string>, ChartError list> =
    statechart<PaymentState, PaymentEvent, PaymentAction, string> {
        root "root"

        classify (function
            | Idle -> stateId "idle"
            | Processing _ -> stateId "active.processing"
            | AwaitingConfirmation _ -> stateId "active.awaiting"
            | Captured _ -> stateId "active.settled.captured"
            | Failed _ -> stateId "failed"
            | Cancelled _ -> stateId "cancelled")

        state "idle" {
            on
                (fun _ e ->
                    match e with
                    | InitiatePayment _ -> true
                    | _ -> false)
                (fun _ e ->
                    match e with
                    | InitiatePayment(amount, customer) -> [], Processing(amount, customer)
                    | _ -> [], Idle)
        }

        compound "active" {
            initial "active.processing"
            onEntry (fun _ _ -> [ ReserveGatewaySlot ])
            onExit (fun _ _ -> [ ReleaseGatewaySlot ])

            // Inherited by every descendant: one Cancel rule handles the whole region.
            on (fun _ e -> e = Cancel) (fun _ _ -> [ NotifyCustomer "cancelled" ], Cancelled "cancelled")

            // A gateway failure is handled once, on the compound, from any descendant.
            on
                (fun _ e ->
                    match e with
                    | GatewayFailed _ -> true
                    | _ -> false)
                (fun _ e ->
                    match e with
                    | GatewayFailed reason -> [], Failed reason
                    | _ -> [], Idle)

            state "active.processing" {
                on
                    (fun _ e ->
                        match e with
                        | GatewayProcessed _ -> true
                        | _ -> false)
                    (fun _ e ->
                        match e with
                        | GatewayProcessed reference -> [], AwaitingConfirmation reference
                        | _ -> [], Idle)
            }

            state "active.awaiting" {
                on
                    (fun _ e ->
                        match e with
                        | ConfirmationReceived _ -> true
                        | _ -> false)
                    (fun _ e ->
                        match e with
                        | ConfirmationReceived reference -> [ NotifyCustomer "captured" ], Captured reference
                        | _ -> [], Idle)
            }

            compound "active.settled" {
                initial "active.settled.captured"
                state "active.settled.captured" { }
            }
        }

        state "failed" { terminal }
        state "cancelled" { terminal }
    }

let private paymentChartValue =
    match paymentChart with
    | Ok chart -> chart
    | Error errors -> failwith $"payment chart failed to construct: %A{errors}"

/// Logs each committed transition with structured state, event, and action properties.
let private logTransition
    (logger: ILogger)
    (transition: CommittedTransition<Entity, PaymentState, PaymentEvent, PaymentAction>)
    (_ct: CancellationToken)
    =
    task {
        logger.LogInformation(
            "Payment transition {FromState} --{PaymentEvent}--> {ToState} at epoch {Epoch}; actions={Actions}",
            transition.Draft.FromState,
            transition.Draft.Event,
            transition.Draft.ToState,
            Epoch.value transition.Epoch,
            transition.Draft.Actions
        )
    }

/// The queue this machine's actions are delivered through, one per machine. Named once, on the
/// store that creates it.
[<Literal>]
let private ActionQueue = "payment_actions"

let private buildMachine
    (log: ILogger)
    (storeArg: IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction, string>)
    =
    machine<Entity, PaymentState, PaymentEvent, PaymentAction, string> (machineId "payments") {
        chart paymentChartValue
        // Declared, never inferred. Every command records the version it was resolved under, so
        // a replay resolves it against the chart that originally decided it. Editing the chart's
        // shape without bumping this is what the fingerprint check at startup catches.
        chartVersion 1
        initialState Idle
        store storeArg
        onTransition (logTransition log)
        logger log
        timeProvider TimeProvider.System
    }

let private readConnectionString () : string option =
    [ "BS_AUTOMATA_CONN"; "ConnectionStrings__BS_AUTOMATA_CONN" ]
    |> List.map Environment.GetEnvironmentVariable
    |> List.tryFind (String.IsNullOrWhiteSpace >> not)

/// <summary>
/// Runs one payment through the durable path.
///
/// Nothing here processes a command. Submitting records it in <c>fsm.command</c> and returns; a
/// processor claims it, resolves it against the chart, and finalizes it. This example runs that
/// processor in the background, which is what a host's worker would do, so the two halves are
/// visible side by side.
/// </summary>
let private runPayment (logger: ILogger) (connectionString: string) : Task =
    task {
        let context =
            PostgresContext.create (DataSource.create connectionString) TransientPolicy.defaults ignore

        // Every default spelled out in one function: JSON codecs, EntityId keys, nothing deleted,
        // leases reaped five minutes after they expire. Override any of them with { ... with }.
        let store =
            MachineStoreOptions.forEntityId<Payment, PaymentState, PaymentEvent, PaymentAction, string>
                context
                ActionQueue
            |> PostgresMachineStore

        let machine =
            match buildMachine logger (store :> IMachineStore<_, _, _, _, _>) with
            | Ok machine -> machine
            | Error errors -> failwith $"machine failed to construct: %A{errors}"

        let registry = PostgresChartRegistry({ Context = context })

        // Starting boots the store first: it checks the schema and extensions, creates the
        // action queue if this is its first run, and registers the machine for maintenance.
        // Nothing starts if any of that is missing.
        match! Machine.startAsync machine registry CancellationToken.None with
        | Ok(Startup.Started ChartRegistration.Registered) ->
            logger.LogInformation "Registered this chart's structure for version 1"
        | Ok(Startup.Started ChartRegistration.Matched) ->
            logger.LogInformation "This chart matches the structure version 1 was registered with"
        | Ok(Startup.Started(ChartRegistration.Mismatched _)) ->
            failwith "this chart's shape changed without a version bump; bump chartVersion or run make db-reset"
        | Ok(Startup.Refused defects) -> failwith $"the database is not ready: %A{defects}; run make migrate"
        | Error error -> failwith $"the chart could not be registered: %A{error}"

        // The worker. In a host this is a BackgroundService under supervision; here it is one
        // task, cancelled when the payment is done.
        use worker = new CancellationTokenSource()
        let processor = Machine.processor machine
        let draining = processor.RunAsync worker.Token

        let order: Entity =
            entityId $"order-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"

        let steps =
            [ "create-1", InitiatePayment(150.00m, "alice")
              "process-1", GatewayProcessed "gw-42"
              "confirm-1", ConfirmationReceived "gw-42" ]

        for key, event in steps do
            // send is enqueue plus a wait for the durable outcome. It costs a round trip through
            // the database, which is the price of an order that survives this process; a caller
            // that cares more about throughput uses enqueue and reads the result later.
            let! outcome = Machine.send machine order (EventEnvelope.create key event) CancellationToken.None

            match outcome with
            | Ok(CommandResult.Committed committed) ->
                logger.LogInformation("Committed {PaymentEvent} at epoch {Epoch}", event, Epoch.value committed.Epoch)
            | Ok(CommandResult.Rejected failure) ->
                logger.LogWarning("Refused {PaymentEvent}: {Failure}", event, failure)
            | Ok(CommandResult.DeadLettered failure) ->
                logger.LogError("Gave up on {PaymentEvent}: {Failure}", event, failure)
            | Ok CommandResult.Pending -> logger.LogWarning("{PaymentEvent} is still pending", event)
            | Error error -> logger.LogWarning("Payment event {PaymentEvent} failed: {MachineError}", event, error)

        let! snapshot = Machine.state machine order CancellationToken.None

        match snapshot with
        | Ok(Some current) ->
            logger.LogInformation(
                "Payment finished in state {PaymentState} at epoch {Epoch}",
                current.State,
                Epoch.value current.Epoch
            )
        | Ok None -> logger.LogWarning("Payment finished without a stored snapshot")
        | Error error -> logger.LogError("Payment state read failed: {MachineError}", error)

        worker.Cancel()
        let! _ = draining
        do! Machine.stopAsync machine CancellationToken.None
    }

let private run (logger: ILogger) (_argv: string array) =
    task {
        match readConnectionString () with
        | None ->
            // The durable authority is the point of this library, so the example needs one. It
            // says so rather than failing somewhere inside Npgsql.
            logger.LogError
                "Set BS_AUTOMATA_CONN to a PostgreSQL connection string, then run `make migrate` before this example."

            return 1
        | Some connectionString ->
            match Migrator.migrate logger connectionString with
            | Error(MigrationError.Failed(script, error)) ->
                logger.LogError(error, "Migration failed at {Script}", script)
                return 1
            | Ok _ ->
                do! runPayment logger connectionString
                return 0
    }

[<EntryPoint>]
let main argv =
    let builder = Host.CreateApplicationBuilder(argv)
    use host = builder.Build()

    let logger =
        host.Services
            .GetRequiredService<ILoggerFactory>()
            .CreateLogger("ByzantineSystems.Automata.Examples.PaymentProcessor")

    try
        (run logger argv).GetAwaiter().GetResult()
    with ex ->
        logger.LogError(ex, "Payment processor example failed")
        1

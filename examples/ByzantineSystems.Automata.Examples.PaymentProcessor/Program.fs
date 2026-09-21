module ByzantineSystems.Automata.Examples.PaymentProcessor.Program

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.InMemory
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
    (transition: Transition<Entity, PaymentState, PaymentEvent, PaymentAction>)
    (_ct: CancellationToken)
    =
    task {
        logger.LogInformation(
            "Payment transition {FromState} --{PaymentEvent}--> {ToState}; actions={Actions}",
            transition.FromState,
            transition.Event,
            transition.ToState,
            transition.Actions
        )
    }

let private buildMachine
    (logger: ILogger)
    (storeArg: IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction>)
    =
    machine<Entity, PaymentState, PaymentEvent, PaymentAction, string> (machineId "payments") {
        chart paymentChartValue
        initialState Idle
        store storeArg
        retry RetryConfig.defaults<string>
        onTransition (logTransition logger)
        timeProvider TimeProvider.System
    }

/// Runs a payment lifecycle against an in-memory or PostgreSQL store.
let private runPayment
    (logger: ILogger)
    (storeArg: IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction>)
    : Task =
    task {
        let machine =
            match buildMachine logger storeArg with
            | Ok machine -> machine
            | Error errors -> failwith $"machine failed to construct: %A{errors}"

        do! Machine.startAsync machine CancellationToken.None

        let order: Entity = entityId "order-1001"

        let steps =
            [ "create-1", InitiatePayment(150.00m, "alice")
              "process-1", GatewayProcessed "gw-42"
              "confirm-1", ConfirmationReceived "gw-42" ]

        for key, event in steps do
            let! outcome = Machine.send machine order (EventEnvelope.create key event) CancellationToken.None

            match outcome with
            | Ok Committed ->
                logger.LogInformation("Sent payment event {PaymentEvent}; outcome={SendOutcome}", event, "Committed")
            | Ok AlreadyApplied ->
                logger.LogInformation(
                    "Sent payment event {PaymentEvent}; outcome={SendOutcome}",
                    event,
                    "AlreadyApplied"
                )
            | Ok(Deferred retryId) ->
                logger.LogInformation(
                    "Sent payment event {PaymentEvent}; outcome={SendOutcome}; retry={RetryId}",
                    event,
                    "Deferred",
                    RetryId.value retryId
                )
            | Ok Ignored ->
                logger.LogInformation("Sent payment event {PaymentEvent}; outcome={SendOutcome}", event, "Ignored")
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

        do! Machine.stopAsync machine CancellationToken.None
    }

let private buildInMemory () : IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction> =
    InMemoryStore<Entity, PaymentState, PaymentEvent, PaymentAction>()
    :> IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction>

let private readConnectionString () : string option =
    [ "BS_AUTOMATA_CONN"; "ConnectionStrings__BS_AUTOMATA_CONN" ]
    |> List.map Environment.GetEnvironmentVariable
    |> List.tryFind (String.IsNullOrWhiteSpace >> not)

let private buildPostgres () : IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction> =
    let connectionString =
        match readConnectionString () with
        | Some value -> value
        | None -> failwith "the --postgres flag requires BS_AUTOMATA_CONN to be set"

    Migrator.migrate connectionString

    let dataSource = PostgresStore.dataSource connectionString
    let encode, decode = EntityKey.forEntityId<Payment>

    PostgresStore<Entity, PaymentState, PaymentEvent, PaymentAction>(
        { DataSource = dataSource
          StateCodec = Serialization.systemTextJson<PaymentState> ()
          EventCodec = Serialization.systemTextJson<PaymentEvent> ()
          ActionCodec = Serialization.systemTextJson<PaymentAction> ()
          ActionListCodec = Serialization.systemTextJsonList<PaymentAction> ()
          EntityIdEncode = encode
          EntityIdDecode = decode
          StatePath = StatePath.ofChart paymentChartValue
          TimeProvider = TimeProvider.System }
    )
    :> IMachineStore<Entity, PaymentState, PaymentEvent, PaymentAction>

let private run (logger: ILogger) argv =
    task {
        let usePostgres = argv |> Array.contains "--postgres"
        let store = if usePostgres then buildPostgres () else buildInMemory ()
        do! runPayment logger store
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
        0
    with ex ->
        logger.LogError(ex, "Payment processor example failed")
        1

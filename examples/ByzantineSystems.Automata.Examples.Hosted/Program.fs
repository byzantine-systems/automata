module ByzantineSystems.Automata.Examples.Hosted.Program

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.DependencyInjection
open ByzantineSystems.Automata.Resilience
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

// ---------------------------------------------------------------------------
// The domain: a document that needs legal and then finance sign-off before it is published.
// ---------------------------------------------------------------------------

/// Phantom marker tying entity ids to documents.
type Document = class end
type DocumentId = EntityId<Document>

type Stage =
    | Drafting
    | LegalPending
    | LegalRejected
    | FinancePending
    | FinanceRejected
    | Published

type Review =
    | Submit
    | Approve
    | Reject of reason: string
    | Revise

type Notice = Notify of recipient: string * message: string

/// <summary>
/// One sign-off, written once and used twice.
///
/// It is a function of what differs between its two uses: who reviews, which state a rejection
/// lands in, and where an approval goes next. The node names inside it are the same both times,
/// which is what <c>Fragment.prefix</c> is for below. Revising goes to <c>drafting</c>, a state the
/// host declares, and the prefix leaves that target alone because the fragment does not declare it.
/// </summary>
let approval (reviewer: string) (rejected: Stage) (approved: Stage) : NodeDraft<Stage, Review, Notice, string> =
    compound "approval" {
        initial "pending"

        state "pending" {
            on (fun _ review -> review = Approve) (fun _ _ -> [ Notify(reviewer, "approved") ], approved)

            on
                (fun _ review ->
                    match review with
                    | Reject _ -> true
                    | _ -> false)
                (fun _ review ->
                    match review with
                    | Reject reason -> [ Notify("author", $"{reviewer} rejected it: {reason}") ], rejected
                    | _ -> [], rejected)
        }

        state "rejected" { goto (fun _ review -> review = Revise) (stateId "drafting") (fun _ _ -> [], Drafting) }
    }

let documentChart =
    statechart<Stage, Review, Notice, string> {
        root "document"

        // The classifier is this chart's own code, so it names the prefixed ids.
        classify (function
            | Drafting -> stateId "drafting"
            | LegalPending -> stateId "legal.pending"
            | LegalRejected -> stateId "legal.rejected"
            | FinancePending -> stateId "finance.pending"
            | FinanceRejected -> stateId "finance.rejected"
            | Published -> stateId "published")

        state "drafting" {
            goto (fun _ review -> review = Submit) (stateId "legal.approval") (fun _ _ -> [], LegalPending)
        }

        yield!
            [ Fragment.prefix "legal" (approval "legal" LegalRejected FinancePending)
              Fragment.prefix "finance" (approval "finance" FinanceRejected Published) ]

        state "published" { terminal }
    }
    |> function
        | Ok chart -> chart
        | Error errors -> invalidOp $"the document chart is invalid: %A{errors}"

/// <summary>
/// The machine, built the same way by everything that needs one. The store owns the action queue,
/// so the queue is named once, here.
/// </summary>
let documents (log: ILogger) (context: PostgresContext) =
    machine<DocumentId, Stage, Review, Notice, string> (machineId "documents") {
        chart documentChart
        chartVersion 1
        initialState Drafting

        store (
            MachineStoreOptions.forEntityId<Document, Stage, Review, Notice, string> context "document_notices"
            |> PostgresMachineStore
        )

        logger log
    }

// ---------------------------------------------------------------------------
// The host's side of delivery, and a caller that submits work.
// ---------------------------------------------------------------------------

/// <summary>
/// Where a notice actually goes. Scoped, so a real one can take scoped dependencies; a fresh scope
/// is created per delivery. Delivery is at least once, so a real destination would deduplicate on
/// the action's <c>(CommandId, Ordinal)</c>.
/// </summary>
type Notifier(logger: ILogger<Notifier>) =
    interface IActionHandler<DocumentId, Notice, string> with
        member _.HandleAsync(action, _) =
            let (Notify(recipient, message)) = action.Work.Action

            logger.LogInformation(
                "Notified {Recipient} about {Document}: {Message}",
                recipient,
                EntityId.value action.Work.EntityId,
                message
            )

            Task.FromResult(Ok())

/// <summary>
/// A caller, with no processor of its own.
///
/// It builds a machine only to submit through it; the supervised generation registered by
/// <c>AddAutomata</c> is what claims and decides each command. The two meet in the database, so
/// this could as well be a web endpoint in another process. <c>Machine.send</c> waits for the
/// durable result, which is the round trip a caller pays for an outcome that survives a crash.
/// </summary>
type Reviewer(context: PostgresContext, lifetime: IHostApplicationLifetime, loggers: ILoggerFactory) =
    inherit BackgroundService()

    let logger = loggers.CreateLogger<Reviewer>()

    let review machine document (key: string, event: Review) ct =
        backgroundTask {
            match! Machine.send machine document (EventEnvelope.create key event) ct with
            | Ok(CommandResult.Committed committed) ->
                logger.LogInformation(
                    "{Review} took {Document} to {Stage}",
                    event,
                    EntityId.value document,
                    committed.Draft.ToState
                )
            | Ok other -> logger.LogWarning("{Review} ended as {Outcome}", event, other)
            | Error error -> logger.LogError("{Review} failed: {Error}", event, error)
        }

    override _.ExecuteAsync(ct: CancellationToken) =
        backgroundTask {
            match documents (loggers.CreateLogger "documents") context with
            | Error errors -> logger.LogError("The document machine is misconfigured: {Errors}", errors)
            | Ok machine ->
                let! _ = Machine.startAsync machine (PostgresChartRegistry { Context = context }) ct

                let document: DocumentId =
                    entityId $"doc-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"

                // Rejected by legal, revised, and then through both sign-offs. Each key is the
                // caller's idempotency key: repeating a send with the same one is recognised,
                // not applied twice.
                let reviews =
                    [ "submit-1", Submit
                      "legal-1", Reject "missing a clause"
                      "revise-1", Revise
                      "submit-2", Submit
                      "legal-2", Approve
                      "finance-1", Approve ]

                for step in reviews do
                    do! review machine document step ct

                do! Machine.stopAsync machine ct

            lifetime.StopApplication()
        }

let private readConnectionString () : string option =
    [ "BS_AUTOMATA_CONN"; "ConnectionStrings__BS_AUTOMATA_CONN" ]
    |> List.map Environment.GetEnvironmentVariable
    |> List.tryFind (String.IsNullOrWhiteSpace >> not)

/// <summary>
/// The whole production shape: one context, supervised processing and delivery, database
/// maintenance, and a caller. Everything a host registers is here and nothing else is needed.
/// </summary>
let configure (services: IServiceCollection) (context: PostgresContext) =
    services
        .AddSingleton(context)
        .AddSingleton<ISupervisionEventStore>(PostgresSupervisionStore context)
        .AddScoped<IActionHandler<DocumentId, Notice, string>, Notifier>()
        .AddAutomata(
            { MachineKey = "documents"
              Supervisor = AutomataSupervisorOptions.defaults "documents"
              Actions = ActionDelivery.registered<DocumentId, Notice, string>
              MachineFactory =
                fun provider ->
                    documents (provider.GetRequiredService<ILoggerFactory>().CreateLogger "documents") context
              ChartRegistry = fun _ -> PostgresChartRegistry { Context = context }
              TimeProvider = TimeProvider.System }
        )
        // Asks pg_cron to run maintenance, and runs it here instead when this role may not.
        .AddAutomataMaintenance(
            { MaintenanceOptions.defaults (fun _ -> PostgresMaintenance context) with
                Scheduler = MaintenanceScheduler.InDatabase }
        )
        .AddHostedService<Reviewer>()
    |> ignore

[<EntryPoint>]
let main argv =
    match readConnectionString () with
    | None ->
        eprintfn "Set BS_AUTOMATA_CONN to a PostgreSQL connection string, then run `make migrate`."
        1
    | Some connectionString ->
        let builder = Host.CreateApplicationBuilder(argv)
        configure builder.Services (PostgresContext.ofConnectionString connectionString)
        use host = builder.Build()

        let logger =
            host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("ByzantineSystems.Automata.Migrate")

        // Migrated before the host starts, through the host's own logging, so a schema that
        // cannot be brought up to date stops everything before any worker boots against it.
        match Migrator.migrate logger connectionString with
        | Error(MigrationError.Failed(script, error)) ->
            logger.LogError(error, "Migration failed at {Script}", script)
            1
        | Ok _ ->
            host.Run()
            0

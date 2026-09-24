namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Globalization
open System.Reflection
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open FsToolkit.ErrorHandling
open Npgsql

/// <summary>What the catalog says about a database, before anything in it is trusted.</summary>
type internal CatalogCheck =
    { HasSchema: bool
      HasJournal: bool
      HasRoutines: bool
      HasBtreeGist: bool
      HasPgmq: bool }

/// <summary>
/// Deciding whether a database can serve, as pure functions over what was read from it, so every
/// combination can be tested without breaking a shared database to produce it.
/// </summary>
[<RequireQualifiedAccess>]
module internal Boot =

    /// <summary>
    /// The numbered migrations this build embeds, by the resource name DbUp journals them under.
    /// </summary>
    let expectedMigrations: string list =
        Assembly.GetExecutingAssembly().GetManifestResourceNames()
        |> Array.filter (fun name -> name.Contains ".migrations.main.")
        |> Array.sort
        |> List.ofArray

    /// <summary>
    /// Compares what this build ships with what the database applied. Behind and ahead are both
    /// reported, because a database can be both at once when two branches' migrations meet.
    /// </summary>
    let compare (expected: string list) (applied: string list) : BootDefect list =
        let pending = expected |> List.except applied
        let unknown = applied |> List.except expected

        [ if not pending.IsEmpty then
              BootDefect.SchemaBehind pending
          if not unknown.IsEmpty then
              BootDefect.SchemaAhead unknown ]

    /// <summary>
    /// Every defect the catalog and the journal show. The journal is <c>None</c> when it does not
    /// exist, which is the answer <see cref="T:ByzantineSystems.Automata.Storage.Postgres.CatalogCheck" />
    /// gave before anyone tried to read it.
    /// </summary>
    let defects (check: CatalogCheck) (applied: string list option) (expected: string list) : BootDefect list =
        [ if not check.HasBtreeGist then
              BootDefect.MissingPrerequisite "btree_gist"
          if not check.HasPgmq then
              BootDefect.MissingPrerequisite "pgmq"
          if not check.HasSchema then
              BootDefect.SchemaMissing
          else
              match applied with
              | None -> BootDefect.MissingPrerequisite "migration journal public.schemaversions"
              | Some applied -> yield! compare expected applied

              // A schema whose numbered migrations all ran but whose routines are missing is one
              // whose repeatable scripts never did, and nothing it could be asked would work.
              if not check.HasRoutines then
                  BootDefect.MissingPrerequisite "fsm routines (the repeatable migrations)" ]

    /// <summary>
    /// The queue name, checked before anything is sent. It reaches the database as an identifier
    /// rather than a parameter, so only names that never need quoting are accepted: the same
    /// allowlist <c>fsm.assert_queue_name</c> applies on the other side.
    /// </summary>
    let queueName (queue: string) : BootDefect option =
        if
            not (isNull queue)
            && Text.RegularExpressions.Regex.IsMatch(queue, "^[a-z_][a-z0-9_]*$")
        then
            None
        else
            Some(BootDefect.Misconfigured $"the action queue name {queue} is not in [a-z_][a-z0-9_]*")

    /// <summary>
    /// How long to keep something, as PostgreSQL reads an interval. Text rather than a
    /// <see cref="T:System.TimeSpan" />, because keeping forever is <c>'infinity'</c> and a
    /// <c>TimeSpan</c> cannot say it.
    /// </summary>
    let interval (retain: Retain) : string =
        match retain with
        | Retain.Forever -> "infinity"
        | Retain.For span when span > TimeSpan.Zero ->
            String.Format(CultureInfo.InvariantCulture, "{0} microseconds", span.Ticks / 10L)
        | Retain.For span -> invalidArg (nameof retain) $"A retention period must be positive, not {span}."

    /// <summary>Reads the catalog check. Never fails for a missing object; see boot_check.sql.</summary>
    let check (conn: NpgsqlConnection) (ct: CancellationToken) : Task<Result<CatalogCheck, StoreError>> =
        backgroundTask {
            use cmd = new NpgsqlCommand(SqlResources.get "system" "boot_check", conn)
            use! reader = cmd.ExecuteReaderAsync ct

            match! reader.ReadAsync ct with
            | false -> return Error(Db.decodeFailure (nameof CatalogCheck) "boot_check returned no row")
            | true ->
                return
                    Ok
                        { HasSchema = Row.bool reader "has_schema"
                          HasJournal = Row.bool reader "has_journal"
                          HasRoutines = Row.bool reader "has_routines"
                          HasBtreeGist = Row.bool reader "has_btree_gist"
                          HasPgmq = Row.bool reader "has_pgmq" }
        }

    /// <summary>The applied migrations, read only once the check has said the journal exists.</summary>
    let journal (conn: NpgsqlConnection) (check: CatalogCheck) (ct: CancellationToken) : Task<string list option> =
        backgroundTask {
            if not check.HasJournal then
                return None
            else
                use cmd = new NpgsqlCommand(SqlResources.get "system" "journal", conn)
                use! reader = cmd.ExecuteReaderAsync ct
                let! applied = Row.all (fun row -> Row.string row "scriptname") reader ct
                return Some applied
        }

    /// <summary>Everything the database is missing, empty when it can serve.</summary>
    let inspect (context: PostgresContext) (ct: CancellationToken) : Task<Result<BootDefect list, StoreError>> =
        Db.protect
            context.Resilience
            (fun cancel ->
                backgroundTaskResult {
                    use! conn = context.DataSource.OpenConnectionAsync(cancel).AsTask()
                    let! check = check conn cancel
                    let! applied = journal conn check cancel
                    return defects check applied expectedMigrations
                })
            ct

    /// <summary>Writes this machine's maintenance registration. An upsert, so safe on every boot.</summary>
    let register
        (context: PostgresContext)
        (machineId: MachineId)
        (queue: string)
        (retention: RetentionPolicy)
        (reapAfter: TimeSpan)
        (ct: CancellationToken)
        : Task<Result<unit, StoreError>> =
        if reapAfter <= TimeSpan.Zero then
            invalidArg (nameof reapAfter) "The reaping grace period must be a positive duration."

        let bind =
            Db.parameters
                [ "machine_id", box (MachineId.value machineId)
                  "action_queue", box queue
                  "keep_commands", box (interval retention.Commands)
                  "keep_belief_history", box (interval retention.BeliefHistory)
                  "keep_action_archive", box (interval retention.ActionArchive)
                  "reap_after", box reapAfter ]

        Db.protect
            context.Resilience
            (fun cancel ->
                backgroundTask {
                    use! conn = context.DataSource.OpenConnectionAsync(cancel).AsTask()
                    use cmd = new NpgsqlCommand(SqlResources.get "system" "register_maintenance", conn)
                    bind cmd
                    let! _ = cmd.ExecuteScalarAsync cancel
                    return Ok()
                })
            ct

/// <summary>
/// Maintenance of one PostgreSQL database, for every machine registered in it.
///
/// Every member is one statement naming one <c>fsm.*</c> routine, and pg_cron calls the same
/// routines when it runs the ticks itself. That is what makes the two schedulers
/// interchangeable: neither does anything the other cannot.
/// </summary>
type PostgresMaintenance(context: PostgresContext) =

    let protect work ct = Db.protect context.Resilience work ct

    /// One statement, its parameters, and a reader over every row it returns.
    let query (domain: string) (operation: string) (bind: NpgsqlCommand -> unit) (read: NpgsqlDataReader -> 'T) ct =
        protect
            (fun cancel ->
                backgroundTask {
                    use! conn = context.DataSource.OpenConnectionAsync(cancel).AsTask()
                    use cmd = new NpgsqlCommand(SqlResources.get domain operation, conn)
                    bind cmd
                    use! reader = cmd.ExecuteReaderAsync cancel
                    let! rows = Row.all read reader cancel
                    return Ok rows
                })
            ct

    /// The same, for a statement that answers exactly one row.
    let single domain operation bind read ct =
        backgroundTask {
            match! query domain operation bind read ct with
            | Ok [ row ] -> return Ok row
            | Ok rows ->
                return
                    Error(
                        Db.decodeFailure
                            operation
                            $"{domain}/{operation} returned {List.length rows} rows rather than one"
                    )
            | Error error -> return Error error
        }

    let noParameters = Db.parameters []

    let schedulingOf (value: string) : Result<Scheduling, StoreError> =
        match value with
        | "scheduled" -> Ok Scheduling.Scheduled
        | "unavailable" -> Ok Scheduling.Unavailable
        | "not_permitted" -> Ok Scheduling.NotPermitted
        | other -> Error(Db.decodeFailure (nameof Scheduling) $"unknown scheduling outcome {other}")

    let positiveBatch (batch: int) =
        if batch < 1 then
            invalidArg (nameof batch) "A maintenance batch size must be positive."

    interface IDatabaseMaintenance with

        member _.NotifyPending(ct) =
            single
                "system"
                "notify_pending"
                noParameters
                (fun row ->
                    { Sent = Row.int32 row "sent"
                      QueueUsage = row.GetDouble(row.GetOrdinal "queue_usage") })
                ct

        member _.Run(batch, ct) =
            positiveBatch batch

            query
                "system"
                "run_maintenance"
                (Db.parameters [ "batch", box batch ])
                (fun row ->
                    { MachineId = MachineId.create (Row.string row "machine_id")
                      Reaped = Row.int64 row "reaped"
                      PurgedCommands = Row.int64 row "purged_commands"
                      PurgedBeliefs = Row.int64 row "purged_beliefs"
                      PurgedActions = Row.int64 row "purged_actions"
                      Drifted = Row.int64 row "drifted" })
                ct

        member _.DetectDrift(ct) =
            query
                "command"
                "blocked_drift"
                noParameters
                (fun row ->
                    { BlockedDrift.CommandId = CommandId.ofInt64 (Row.int64 row "command_id")
                      MachineId = MachineId.create (Row.string row "machine_id")
                      EntityId = Row.string row "entity_id"
                      Sequence = Row.int64 row "seq"
                      Blocked = Row.bool row "blocked"
                      Expected = Row.bool row "expected_blocked" })
                ct

        member _.RepairDrift(ct) =
            query
                "command"
                "repair_blocked"
                noParameters
                (fun row ->
                    { RepairedBlock.CommandId = CommandId.ofInt64 (Row.int64 row "command_id")
                      Blocked = Row.bool row "blocked" })
                ct

        member _.Schedule(notifyEvery, runEvery, batch, ct) =
            positiveBatch batch

            single
                "system"
                "schedule"
                (Db.parameters
                    [ "notify_every", box notifyEvery
                      "run_every", box runEvery
                      "batch", box batch ])
                (fun row -> Row.string row "scheduling")
                ct
            |> Task.map (Result.bind schedulingOf)

        member _.Unschedule(ct) =
            single "system" "unschedule" noParameters (fun row -> Row.int32 row "removed") ct

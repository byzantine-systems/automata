namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Globalization
open System.Reflection
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
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

    let private bootCheck =
        StatementKey.create "system" "boot_check" |> Sql.prepareProgram

    let private journalOf = StatementKey.create "system" "journal" |> Sql.prepareProgram

    let private registerMaintenance =
        StatementKey.create "system" "register_maintenance" |> Sql.prepareProgram

    /// <summary>Reads the catalog check. Never fails for a missing object; see boot_check.sql.</summary>
    let check: StoreProgram<ReadAccess, (string * Param) list, NpgsqlDataReader, CatalogCheck> =
        StoreProgram.queryOne bootCheck [] (fun reader ->
            Ok
                { HasSchema = Row.bool reader "has_schema"
                  HasJournal = Row.bool reader "has_journal"
                  HasRoutines = Row.bool reader "has_routines"
                  HasBtreeGist = Row.bool reader "has_btree_gist"
                  HasPgmq = Row.bool reader "has_pgmq" })

    /// <summary>The applied migrations, read only once the check has said the journal exists.</summary>
    let journal
        (check: CatalogCheck)
        : StoreProgram<ReadAccess, (string * Param) list, NpgsqlDataReader, string list option> =
        if check.HasJournal then
            StoreProgram.queryRows journalOf [] (fun row -> Row.string row "scriptname" |> Ok)
            |> StoreProgram.map Some
        else
            StoreProgram.ok None

    /// <summary>Everything the database is missing, empty when it can serve.</summary>
    let inspect (context: PostgresContext) (ct: CancellationToken) : Task<Result<BootDefect list, StoreError>> =
        Sql.readProgram
            context
            (store {
                let! check = check
                let! applied = journal check
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

        Sql.runProgram
            context
            (StoreProgram.executeDiscard
                registerMaintenance
                [ "machine_id", Param.Text(MachineId.value machineId)
                  "action_queue", Param.Text queue
                  "keep_commands", Param.Text(interval retention.Commands)
                  "keep_belief_history", Param.Text(interval retention.BeliefHistory)
                  "keep_action_archive", Param.Text(interval retention.ActionArchive)
                  "reap_after", Param.Interval reapAfter ])
            ct

/// <summary>
/// Maintenance of one PostgreSQL database, for every machine registered in it.
///
/// Every member is one statement naming one <c>fsm.*</c> routine, and pg_cron calls the same
/// routines when it runs the ticks itself. That is what makes the two schedulers
/// interchangeable: neither does anything the other cannot.
/// </summary>
type PostgresMaintenance(context: PostgresContext) =

    let statement domain operation =
        StatementKey.create domain operation |> Sql.prepareProgram

    let notifyPending = statement "system" "notify_pending"
    let runMaintenance = statement "system" "run_maintenance"
    let blockedDrift = statement "command" "blocked_drift"
    let repairBlocked = statement "command" "repair_blocked"
    let scheduleMaintenance = statement "system" "schedule"
    let unscheduleMaintenance = statement "system" "unschedule"
    let beliefColdStale = statement "system" "belief_cold_stale"
    let refreshBeliefCold = statement "system" "refresh_belief_cold"
    let markBeliefCold = statement "system" "mark_belief_cold"

    let schedulingOf (value: string) : Result<Scheduling, StoreError> =
        match value with
        | "scheduled" -> Ok Scheduling.Scheduled
        | "unavailable" -> Ok Scheduling.Unavailable
        | "not_permitted" -> Ok Scheduling.NotPermitted
        | other -> Error(Db.decodeFailure (nameof Scheduling) $"unknown scheduling outcome {other}")

    /// <summary>
    /// Refreshes the materialized cold layer of <c>fsm.belief</c> when a day has turned since the
    /// last refresh, and does nothing otherwise, so a pass every minute costs one cheap check.
    ///
    /// Three statements on one connection rather than one routine, because
    /// <c>REFRESH ... CONCURRENTLY</c> cannot run inside a function or a transaction. Safe to
    /// repeat and safe to race: a second host's refresh waits for the first and finds nothing new.
    /// </summary>
    let refreshColdBeliefs ct : Task<Result<unit, StoreError>> =
        Sql.runProgram
            context
            (store {
                let! stale = StoreProgram.queryOne beliefColdStale [] (fun reader -> Row.bool reader "stale" |> Ok)

                if stale then
                    do! StoreProgram.executeDiscard refreshBeliefCold []
                    do! StoreProgram.executeDiscard markBeliefCold []
            })
            ct

    let driftRow (reader: NpgsqlDataReader) : Result<BlockedDrift, StoreError> =
        let required (read: int -> 'T) (column: string) =
            let ordinal = reader.GetOrdinal column

            if reader.IsDBNull ordinal then
                Error(Db.decodeFailure (nameof BlockedDrift) $"the view returned no {column}")
            else
                Ok(read ordinal)

        result {
            let! commandId = required reader.GetInt64 "command_id"
            let! machineId = required reader.GetString "machine_id"
            let! entityId = required reader.GetString "entity_id"
            let! sequence = required reader.GetInt64 "seq"
            let! blocked = required reader.GetBoolean "blocked"
            let! expected = required reader.GetBoolean "expected_blocked"

            return
                { BlockedDrift.CommandId = CommandId.ofInt64 commandId
                  MachineId = MachineId.create machineId
                  EntityId = entityId
                  Sequence = sequence
                  Blocked = blocked
                  Expected = expected }
        }

    let positiveBatch (batch: int) =
        if batch < 1 then
            invalidArg (nameof batch) "A maintenance batch size must be positive."

    interface IDatabaseMaintenance with

        member _.NotifyPending(ct) =
            Sql.runProgram
                context
                (StoreProgram.queryOne notifyPending [] (fun row ->
                    Ok
                        { Sent = Row.int32 row "sent"
                          QueueUsage = row.GetDouble(row.GetOrdinal "queue_usage") }))
                ct

        member _.Run(batch, ct) =
            positiveBatch batch

            backgroundTaskResult {
                let! reports =
                    Sql.runProgram
                        context
                        (StoreProgram.queryRows runMaintenance [ "batch", Param.Int batch ] (fun row ->
                            Ok
                                { MachineId = MachineId.create (Row.string row "machine_id")
                                  Reaped = Row.int64 row "reaped"
                                  PurgedCommands = Row.int64 row "purged_commands"
                                  PurgedBeliefs = Row.int64 row "purged_beliefs"
                                  PurgedActions = Row.int64 row "purged_actions"
                                  Drifted = Row.int64 row "drifted" }))
                        ct

                do! refreshColdBeliefs ct
                return reports
            }

        member _.DetectDrift(ct) =
            Sql.readProgram context (StoreProgram.queryRows blockedDrift [] driftRow) ct

        member _.RepairDrift(ct) =
            Sql.runProgram
                context
                (StoreProgram.queryRows repairBlocked [] (fun row ->
                    Ok
                        { RepairedBlock.CommandId = CommandId.ofInt64 (Row.int64 row "command_id")
                          Blocked = Row.bool row "blocked" }))
                ct

        member _.Schedule(notifyEvery, runEvery, batch, ct) =
            positiveBatch batch

            Sql.runProgram
                context
                (StoreProgram.queryOne
                    scheduleMaintenance
                    [ "notify_every", Param.Interval notifyEvery
                      "run_every", Param.Interval runEvery
                      "batch", Param.Int batch ]
                    (fun row -> Row.string row "scheduling" |> schedulingOf))
                ct

        member _.Unschedule(ct) =
            Sql.runProgram
                context
                (StoreProgram.queryOne unscheduleMaintenance [] (fun row -> Row.int32 row "removed" |> Ok))
                ct

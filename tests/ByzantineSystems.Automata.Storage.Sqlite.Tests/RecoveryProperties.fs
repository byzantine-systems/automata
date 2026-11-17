module ByzantineSystems.Automata.Storage.Sqlite.Tests.RecoveryProperties

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto
open FsCheck
open Microsoft.Data.Sqlite

// ---------------------------------------------------------------------------------------------
// Poison recovery, as properties rather than paths. The batch law holds for any mix of healthy
// and undecodable commands: the claim returns the healthy rows and leaves every quarantined row
// with exactly one complete machine error and nothing else. The fence law holds for any way a
// claim can stop being current — a higher token, a reschedule, another worker's finish, a purge
// — recovery writes nothing at all, while a fence that still holds writes the complete outcome.
// Generated fault locations check that every partial recovery rolls back with its transaction.
// ---------------------------------------------------------------------------------------------

let private entity name : Entity = entityId name

let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

let private query
    (db: string)
    (sql: string)
    (parameters: (string * obj) list)
    (read: SqliteDataReader -> 'a)
    : 'a list =
    use conn = new SqliteConnection(db)
    conn.Open()
    use cmd = conn.CreateCommand()
    cmd.CommandText <- sql

    for name, value in parameters do
        cmd.Parameters.AddWithValue(name, value) |> ignore

    use reader = cmd.ExecuteReader()

    [ while reader.Read() do
          read reader ]

/// Every machine error as (command id, reason), joined to its header so an orphan child cannot
/// pass as evidence.
let private machineErrors (db: string) : (int64 * string) list =
    query db "SELECT m.command_id, m.reason
         FROM fsm_command_error_machine m
         JOIN fsm_command_error h ON h.command_id = m.command_id AND h.kind = m.kind;" [] (fun reader ->
        reader.GetInt64 0, reader.GetString 1)

/// Preserve complete ordered rows, including BLOB identity and failure payloads. Counts alone
/// would miss a stale recovery overwriting an existing fact without changing its cardinality.
let private stateOf db =
    [ "fsm_command", "command_id"
      "fsm_command_error", "command_id"
      "fsm_command_error_domain", "command_id"
      "fsm_command_error_machine", "command_id"
      "fsm_command_error_replay", "command_id"
      "fsm_transition", "command_id"
      "fsm_entity_snapshot", "machine_id, entity_id"
      "fsm_action", "command_id, ordinal" ]
    |> List.map (fun (table, order) ->
        table,
        query db $"SELECT * FROM {table} ORDER BY {order}" [] (fun reader ->
            [ for index in 0 .. reader.FieldCount - 1 do
                  match reader.GetValue index with
                  | :? (byte array) as bytes -> Convert.ToHexString bytes
                  | value -> Convert.ToString(value, Globalization.CultureInfo.InvariantCulture) ]))

/// The raw claimed row, as recovery itself reads it.
let private commandRow (db: string) (key: string) =
    query db "SELECT * FROM fsm_command WHERE idempotency_key = @key;" [ "@key", box key ] CommandMapping.commandRow
    |> List.exactlyOne

/// The submission trigger, restored exactly as the migration declares it, so a payload the test
/// corrupted is the only thing standing between the schema and its own rules.
let private restoreSubmissionTrigger (db: string) =
    exec
        db
        "CREATE TRIGGER fsm_command_submission_is_immutable
         BEFORE UPDATE OF command_id, machine_id, entity_id, seq, idempotency_key, chart_version, event, kind, received_at, tenant, principal, source, correlation_id, causation_id ON fsm_command
         BEGIN
             SELECT
                 RAISE(ABORT, 'fsm_command: a submitted command is never rewritten');
         END;"
        []

let private corruptPayload (db: string) (key: string) (garbage: string) =
    exec db "DROP TRIGGER fsm_command_submission_is_immutable;" []

    exec db "UPDATE fsm_command SET event = @g WHERE idempotency_key = @k;" [ "@g", box garbage; "@k", box key ]

    restoreSubmissionTrigger db

let private statusOfKey (db: string) (key: string) : string =
    query db "SELECT status FROM fsm_command WHERE idempotency_key = @k;" [ "@k", box key ] (fun r -> r.GetString 0)
    |> List.exactlyOne

let private blockedOfKey (db: string) (key: string) : int64 =
    query db "SELECT blocked FROM fsm_command WHERE idempotency_key = @k;" [ "@k", box key ] (fun r -> r.GetInt64 0)
    |> List.exactlyOne

let private backoff =
    Backoff.create (TimeSpan.FromMilliseconds 10.0) (TimeSpan.FromMilliseconds 100.0)

let private config cases =
    Config.QuickThrowOnFailure.WithMaxTest(cases).WithEndSize(30)

// ---------------------------------------------------------------------------------------------
// Property 1 — the batch law, through the public claim.
// ---------------------------------------------------------------------------------------------

let private batchCase (healthySeed: byte, poisonSeed: byte, garbageSeed: byte) : bool =
    let healthyCount = int healthySeed % 4
    let poisonCount = 1 + int poisonSeed % 3
    // Every payload is valid JSON the schema accepts, so decoding the event is the only thing
    // that fails — exactly the poison the claim path promises to quarantine.
    let garbage = [ "{}"; "[1,2]"; "{\"Case\":\"NeverDeclared\"}" ][int garbageSeed % 3]

    let db = fixture "recovery-batch"
    let inbox = inboxOf db

    let submitted index =
        (submit inbox (entity $"e{index}") $"k{index}" (Start index)).GetAwaiter().GetResult()
        |> CommandId.value

    let healthy = List.init healthyCount id |> List.map (fun index -> submitted index)

    let poison = List.init poisonCount (fun index -> submitted (100 + index))

    // Only the payload is broken; every other column stays exactly as the store wrote it.
    for index in 0 .. poisonCount - 1 do
        corruptPayload db.Db $"k{100 + index}" garbage

    let claimed =
        inbox
            .ClaimAsync(
                testMachine,
                ClaimBatch.create (healthyCount + poisonCount),
                LeaseDuration.create (TimeSpan.FromSeconds 30.0),
                noCancellation
            )
            .GetAwaiter()
            .GetResult()
        |> expectOk "claim"
        |> List.map (fun held -> CommandId.value held.Work.CommandId)

    let returnedHealthy = List.sort claimed = List.sort healthy

    let quarantined =
        [ 0 .. poisonCount - 1 ]
        |> List.forall (fun index -> statusOfKey db.Db $"k{100 + index}" = "dead_letter")

    let evidence =
        let rows = machineErrors db.Db |> List.sortBy fst

        List.map fst rows = List.sort poison
        && scalar db.Db "SELECT count(*) FROM fsm_command_error" = string poisonCount
        && rows |> List.forall (fun (_, reason) -> reason.StartsWith "undecodable:")

    let healthyStillLeased =
        [ 0 .. healthyCount - 1 ]
        |> List.forall (fun index -> statusOfKey db.Db $"k{index}" = "leased")

    let nothingElse =
        query db.Db "SELECT count(*) FROM fsm_command_error_domain" [] (fun r -> r.GetInt64 0)
        |> List.exactlyOne = 0L
        && query db.Db "SELECT count(*) FROM fsm_command_error_replay" [] (fun r -> r.GetInt64 0)
           |> List.exactlyOne = 0L
        && query db.Db "SELECT count(*) FROM fsm_transition" [] (fun r -> r.GetInt64 0)
           |> List.exactlyOne = 0L

    returnedHealthy && quarantined && evidence && healthyStillLeased && nothingElse

// ---------------------------------------------------------------------------------------------
// Property 2 — the fence law, against the recovery module itself.
//
// A healthy row is claimed first, so the claim's own recovery never runs; its payload is
// corrupted while it is leased, which is the state a crash between claim and decode would
// leave. Each scenario then ages the claim in one generated way — or leaves it current — and
// the recovery module runs against the row exactly as it was leased.
// ---------------------------------------------------------------------------------------------

let private fenceCase (scenarioSeed: byte, reason: string, withSuccessor: bool) : bool =
    let scenario = int scenarioSeed % 5
    let db = fixture "recovery-fence"
    let inbox = inboxOf db

    let poisonTask =
        task {
            let! poisonId = submit inbox (entity "poison") "poison" (Start 1)

            if withSuccessor then
                let! _ = submit inbox (entity "poison") "successor" Finish
                ()

            return poisonId
        }

    poisonTask.GetAwaiter().GetResult() |> ignore

    // Claim the healthy row, then corrupt the payload it was claimed under.
    let held =
        inbox
            .ClaimAsync(
                testMachine,
                ClaimBatch.create 1,
                LeaseDuration.create (TimeSpan.FromSeconds 30.0),
                noCancellation
            )
            .GetAwaiter()
            .GetResult()
        |> expectOk "claim"
        |> List.exactlyOne

    corruptPayload db.Db "poison" "{\"Case\":\"NeverDeclared\"}"

    let row = commandRow db.Db "poison"

    match scenario with
    // 0: another worker re-claimed, so the file holds a strictly higher token.
    | 0 ->
        exec
            db.Db
            "UPDATE fsm_command SET lease_token = lease_token + 1 WHERE command_id = @id;"
            [ "@id", box row.command_id ]
    // 1: the claim was rescheduled; the row is ready with the zero token.
    | 1 ->
        inbox.RescheduleAsync(held.Work.CommandId, held.Token, backoff, noCancellation).GetAwaiter().GetResult()
        |> expectOk "reschedule"
        |> ignore
    // 2: another worker finished the command under its lease.
    | 2 ->
        (processorOf db)
            .CommitAsync(
                held.Work.CommandId,
                held.Token,
                Epoch.initial,
                draft held.Work.EntityId Idle (Active 3) effectiveAt,
                noCancellation
            )
            .GetAwaiter()
            .GetResult()
        |> expectOk "finish by another worker"
        |> ignore
    // 3: retention deleted the row entirely.
    | 3 -> exec db.Db "DELETE FROM fsm_command WHERE command_id = @id;" [ "@id", box row.command_id ]
    // 4: the fence holds; recovery must write the complete outcome.
    | _ -> ()

    let before = stateOf db.Db

    let outcome =
        (CommandRecovery.recover db.Context row reason noCancellation).GetAwaiter().GetResult()

    if scenario = 4 then
        // The fresh control: a recovery that always refused would pass every stale branch, so
        // this branch must prove the writes happen and are complete.
        outcome |> expectOk "fresh recovery" |> ignore

        stateOf db.Db <> before
        && statusOfKey db.Db "poison" = "dead_letter"
        && machineErrors db.Db = [ row.command_id, reason ]
        && (not withSuccessor || blockedOfKey db.Db "successor" = 0L)
    else
        // Stale: nothing observable changes — the projection covers every command row including
        // any successor's blocked flag — and the caller is told the no-op succeeded rather than
        // receiving a failure it might retry forever.
        outcome = Ok() && stateOf db.Db = before

// ---------------------------------------------------------------------------------------------
// Fault injection is generic too: vary the reason, then refuse each stage in turn and compare
// complete before/after rows. Every stage must fail without leaking preceding writes.
// ---------------------------------------------------------------------------------------------

let private rollbackCase reason stage =
    let db = fixture "recovery-rollback"
    let inbox = inboxOf db
    (leased inbox (entity "e1") "poison").GetAwaiter().GetResult() |> ignore

    (submit inbox (entity "e1") "successor" Finish).GetAwaiter().GetResult()
    |> ignore

    let row = commandRow db.Db "poison"

    let clause =
        match stage with
        | 0 -> "BEFORE INSERT ON fsm_command_error"
        | 1 -> "BEFORE INSERT ON fsm_command_error_machine"
        | 2 -> "BEFORE UPDATE ON fsm_command WHEN NEW.status = 'dead_letter'"
        | _ -> "BEFORE UPDATE OF blocked ON fsm_command WHEN NEW.idempotency_key = 'successor'"

    exec
        db.Db
        $"CREATE TRIGGER test_refuse_recovery {clause} BEGIN SELECT RAISE(ABORT, 'injected recovery failure'); END"
        []

    let before = stateOf db.Db

    let result =
        (CommandRecovery.recover db.Context row reason noCancellation).GetAwaiter().GetResult()

    match result with
    | Error(StoreError.Unexpected(:? SqliteException)) -> stateOf db.Db = before
    | _ -> false

let tests =
    testList
        "poison recovery"
        [ testCase
              "the batch law: healthy rows return, poisoned rows carry exactly one complete machine error"
              (fun () ->
                  Check.One(
                      config 25,
                      fun (healthySeed: byte, poisonSeed: byte, garbageSeed: byte) ->
                          batchCase (healthySeed, poisonSeed, garbageSeed)
                  ))

          testCase "the fence law: stale claims write nothing, current claims write everything" (fun () ->
              Check.One(
                  config 12,
                  fun (reason: string, withSuccessor: bool) ->
                      [ 0uy .. 4uy ]
                      |> List.forall (fun scenario -> fenceCase (scenario, reason, withSuccessor))
              ))

          testCase "every refused recovery stage rolls back all preceding writes" (fun () ->
              Check.One(config 12, fun (reason: string) -> [ 0..3 ] |> List.forall (rollbackCase reason))) ]

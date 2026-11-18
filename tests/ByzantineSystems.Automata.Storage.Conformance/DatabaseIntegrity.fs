module ByzantineSystems.Automata.Storage.DatabaseIntegrity

open System
open Expecto
open FsCheck
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.LifecycleModel

/// Only SQL spelling and connection/transaction mechanics vary between the real providers.
/// Rejected statements must be rolled back to a savepoint before the next observation.
type Database =
    { Table: string -> string
      Execute: string -> unit
      Refuses: string -> bool
      Scalar: string -> string
      Snapshot: unit -> string list list list
      Columns: string -> string list
      Insert: CommandStatus -> string -> int64
      Now: string
      Dispose: unit -> unit }

let private quoted (text: string) = "'" + text.Replace("'", "''") + "'"

let private command = "command"
let private error = "command_error"
let private domain = "command_error_domain"
let private machine = "command_error_machine"
let private replay = "command_error_replay"

let private assign db id status =
    let unblocked = if db.Now = "0" then "0" else "false"
    $"UPDATE {db.Table command} SET status = {quoted (Lifecycle.sql status)}, lease_token = {if status = CommandStatus.Ready then 0 else 7}, blocked = {unblocked} WHERE command_id = {id}"

let private failureKind variant =
    match variant with
    | 0 -> "domain"
    | 1 -> "machine"
    | _ -> "replay"

let private header db id variant =
    db.Execute
        $"INSERT INTO {db.Table error}(command_id, kind, recorded_at) VALUES ({id}, {quoted (failureKind variant)}, {db.Now})"

let private child db id variant seed =
    let reason = quoted $"failure-{seed}-'λ"

    let sql =
        match variant with
        | 0 -> $"INSERT INTO {db.Table domain}(command_id, error) VALUES ({id}, '{{\"payload\":{seed}}}')"
        | 1 -> $"INSERT INTO {db.Table machine}(command_id, reason) VALUES ({id}, {reason})"
        | 2 ->
            $"INSERT INTO {db.Table replay}(command_id, replay_kind, version) VALUES ({id}, 'unknown_chart_version', 2)"
        | 3 ->
            $"INSERT INTO {db.Table replay}(command_id, replay_kind, epoch, reason) VALUES ({id}, 'diverged', 3, {reason})"
        | 4 -> $"INSERT INTO {db.Table replay}(command_id, replay_kind, budget) VALUES ({id}, 'budget_exceeded', 4)"
        | _ -> $"INSERT INTO {db.Table replay}(command_id, replay_kind) VALUES ({id}, 'history_purged')"

    db.Execute sql

let private withDatabase fresh action =
    let db = fresh ()

    try
        action db
    finally
        db.Dispose()

let private property name law =
    testCase name (fun () -> Check.One(Config.QuickThrowOnFailure.WithMaxTest(12), law))

/// Fail at the first unexpected acceptance rather than interpreting a later
/// constraint error in already-corrupted state as the original counterexample.
let private requiredRefusal db statement reason =
    let before = db.Snapshot()
    Expect.isTrue (db.Refuses statement) reason
    Expect.equal (db.Snapshot()) before "refusal preserves the complete observation"
    true

let lifecycleTests fresh =
    testList
        "database lifecycle laws"
        [ property "all status assignments agree with the independent lifecycle model" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  [ for source in Lifecycle.all do
                        for destination in Lifecycle.all do
                            let id = db.Insert CommandStatus.Ready "event"
                            let variant = int (uint32 seed % 6u)

                            if Lifecycle.closureEvidence source = ClosureEvidence.CompleteFailure then
                                header db id variant
                                child db id variant seed

                            if source <> CommandStatus.Ready then
                                db.Execute(assign db id source)

                            if
                                not (Lifecycle.isTerminal source)
                                && Lifecycle.closureEvidence destination = ClosureEvidence.CompleteFailure
                            then
                                header db id variant
                                child db id variant seed

                            let before = db.Snapshot()
                            let refused = db.Refuses(assign db id destination)

                            yield
                                match Lifecycle.classify source destination with
                                | LifecycleVerdict.Refused -> refused && db.Snapshot() = before
                                | LifecycleVerdict.LegalChange
                                | LifecycleVerdict.LegalUnchanged ->
                                    not refused
                                    && db.Scalar $"SELECT status FROM {db.Table command} WHERE command_id = {id}" = Lifecycle.sql
                                        destination ]
                  |> List.forall id))

          property
              "only ready commands can be inserted and terminal rows reject every operational update"
              (fun (seed: int) ->
                  withDatabase fresh (fun db ->
                      [ for status in Lifecycle.all do
                            let before = db.Snapshot()
                            let mutable rejected = false

                            try
                                db.Insert status "event" |> ignore
                            with _ ->
                                rejected <- true

                            yield
                                if status = CommandStatus.Ready then
                                    not rejected
                                else
                                    rejected && db.Snapshot() = before
                        for terminal in Lifecycle.all |> List.filter Lifecycle.isTerminal do
                            let id = db.Insert CommandStatus.Ready "event"

                            if terminal <> CommandStatus.Succeeded then
                                header db id 1
                                child db id 1 seed

                            db.Execute(assign db id terminal)
                            let before = db.Snapshot()

                            yield
                                db.Refuses
                                    $"UPDATE {db.Table command} SET read_ct = read_ct + 1 WHERE command_id = {id}"
                                && db.Snapshot() = before ]
                      |> List.forall id))

          property
              "every failed closure needs its selected child and success cannot carry a failure"
              (fun (seed: int) ->
                  withDatabase fresh (fun db ->
                      [ for terminal in [ CommandStatus.Rejected; CommandStatus.DeadLettered ] do
                            for variant in 0..5 do
                                let id = db.Insert CommandStatus.Ready "event"

                                yield
                                    requiredRefusal
                                        db
                                        (assign db id terminal)
                                        "closing without a header must be refused"

                                header db id variant

                                yield
                                    requiredRefusal
                                        db
                                        (assign db id terminal)
                                        "closing with only a header must be refused"

                                child db id variant seed

                                yield
                                    requiredRefusal
                                        db
                                        (assign db id CommandStatus.Succeeded)
                                        "success must refuse existing failure evidence"

                                db.Execute(assign db id terminal)

                                yield
                                    db.Scalar $"SELECT status FROM {db.Table command} WHERE command_id = {id}" = Lifecycle.sql
                                        terminal
                        let success = db.Insert CommandStatus.Ready "event"
                        db.Execute(assign db success CommandStatus.Succeeded)

                        yield
                            db.Refuses
                                $"INSERT INTO {db.Table error}(command_id, kind, recorded_at) VALUES ({success}, 'machine', {db.Now})" ]
                      |> List.forall id))

          property "failure deletion is owned by the command, for live and closed owners" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  [ for variant in 0..5 do
                        for close in [ false; true ] do
                            let id = db.Insert CommandStatus.Ready "event"
                            header db id variant
                            child db id variant seed

                            if close then
                                db.Execute(assign db id CommandStatus.DeadLettered)

                            let before = db.Snapshot()

                            for table in [ "command_error"; "command_error_" + failureKind variant ] do
                                yield
                                    db.Refuses $"DELETE FROM {db.Table table} WHERE command_id = {id}"
                                    && db.Snapshot() = before

                            db.Execute $"DELETE FROM {db.Table command} WHERE command_id = {id}"

                            for table in [ "command_error"; "command_error_" + failureKind variant ] do
                                yield db.Scalar $"SELECT count(*) FROM {db.Table table} WHERE command_id = {id}" = "0" ]
                  |> List.forall id)) ]

let ownershipRollbackTests fresh =
    property "a restricted root deletion restores its entire cascaded failure graph" (fun (seed: int) ->
        withDatabase fresh (fun db ->
            let id = db.Insert CommandStatus.Ready "event"
            let variant = int (uint32 seed % 6u)
            header db id variant
            child db id variant seed
            db.Execute(assign db id CommandStatus.Rejected)
            let pin = db.Table "integrity_pin"
            db.Execute $"CREATE TABLE {pin}(command_id bigint REFERENCES {db.Table command}(command_id))"
            db.Execute $"INSERT INTO {pin} VALUES ({id})"
            let before = db.Snapshot()

            let restored =
                db.Refuses $"DELETE FROM {db.Table command} WHERE command_id = {id}"
                && db.Snapshot() = before

            db.Execute $"DELETE FROM {pin}"
            db.Execute $"DELETE FROM {db.Table command} WHERE command_id = {id}"

            restored
            && db.Scalar $"SELECT count(*) FROM {db.Table error} WHERE command_id = {id}" = "0"))

let immutableTests fresh =
    testList
        "database immutable fact laws"
        [ property "every submitted field rejects even same-value assignments on a live command" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  let id = db.Insert CommandStatus.Ready "event"

                  if seed % 2 <> 0 then
                      db.Execute(assign db id CommandStatus.Leased)

                  db.Execute
                      $"UPDATE {db.Table command} SET attempts = {int (uint32 seed % 100u)} WHERE command_id = {id}"

                  let before = db.Snapshot()

                  [ "command_id"
                    "machine_id"
                    "entity_id"
                    "seq"
                    "idempotency_key"
                    "chart_version"
                    "event"
                    "request_digest"
                    "kind"
                    "received_at"
                    "tenant"
                    "principal"
                    "source"
                    "correlation_id"
                    "causation_id" ]
                  |> List.forall (fun field ->
                      db.Refuses $"UPDATE {db.Table command} SET {field} = {field} WHERE command_id = {id}"
                      && db.Snapshot() = before)))

          property "every failure fact column rejects updates without changing the stored evidence" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  [ for variant in 0..5 do
                        let id = db.Insert CommandStatus.Ready "event"
                        header db id variant
                        child db id variant seed

                        for table in [ error; "command_error_" + failureKind variant ] do
                            let before = db.Snapshot()

                            for field in db.Columns table do
                                yield
                                    db.Refuses
                                        $"UPDATE {db.Table table} SET {field} = {field} WHERE command_id = {id}"
                                    && db.Snapshot() = before ]
                  |> List.forall id))

          property "correction details are immutable root-owned requests" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  let id = db.Insert CommandStatus.Ready "correction"
                  let correction = db.Table "command_correction"

                  db.Execute
                      $"INSERT INTO {correction}(command_id, effective_at, on_divergence, replay_limit) VALUES ({id}, {db.Now}, 'fail', {1 + int (uint32 seed % 100u)})"

                  let before = db.Snapshot()

                  let protectedFields =
                      db.Columns "command_correction"
                      |> List.forall (fun field ->
                          db.Refuses $"UPDATE {correction} SET {field} = {field} WHERE command_id = {id}"
                          && db.Snapshot() = before)

                  let owned = db.Refuses $"DELETE FROM {correction} WHERE command_id = {id}"
                  db.Execute $"DELETE FROM {db.Table command} WHERE command_id = {id}"

                  protectedFields
                  && owned
                  && db.Scalar $"SELECT count(*) FROM {correction} WHERE command_id = {id}" = "0"))

          property "transition and delivery evidence reject updates but permit retention deletion" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  let id = db.Insert CommandStatus.Ready "event"
                  let transition = db.Table "transition"
                  let deadLetter = db.Table "action_dead_letter"
                  let emptyPaths = if db.Now = "0" then "[]" else "{}"

                  db.Execute
                      $"INSERT INTO {transition}(machine_id, entity_id, epoch, command_id, chart_version, actions, from_state, to_state, handled_by, exited, entered, status, effective_at, committed_at)
                               SELECT machine_id, entity_id, 1, command_id, chart_version, '[]', '{{}}', '{{}}', 'root', '{emptyPaths}', '{emptyPaths}', 'running', {db.Now}, {db.Now} FROM {db.Table command} WHERE command_id = {id}"

                  db.Execute
                      $"INSERT INTO {deadLetter}(machine_id, entity_id, command_id, epoch, ordinal, action, reason, deliveries, recorded_at)
                               SELECT machine_id, entity_id, command_id, 1, 0, '{{}}', 'undelivered-{seed}', 1, {db.Now} FROM {db.Table command} WHERE command_id = {id}"

                  let protectedFields =
                      [ transition, "transition"; deadLetter, "action_dead_letter" ]
                      |> List.forall (fun (table, name) ->
                          db.Columns name
                          |> List.forall (fun field ->
                              let before = db.Snapshot()

                              db.Refuses $"UPDATE {table} SET {field} = {field} WHERE command_id = {id}"
                              && db.Snapshot() = before))

                  db.Execute $"DELETE FROM {transition} WHERE command_id = {id}"
                  db.Execute $"DELETE FROM {deadLetter} WHERE command_id = {id}"

                  protectedFields
                  && db.Scalar $"SELECT count(*) FROM {transition} WHERE command_id = {id}" = "0"
                  && db.Scalar $"SELECT count(*) FROM {deadLetter} WHERE command_id = {id}" = "0")) ]

/// SQLite's conflict algorithms can delete a victim without visiting its UPDATE
/// guard. Exercise each identity independently, not just identical primary keys.
let replacementTests fresh =
    let copy db table id omit overrides =
        let columns =
            db.Columns table |> List.filter (fun field -> not (List.contains field omit))

        let projection =
            columns
            |> List.map (fun field -> Map.tryFind field overrides |> Option.defaultValue field)
            |> String.concat ", "

        let names = String.concat ", " columns
        $"INTO {db.Table table} ({names}) SELECT {projection} FROM {db.Table table} WHERE command_id = {id}"

    testList
        "SQLite conflict algorithm laws"
        [ property "all conflict algorithms preserve existing failure and correction identities" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  [ for variant in 0..5 do
                        let id = db.Insert CommandStatus.Ready "event"
                        header db id variant
                        child db id variant seed

                        for table in [ error; "command_error_" + failureKind variant ] do
                            let insertion = copy db table id [] Map.empty
                            let before = db.Snapshot()

                            for prefix, suffix in
                                [ "INSERT OR REPLACE", ""
                                  "REPLACE", ""
                                  "INSERT OR IGNORE", ""
                                  "INSERT", " ON CONFLICT DO NOTHING"
                                  "INSERT", " ON CONFLICT DO UPDATE SET command_id = excluded.command_id" ] do
                                yield db.Refuses $"{prefix} {insertion}{suffix}" && db.Snapshot() = before
                    let id = db.Insert CommandStatus.Ready "correction"
                    let table = db.Table "command_correction"
                    db.Execute $"INSERT INTO {table} VALUES ({id}, {db.Now}, 'fail', 2)"
                    let before = db.Snapshot()
                    let insertion = copy db "command_correction" id [] Map.empty
                    yield db.Refuses $"INSERT OR REPLACE {insertion}" && db.Snapshot() = before ]
                  |> List.forall id))

          property "replacement cannot steal any command identity or the current head slot" (fun (seed: int) ->
              withDatabase fresh (fun db ->
                  let id = db.Insert CommandStatus.Ready "event"
                  let original = db.Snapshot()

                  let changes =
                      [ [],
                        Map
                            [ "entity_id", "entity_id || '-other'"
                              "idempotency_key", "idempotency_key || '-other'" ]
                        [ "command_id" ], Map [ "idempotency_key", "idempotency_key || '-seq'"; "blocked", "1" ]
                        [ "command_id" ], Map [ "seq", "seq + 1"; "blocked", "1" ]
                        [ "command_id" ], Map [ "seq", "seq + 1"; "idempotency_key", "idempotency_key || '-head'" ]
                        [ "command_id" ],
                        Map
                            [ "seq", "seq + 1"
                              "idempotency_key", "idempotency_key || '-null'"
                              "blocked", "NULL" ] ]

                  let preserved =
                      changes
                      |> List.forall (fun (omit, overrides) ->
                          let insertion = copy db command id omit overrides
                          db.Refuses $"INSERT OR REPLACE {insertion}" && db.Snapshot() = original)

                  let waiting =
                      copy
                          db
                          command
                          id
                          [ "command_id" ]
                          (Map
                              [ "seq", "seq + 1"
                                "idempotency_key", "idempotency_key || '-waiting'"
                                "blocked", "1" ])

                  db.Execute $"INSERT {waiting}"

                  let pending =
                      db.Scalar $"SELECT command_id FROM {db.Table command} WHERE command_id <> {id}"
                      |> int64

                  let before = db.Snapshot()

                  let headProtected =
                      db.Refuses $"UPDATE OR REPLACE {db.Table command} SET blocked = 0 WHERE command_id = {pending}"
                      && db.Snapshot() = before

                  let nullProtected =
                      db.Refuses
                          $"UPDATE OR REPLACE {db.Table command} SET blocked = NULL WHERE command_id = {pending}"
                      && db.Snapshot() = before

                  let aliasesProtected =
                      [ "rowid"; "_rowid_"; "oid" ]
                      |> List.forall (fun alias ->
                          db.Refuses
                              $"UPDATE OR REPLACE {db.Table command} SET {alias} = {id} WHERE command_id = {pending}"
                          && db.Snapshot() = before)

                  db.Execute(assign db id CommandStatus.Succeeded)

                  db.Execute
                      $"UPDATE {db.Table command} SET blocked = 0, read_ct = {int (uint32 seed % 100u)} WHERE command_id = {pending}"

                  preserved
                  && headProtected
                  && nullProtected
                  && aliasesProtected
                  && db.Scalar $"SELECT blocked FROM {db.Table command} WHERE command_id = {pending}" = "0"))

          property
              "composite-key evidence cannot be replaced through either declared keys or hidden rowids"
              (fun (seed: int) ->
                  withDatabase fresh (fun db ->
                      let id = db.Insert CommandStatus.Ready "event"
                      let other = db.Insert CommandStatus.Ready "event"
                      let transition = db.Table "transition"
                      let deadLetter = db.Table "action_dead_letter"

                      db.Execute
                          $"INSERT INTO {transition}(machine_id, entity_id, epoch, command_id, chart_version, actions, from_state, to_state, handled_by, exited, entered, status, effective_at, committed_at)
                               SELECT machine_id, entity_id, 1, command_id, chart_version, '[]', '{{}}', '{{}}', 'root', '[]', '[]', 'running', 0, 0 FROM {db.Table command} WHERE command_id = {id}"

                      db.Execute
                          $"INSERT INTO {deadLetter}(machine_id, entity_id, command_id, epoch, ordinal, action, reason, deliveries, recorded_at)
                               SELECT machine_id, entity_id, command_id, 1, 0, '{{}}', 'undelivered-{seed}', 1, 0 FROM {db.Table command} WHERE command_id = {id}"

                      let before = db.Snapshot()

                      let identitiesProtected =
                          [ "transition", Map.empty
                            "transition", Map [ "command_id", string other ]
                            "transition", Map [ "epoch", "epoch + 1" ]
                            "action_dead_letter", Map.empty ]
                          |> List.forall (fun (table, overrides) ->
                              let insertion = copy db table id [] overrides
                              db.Refuses $"INSERT OR REPLACE {insertion}" && db.Snapshot() = before)

                      let rowidsAbsent =
                          [ "transition"; "action_dead_letter" ]
                          |> List.forall (fun table ->
                              let tableName = db.Table table
                              db.Scalar $"SELECT wr FROM pragma_table_list WHERE name = '{tableName}'" = "1")

                      identitiesProtected && rowidsAbsent)) ]

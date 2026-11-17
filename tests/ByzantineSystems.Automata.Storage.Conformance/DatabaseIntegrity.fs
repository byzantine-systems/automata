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
                                let before = db.Snapshot()
                                yield db.Refuses(assign db id terminal) && db.Snapshot() = before
                                header db id variant
                                let incomplete = db.Snapshot()
                                yield db.Refuses(assign db id terminal) && db.Snapshot() = incomplete
                                child db id variant seed
                                yield db.Refuses(assign db id CommandStatus.Succeeded)
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

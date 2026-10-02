module ByzantineSystems.Automata.Storage.Postgres.Tests.SqlTests

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext
open Expecto

/// A submission under a chart version nobody registered, which the server refuses under the
/// command's foreign key to the version table.
let private unregisteredSubmission =
    let submit = Statement.load "command" "submit"

    Sql.one
        "SubmissionOutcome"
        submit
        [ "machine_id", Param.Text "pg-sql-tests"
          "entity_id", Param.Text "e1"
          "idempotency_key", Param.Text "k1"
          "chart_version", Param.Int 999
          "event", Param.Jsonb "{}"
          "visible_at", Param.TimestampOrNull None
          "received_at", Param.TimestampOrNull None
          "tenant", Param.Text ""
          "principal", Param.Text ""
          "source", Param.Text ""
          "correlation_id", Param.Text ""
          "causation_id", Param.Text "" ]
        (fun reader -> Row.int64 reader "command_id")

let tests =
    testList
        "Sql"
        [ testTask "a named violation is a refusal the caller can match on" {
              do! reset ()
              let! attempt = Sql.attempt (context ()) unregisteredSubmission noCancellation

              match attempt with
              | Db.Refused(Db.ForeignKey "command_chart_version_fkey", _) -> ()
              | other -> failtestf "expected the foreign key refusal, got %A" other
          }

          testTask "a violation nobody asked to handle is Unexpected" {
              do! reset ()
              let! outcome = Sql.run (context ()) unregisteredSubmission noCancellation

              match outcome with
              | Error(StoreError.Unexpected _) -> ()
              | other -> failtestf "expected a defect, got %A" other
          }

          testTask "one refuses a statement that returns no row" {
              do! reset ()

              let byVersion =
                  Sql.one
                      "ChartFingerprint"
                      (Statement.load "chart" "by_version")
                      [ "machine_id", Param.Text "nobody"; "version", Param.Int 1 ]
                      (fun reader -> Row.string reader "fingerprint")

              let! outcome = Sql.run (context ()) byVersion noCancellation

              match outcome with
              | Error(StoreError.Serialization(_, error)) ->
                  Expect.stringContains error.Message "chart/by_version returned no row" "names the statement"
              | other -> failtestf "expected a decode failure, got %A" other
          }

          testTask "one refuses a statement that returns more than one row" {
              let journal =
                  Sql.one "Script" (Statement.load "system" "journal") [] (fun reader -> Row.string reader "scriptname")

              let! outcome = Sql.run (context ()) journal noCancellation

              match outcome with
              | Error(StoreError.Serialization(_, error)) ->
                  Expect.stringContains error.Message "system/journal returned more than one row" "names the statement"
              | other -> failtestf "expected a decode failure, got %A" other
          } ]

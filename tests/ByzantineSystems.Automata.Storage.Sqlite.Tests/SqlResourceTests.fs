module ByzantineSystems.Automata.Storage.Sqlite.Tests.SqlResourceTests

open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto
open Microsoft.Data.Sqlite

/// The loader shares an assembly with the migrations, and the migrations are embedded too. These
/// hold the line between the two trees, and prove every shipped statement compiles against the
/// schema it was written for.
[<Tests>]
let tests =
    testList
        "SqlResources"
        [ testCase "migration scripts are not mistaken for queries"
          <| fun _ ->
              // Migration scripts are numbered and queries are named, so a numbered key is a
              // migration that slipped through the tree-root check.
              let leaked =
                  SqlResources.keys
                  |> List.filter (fun (_, operation) -> operation.Length > 0 && System.Char.IsDigit operation[0])

              Expect.isEmpty leaked "a migration script was keyed as a query"

          testCase "a missing statement names the file it expected"
          <| fun _ ->
              let error =
                  Expect.throwsC (fun () -> SqlResources.get "nowhere" "nothing" |> ignore) id

              Expect.stringContains error.Message "sql/nowhere/nothing.sql" "the message names the expected file"

          testCase "every shipped statement compiles against the migrated schema"
          <| fun _ ->
              // Preparing compiles the statement without running it, so a misspelled column or a
              // table a migration renamed fails here rather than on the first call that needs it.
              use conn = new SqliteConnection(shared ())
              conn.Open()

              let failures =
                  [ for domain, operation in SqlResources.keys do
                        use cmd = conn.CreateCommand()
                        cmd.CommandText <- SqlResources.get domain operation

                        match
                            (try
                                Ok(cmd.Prepare())
                             with :? SqliteException as error ->
                                 Error error.Message)
                        with
                        | Ok() -> ()
                        | Error message -> $"{domain}/{operation}: {message}" ]

              Expect.isEmpty failures "every statement prepares"
              Expect.isGreaterThanOrEqual (List.length SqlResources.keys) 25 "and the stores' statements are all there" ]

module ByzantineSystems.Automata.Storage.Postgres.Tests.SqlResourceTests

open ByzantineSystems.Automata.Storage.Postgres
open Expecto

/// Every statement this assembly is expected to ship. Listed rather than derived, so that
/// deleting a file or mistyping an EmbeddedResource glob fails here instead of at the first call
/// that needed it, which in an integration-gated suite could be never.
let private expected =
    [ "command", "submit"
      "command", "claim"
      "command", "finalize"
      "command", "result"
      "command", "reschedule"
      "command", "extend_lease"
      "command", "by_id"
      "command", "by_idempotency_key"
      "command", "blocked_drift"
      "chart", "register"
      "chart", "by_version"
      "belief", "live"
      "belief", "as_of"
      "belief", "correct"
      "transition", "history"
      "system", "command_metrics"
      "supervision", "record"
      "supervision", "list_recent"
      "action", "ensure_queue"
      "action", "claim"
      "action", "complete"
      "action", "reschedule"
      "action", "abandon" ]

/// These run without a database: they are about what is embedded in the assembly, not about what
/// PostgreSQL does with it.
let tests =
    testList
        "SqlResources"
        [ test "every expected statement is embedded and non-empty" {
              for domain, operation in expected do
                  match SqlResources.tryGet domain operation with
                  | None -> failtestf "sql/%s/%s.sql is not embedded" domain operation
                  | Some statement ->
                      Expect.isGreaterThan
                          (statement.Trim().Length)
                          0
                          $"sql/{domain}/{operation}.sql should not be empty"
          }

          test "nothing unexpected is embedded under the sql tree" {
              let unexpected = SqlResources.keys |> List.except expected

              Expect.isEmpty
                  unexpected
                  "a new sql/ file should be added to this test's expected list, so that its key is deliberate"
          }

          test "the migration scripts are not mistaken for queries" {
              // Migrations are embedded from a sibling tree and must stay out of the map, or a
              // typo'd lookup could silently return a whole schema script.
              let migrationish =
                  SqlResources.keys
                  |> List.filter (fun (domain, _) -> domain = "main" || domain = "repeatable")

              Expect.isEmpty migrationish "migration scripts should not be keyed as queries"
          }

          test "a missing statement names the file and the known keys" {
              let failure =
                  try
                      SqlResources.get "command" "nope" |> ignore
                      None
                  with error ->
                      Some error

              match failure with
              | None -> failtest "a missing statement should raise rather than return"
              | Some error ->
                  Expect.stringContains
                      error.Message
                      "sql/command/nope.sql"
                      "the message should name the file that was expected"

                  Expect.stringContains error.Message "command/submit" "the message should list the known keys"
          } ]

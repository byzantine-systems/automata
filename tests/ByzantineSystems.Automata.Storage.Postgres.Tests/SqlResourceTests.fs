module ByzantineSystems.Automata.Storage.Postgres.Tests.SqlResourceTests

open System
open System.IO
open System.Text.RegularExpressions
open ByzantineSystems.Automata.Storage.Postgres
open Expecto

/// Every statement this assembly is expected to ship. Listed rather than derived, so that
/// deleting a file or mistyping an EmbeddedResource glob fails here instead of at the first call
/// that needed it, which in an integration-gated suite could be never.
let private expected =
    [ "command", "submit"
      "command", "claim"
      "command", "dead_letter_undecodable"
      "command", "by_id"
      "command", "find_by_key"
      "command", "finalize"
      "command", "finalize_failure"
      "command", "finalize_temporal"
      "command", "reschedule"
      "command", "extend_lease"
      "command", "repair_blocked"
      "command", "blocked_drift"
      "command", "submit_correction"
      "command", "finalize_correction"
      "belief", "as_of"
      "belief", "before"
      "belief", "valid_at"
      "belief", "correct"
      "belief", "current"
      "transition", "history_page"
      "transition", "result"
      "transition", "suffix"
      "transition", "purge_watermark"
      "chart", "register"
      "chart", "by_version"
      "system", "command_metrics"
      "system", "boot_check"
      "system", "profile"
      "system", "journal"
      "system", "register_maintenance"
      "system", "run_maintenance"
      "system", "notify_pending"
      "system", "schedule"
      "system", "unschedule"
      "system", "belief_cold_stale"
      "system", "refresh_belief_cold"
      "supervision", "record"
      "supervision", "recent"
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
          }

          test "embedded SQL uses only the two schema template variables" {
              let assembly = typeof<SchemaName>.Assembly
              let variable = Regex(@"\$[A-Za-z_][A-Za-z0-9_]*\$", RegexOptions.CultureInvariant)
              let known = set [ "$schema$"; "$temporal_schema$" ]

              let unknown =
                  assembly.GetManifestResourceNames()
                  |> Array.filter (fun name -> name.EndsWith(".sql", StringComparison.Ordinal))
                  |> Array.collect (fun name ->
                      use stream = assembly.GetManifestResourceStream name
                      use reader = new StreamReader(stream)

                      variable.Matches(reader.ReadToEnd())
                      |> Seq.cast<Match>
                      |> Seq.map (fun found -> name, found.Value)
                      |> Array.ofSeq)
                  |> Array.filter (snd >> known.Contains >> not)

              Expect.isEmpty
                  unknown
                  "a named dollar token is a template variable, not a PostgreSQL dollar quote; add it deliberately"
          }

          test "statement text is specialized per schema" {
              let alternate =
                  SchemaName.tryCreate "automata_ci"
                  |> Result.defaultWith (fun error -> failwithf "%A" error)

              let defaultText =
                  Statement.load SchemaName.defaults "command" "submit" |> Statement.text

              let alternateText = Statement.load alternate "command" "submit" |> Statement.text

              Expect.isFalse (defaultText = alternateText) "two schemas cannot share prepared statement text"
              Expect.stringContains defaultText "fsm.submit_command" "the default statement"
              Expect.stringContains alternateText "automata_ci.submit_command" "the alternate statement"
          } ]

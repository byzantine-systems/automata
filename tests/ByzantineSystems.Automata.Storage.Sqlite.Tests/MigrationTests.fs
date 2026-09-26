module ByzantineSystems.Automata.Storage.Sqlite.Tests.MigrationTests

open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Microsoft.Extensions.Logging.Abstractions
open Expecto

let private scripts =
    [ "001_chart_version.sql"
      "002_command.sql"
      "003_entity_snapshot.sql"
      "004_transition.sql"
      "005_action.sql" ]

let private migrate (connectionString: string) =
    match Migrator.migrate NullLogger.Instance connectionString with
    | Ok report -> report.Applied
    | Error error -> failtest $"migration failed: %A{error}"

/// Each test migrates a file of its own: what is under test is the migration itself.
[<Tests>]
let tests =
    testList
        "Migrator"
        [ testCase "a fresh database takes every script, in order"
          <| fun _ ->
              let connectionString = DataSource.connectionString (freshPath "fresh")
              let applied = migrate connectionString

              Expect.equal
                  (applied
                   |> List.map (fun name -> name.Substring(name.LastIndexOf ".migrations.main." + 17)))
                  scripts
                  "all five, numbered order"

              Expect.equal
                  (column connectionString "SELECT ScriptName FROM SchemaVersions ORDER BY ScriptName;" [])
                  applied
                  "and every one is journaled"

          testCase "a second run applies nothing"
          <| fun _ ->
              let connectionString = DataSource.connectionString (freshPath "rerun")
              migrate connectionString |> ignore
              Expect.isEmpty (migrate connectionString) "run-once scripts stay run"

          testCase "the file is left in WAL mode"
          <| fun _ ->
              let connectionString = migrated "wal"
              Expect.equal (scalar connectionString "PRAGMA journal_mode;") "wal" "a property of the file, kept"

          testCase "trigger bodies survive the script splitter"
          <| fun _ ->
              // Each trigger body holds a semicolon. A splitter that cut on it would have failed
              // the script, or created a trigger with half a body.
              let connectionString = migrated "triggers"

              let triggers =
                  column connectionString "SELECT name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name;" []

              Expect.equal
                  triggers
                  [ "fsm_command_submission_is_immutable"
                    "fsm_command_terminal_is_final"
                    "fsm_transition_is_append_only" ]
                  "every guard is installed"

          testCase "the migrated file is structurally sound"
          <| fun _ ->
              let connectionString = migrated "integrity"
              Expect.equal (scalar connectionString "PRAGMA integrity_check;") "ok" "integrity_check"
              Expect.isEmpty (column connectionString "PRAGMA foreign_key_check;" []) "foreign_key_check"

          testCase "an in-memory database is refused before any script runs"
          <| fun _ ->
              match Migrator.migrate NullLogger.Instance "Data Source=:memory:" with
              | Error(MigrationError.Unsupported reason) -> Expect.stringContains reason "in-memory" "says why"
              | other -> failtest $"expected a refusal, got %A{other}" ]

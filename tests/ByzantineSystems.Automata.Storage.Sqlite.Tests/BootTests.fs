module ByzantineSystems.Automata.Storage.Sqlite.Tests.BootTests

open System
open System.IO
open System.Threading.Tasks
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto
open Microsoft.Data.Sqlite

/// A file that can serve, as the pure check sees it.
let private healthy: FileCheck =
    { Version = System.Version(3, 53, 3)
      JournalMode = Wal
      HasSchema = true
      Applied = Some Boot.expectedMigrations }

let private defects check =
    Boot.defects check Boot.expectedMigrations

let private boot (connectionString: string) : Task<BootReport> =
    task {
        let store = storeOf (contextFor connectionString) :> IStoreBoot
        let! report = store.Boot(testMachine, noCancellation)
        return report |> expectOk "boot"
    }

let private pureTests =
    testList
        "defects"
        [ test "a healthy file has none" { Expect.isEmpty (defects healthy) "nothing to report" }

          test "an old SQLite is a missing prerequisite" {
              match
                  defects
                      { healthy with
                          Version = System.Version(3, 37, 2) }
              with
              | [ BootDefect.MissingPrerequisite what ] -> Expect.stringContains what "3.38" "it names the floor"
              | other -> failtestf "expected one missing prerequisite, got %A" other
          }

          test "a file outside WAL is misconfigured" {
              match
                  defects
                      { healthy with
                          JournalMode = Other "delete" }
              with
              | [ BootDefect.Misconfigured why ] -> Expect.stringContains why "delete" "it names the mode it found"
              | other -> failtestf "expected one misconfiguration, got %A" other
          }

          test "no schema is missing, whatever the journal says" {
              Expect.equal
                  (defects
                      { healthy with
                          HasSchema = false
                          Applied = None })
                  [ BootDefect.SchemaMissing ]
                  "the schema was never installed"
          }

          test "a schema without its journal is missing a prerequisite" {
              match defects { healthy with Applied = None } with
              | [ BootDefect.MissingPrerequisite what ] -> Expect.stringContains what "SchemaVersions" "the journal"
              | other -> failtestf "expected the journal to be reported, got %A" other
          }

          test "behind and ahead are both reported" {
              let applied =
                  (Boot.expectedMigrations |> List.take (Boot.expectedMigrations.Length - 1))
                  @ [ "future.migrations.main.999_future.sql" ]

              Expect.equal
                  (defects { healthy with Applied = Some applied })
                  [ BootDefect.SchemaBehind [ List.last Boot.expectedMigrations ]
                    BootDefect.SchemaAhead [ "future.migrations.main.999_future.sql" ] ]
                  "a file can be both when two branches' migrations meet"
          }

          test "the build ships the five numbered migrations" {
              Expect.hasLength Boot.expectedMigrations 5 "001 to 005"
          } ]

let private fileTests =
    testList
        "files"
        [ testTask "a migrated file boots ready" {
              let! report = boot (migrated "boot")
              Expect.equal report BootReport.Ready "nothing is missing"
          }

          testTask "a missing file is a missing schema, and is not created" {
              let path = freshPath "boot-missing"
              let! report = boot (DataSource.connectionString path)

              Expect.equal report (BootReport.Refused [ BootDefect.SchemaMissing ]) "nothing was installed"
              Expect.isFalse (File.Exists path) "asking did not create the file"
          }

          testTask "an unmigrated file is refused for its mode and its schema" {
              let db = DataSource.connectionString (freshPath "boot-bare")
              exec db "CREATE TABLE application_owned (x INTEGER);" []

              match! boot db with
              | BootReport.Refused [ BootDefect.Misconfigured _; BootDefect.SchemaMissing ] -> ()
              | other -> failtestf "expected the journal mode and the schema, got %A" other
          }

          testTask "a file whose journal lost a migration is behind" {
              let db = migrated "boot-behind"
              let last = List.last Boot.expectedMigrations
              exec db "DELETE FROM SchemaVersions WHERE ScriptName = @name;" [ "@name", box last ]

              let! report = boot db
              Expect.equal report (BootReport.Refused [ BootDefect.SchemaBehind [ last ] ]) "it names the migration"
          }

          testTask "a file with a migration this build does not know is ahead" {
              let db = migrated "boot-ahead"
              let future = "future.migrations.main.999_future.sql"

              exec
                  db
                  "INSERT INTO SchemaVersions (ScriptName, Applied) VALUES (@name, '2026-09-26 00:00:00');"
                  [ "@name", box future ]

              let! report = boot db
              Expect.equal report (BootReport.Refused [ BootDefect.SchemaAhead [ future ] ]) "it names the migration"
          }

          testTask "a file switched out of WAL is refused" {
              let db = migrated "boot-mode"

              // Leaving WAL needs the only connection to the file, so the pooled ones the
              // migration left behind are closed first.
              SqliteConnection.ClearPool(new SqliteConnection(db))
              Expect.equal (scalar db "PRAGMA journal_mode = DELETE;") "delete" "the file left WAL"
              SqliteConnection.ClearPool(new SqliteConnection(db))

              match! boot db with
              | BootReport.Refused [ BootDefect.Misconfigured why ] -> Expect.stringContains why "WAL" "it says why"
              | other -> failtestf "expected the journal mode to be refused, got %A" other
          }

          testTask "an in-memory database is refused before anything is opened" {
              match! boot "Data Source=:memory:" with
              | BootReport.Refused [ BootDefect.Misconfigured why ] ->
                  Expect.stringContains why "in-memory" "it says why"
              | other -> failtestf "expected an in-memory database to be refused, got %A" other
          } ]

let private locationTests =
    testList
        "Location"
        [ test "a connection string round-trips through its location" {
              let path = freshPath "location"
              let location = Location.ofConnectionString (DataSource.connectionString path)

              match location with
              | Missing file -> Expect.equal file.FullName (Path.GetFullPath path) "the file it names"
              | other -> failtestf "a file not yet created is missing, got %A" other

              Expect.equal
                  (Location.toConnectionString location)
                  (Some(DataSource.connectionString (Path.GetFullPath path)))
                  "and back to the connection string the stores use"
          }

          test "an in-memory database has no file" {
              let location = Location.ofConnectionString "Data Source=:memory:"
              Expect.equal location InMemory "nothing on disk"
              Expect.isNone (Location.toConnectionString location) "and nothing to connect a store to"
          } ]

let tests = testList "IStoreBoot" [ pureTests; fileTests; locationTests ]

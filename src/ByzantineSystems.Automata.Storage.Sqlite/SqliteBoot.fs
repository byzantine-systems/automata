namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open System.IO
open System.Reflection
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Microsoft.Data.Sqlite

/// <summary>Where a connection string points, decided before anything is opened.</summary>
type internal Location =
    /// <summary>A database that lives inside one connection. The stores cannot serve from it.</summary>
    | InMemory

    /// <summary>A file that does not exist. Opening it read-only would fail, and must not create it.</summary>
    | Missing of file: FileInfo

    /// <summary>A file that exists, and can be asked about itself.</summary>
    | OnDisk of file: FileInfo

/// <summary>Conversions between <see cref="T:ByzantineSystems.Automata.Storage.Sqlite.Location" /> and connection strings.</summary>
[<RequireQualifiedAccess>]
module internal Location =

    /// <summary>
    /// Where a connection string points. A relative path resolves against the current directory,
    /// as SQLite resolves it. Only this reads the filesystem.
    /// </summary>
    let ofConnectionString (connectionString: string) : Location =
        if DataSource.isInMemory connectionString then
            InMemory
        else
            let file = FileInfo(WriteGate.keyOf connectionString)

            if file.Exists then OnDisk file else Missing file

    /// <summary>The file a location names, or <c>None</c> for an in-memory database.</summary>
    let file (location: Location) : FileInfo option =
        match location with
        | InMemory -> None
        | Missing file
        | OnDisk file -> Some file

    /// <summary>
    /// The read-write connection string the stores use for a location's file, or <c>None</c> for
    /// an in-memory database, which they refuse.
    /// </summary>
    let toConnectionString (location: Location) : string option =
        file location
        |> Option.map (fun file -> DataSource.connectionString file.FullName)

/// <summary>The journal mode a file is in. Only WAL serves; anything else is named for the report.</summary>
type internal JournalMode =
    | Wal
    | Other of mode: string

/// <summary>What a database file says about itself, before anything in it is trusted.</summary>
type internal FileCheck =
    {
        Version: Version
        JournalMode: JournalMode
        /// <summary>Whether any <c>fsm_</c> table exists: the schema was installed at all.</summary>
        HasSchema: bool
        /// <summary>The applied migrations, or <c>None</c> when DbUp's journal does not exist.</summary>
        Applied: string list option
    }

/// <summary>
/// Deciding whether a database file can serve, as pure functions over what was read from it, so
/// every combination can be tested without breaking a real file to produce it.
/// </summary>
[<RequireQualifiedAccess>]
module internal Boot =

    /// <summary>The numbered migrations this build embeds, by the resource name DbUp journals them under.</summary>
    let expectedMigrations: string list =
        Assembly.GetExecutingAssembly().GetManifestResourceNames()
        |> Array.filter (fun name -> name.Contains ".migrations.main.")
        |> Array.sort
        |> List.ofArray

    /// <summary>
    /// Compares what this build ships with what the file applied. Behind and ahead are both
    /// reported, because a file can be both at once when two branches' migrations meet.
    /// </summary>
    let compare (expected: string list) (applied: string list) : BootDefect list =
        let pending = expected |> List.except applied
        let unknown = applied |> List.except expected

        [ if not pending.IsEmpty then
              BootDefect.SchemaBehind pending
          if not unknown.IsEmpty then
              BootDefect.SchemaAhead unknown ]

    let journalMode (answer: string) : JournalMode =
        match answer with
        | "wal" -> Wal
        | other -> Other other

    /// <summary>Every defect a file shows, empty when it can serve.</summary>
    let defects (check: FileCheck) (expected: string list) : BootDefect list =
        [ if check.Version < Migrator.minimumVersion then
              BootDefect.MissingPrerequisite $"SQLite %O{Migrator.minimumVersion} or later (this is %O{check.Version})"

          match check.JournalMode with
          | Wal -> ()
          | Other mode ->
              // WAL is what lets reads run beside the writer, and the migrator sets it. A file in
              // any other mode was migrated by something else, or copied without its mode.
              BootDefect.Misconfigured
                  $"the database is in %s{mode} journal mode rather than WAL; Migrator.migrate sets it"

          if not check.HasSchema then
              BootDefect.SchemaMissing
          else
              match check.Applied with
              | None -> BootDefect.MissingPrerequisite "migration journal SchemaVersions"
              | Some applied -> yield! compare expected applied ]

    /// <summary>
    /// Reads a file's check over a read-only connection. Never fails for a missing object; see
    /// boot_check.sql.
    /// </summary>
    let private read (connectionString: string) (ct: CancellationToken) : Task<FileCheck> =
        backgroundTask {
            use! conn = Db.openConnection (DataSource.readOnly connectionString) ct

            let command (sql: string) =
                let cmd = conn.CreateCommand()
                cmd.CommandText <- sql
                cmd

            use check = command (SqlResources.get "system" "boot_check")
            use! reader = check.ExecuteReaderAsync ct
            let! _ = reader.ReadAsync ct
            let version = Version.Parse(Row.string reader "version")
            let hasSchema = Row.bool reader "has_schema"
            let hasJournal = Row.bool reader "has_journal"
            do! reader.CloseAsync()

            use pragma = command "PRAGMA journal_mode;"
            let! mode = pragma.ExecuteScalarAsync ct

            let! applied =
                if hasJournal then
                    backgroundTask {
                        use journal = command (SqlResources.get "system" "journal")
                        use! rows = journal.ExecuteReaderAsync ct
                        let! names = Row.all (fun row -> Row.string row "script_name") rows ct
                        return Some names
                    }
                else
                    Task.FromResult None

            return
                { Version = version
                  JournalMode = journalMode (string mode)
                  HasSchema = hasSchema
                  Applied = applied }
        }

    /// <summary>Everything the file is missing, empty when it can serve.</summary>
    let inspect (context: SqliteContext) (ct: CancellationToken) : Task<Result<BootDefect list, StoreError>> =
        match Location.ofConnectionString context.ConnectionString with
        | InMemory ->
            Task.FromResult(
                Ok
                    [ BootDefect.Misconfigured
                          "an in-memory database: each pooled connection would open a different one, and it cannot run in WAL mode" ]
            )
        | Missing _ -> Task.FromResult(Ok [ BootDefect.SchemaMissing ])
        | OnDisk _ ->
            Db.protect
                context.Resilience
                (fun cancel ->
                    backgroundTask {
                        let! check = read context.ConnectionString cancel
                        return Ok(defects check expectedMigrations)
                    })
                ct

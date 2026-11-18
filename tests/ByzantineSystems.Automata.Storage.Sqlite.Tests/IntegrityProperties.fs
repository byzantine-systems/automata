module ByzantineSystems.Automata.Storage.Sqlite.Tests.IntegrityProperties

open System
open Expecto
open FsCheck
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.DatabaseIntegrity
open ByzantineSystems.Automata.Storage.LifecycleModel
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Microsoft.Data.Sqlite

let private fresh () : Database =
    let db = fixture "integrity-laws"
    let conn = new SqliteConnection(db.Db)
    conn.Open()
    let transaction = conn.BeginTransaction(deferred = false)

    let execute sql =
        use cmd = conn.CreateCommand()
        cmd.Transaction <- transaction
        cmd.CommandText <- sql
        cmd.ExecuteNonQuery() |> ignore

    let attempt sql =
        execute "SAVEPOINT integrity_probe"

        try
            execute sql
            execute "RELEASE integrity_probe"
        with _ ->
            execute "ROLLBACK TO integrity_probe; RELEASE integrity_probe"
            reraise ()

    let read sql =
        use cmd = conn.CreateCommand()
        cmd.Transaction <- transaction
        cmd.CommandText <- sql
        use reader = cmd.ExecuteReader()

        [ while reader.Read() do
              [ for index in 0 .. reader.FieldCount - 1 do
                    match reader.GetValue index with
                    | :? (byte array) as bytes -> Convert.ToHexString bytes
                    | value -> Convert.ToString(value, Globalization.CultureInfo.InvariantCulture) ] ]

    let scalar sql =
        read sql |> List.exactlyOne |> List.exactlyOne

    let mutable next = 0

    { Table = fun name -> "fsm_" + name
      Execute = attempt
      Refuses =
        fun sql ->
            try
                attempt sql
                false
            with :? SqliteException as error when error.SqliteErrorCode = 19 ->
                true
      Scalar = scalar
      Snapshot =
        fun () ->
            [ "command", "command_id"
              "command_error", "command_id"
              "command_error_domain", "command_id"
              "command_error_machine", "command_id"
              "command_error_replay", "command_id"
              "command_correction", "command_id"
              "transition", "command_id"
              "action_dead_letter", "command_id, ordinal" ]
            |> List.map (fun (name, order) -> read $"SELECT * FROM fsm_{name} ORDER BY {order}")
      Columns =
        fun table ->
            read $"SELECT name FROM pragma_table_info('fsm_{table}') ORDER BY cid"
            |> List.collect id
      Insert =
        fun status kind ->
            next <- next + 1

            attempt
                $"INSERT INTO fsm_command(machine_id, entity_id, seq, idempotency_key, chart_version, event, request_digest, kind, status, visible_at, lease_token, received_at)
                    VALUES ('{machine}', 'entity-{next}', 1, 'key-{next}', 1, '{{}}', x'01', '{kind}', '{Lifecycle.sql status}', 0, {if status = CommandStatus.Ready then 0 else 7}, 0)"

            scalar "SELECT last_insert_rowid()" |> int64
      Now = "0"
      Dispose =
        fun () ->
            transaction.Dispose()
            conn.Dispose() }

let private driverProperty =
    testCase "integrity failures are never transient retry candidates" (fun () ->
        Check.One(
            Config.QuickThrowOnFailure,
            fun (seed: int) ->
                [ 275; 787; 1555; 1811; 2067 ]
                |> List.forall (fun code -> not (Db.isTransient (SqliteException($"guard-{seed}", 19, code))))
        ))

let tests =
    testList
        "SQLite database integrity"
        [ lifecycleTests fresh
          ownershipRollbackTests fresh
          immutableTests fresh
          replacementTests fresh
          driverProperty ]

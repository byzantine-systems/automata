module ByzantineSystems.Automata.Storage.Postgres.Tests.IntegrityProperties

open System
open System.Threading.Tasks
open Expecto
open FsCheck
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.DatabaseIntegrity
open ByzantineSystems.Automata.Storage.LifecycleModel
open ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext
open Npgsql

let private freshFor (schemaValue: string) (initialize: unit -> Task) () : Database =
    (initialize ()).GetAwaiter().GetResult()
    let conn = new NpgsqlConnection(connectionString)
    conn.Open()
    let transaction = conn.BeginTransaction()

    let execute text =
        use cmd = new NpgsqlCommand(text, conn, transaction)
        cmd.ExecuteNonQuery() |> ignore

    let attempt text =
        execute "SAVEPOINT integrity_probe"

        try
            execute text
            execute "RELEASE SAVEPOINT integrity_probe"
        with _ ->
            execute "ROLLBACK TO SAVEPOINT integrity_probe; RELEASE SAVEPOINT integrity_probe"
            reraise ()

    let read text =
        use cmd = new NpgsqlCommand(text, conn, transaction)
        use reader = cmd.ExecuteReader()

        [ while reader.Read() do
              [ for index in 0 .. reader.FieldCount - 1 do
                    match reader.GetValue index with
                    | :? (byte array) as bytes -> Convert.ToHexString bytes
                    | :? (string array) as values -> System.Text.Json.JsonSerializer.Serialize values
                    | value -> Convert.ToString(value, Globalization.CultureInfo.InvariantCulture) ] ]

    let scalar text =
        read text |> List.exactlyOne |> List.exactlyOne

    let mutable next = 0

    { Table = fun name -> schemaValue + "." + name
      Execute = attempt
      Refuses =
        fun text ->
            try
                attempt text
                false
            // GENERATED ALWAYS identities refuse assignment during rewrite,
            // before row triggers. Do not mistake that protection for bad SQL.
            with :? PostgresException as error when error.SqlState.StartsWith "23" || error.SqlState = "428C9" ->
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
            |> List.map (fun (name, order) -> read $"SELECT * FROM {schemaValue}.{name} ORDER BY {order}")
      Columns =
        fun table ->
            read
                $"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schemaValue}' AND table_name = '{table}' ORDER BY ordinal_position"
            |> List.collect id
      Insert =
        fun status kind ->
            next <- next + 1

            attempt
                $"INSERT INTO {schemaValue}.command(machine_id, entity_id, seq, idempotency_key, chart_version, event, request_digest, kind, status, lease_token)
                    VALUES ('{MachineId.value machine}', 'entity-{next}', 1, 'key-{next}', 1, '{{}}', decode('01', 'hex'), '{kind}', '{Lifecycle.sql status}', {if status = CommandStatus.Ready then 0 else 7})"

            scalar $"SELECT currval(pg_get_serial_sequence('{schemaValue}.command', 'command_id'))"
            |> int64
      Now = "CURRENT_TIMESTAMP"
      Dispose =
        fun () ->
            transaction.Dispose()
            conn.Dispose() }

let private fresh () = freshFor schemaValue reset ()

let forInstallation schema initialize =
    let fresh () = freshFor schema initialize ()

    testList
        "snapshot database integrity"
        [ lifecycleTests fresh
          ownershipRollbackTests fresh
          immutableTests fresh
          upsertTests fresh ]

let private auditProperties =
    testCase "supervision and quarantine facts reject every column update and retain duplicate no-ops" (fun () ->
        Check.One(
            Config.QuickThrowOnFailure.WithMaxTest(12),
            fun (seed: int) ->
                let db = fresh ()

                try
                    let quarantine = db.Table "action_quarantine"
                    let supervision = db.Table "supervision_event"

                    db.Execute
                        $"INSERT INTO {quarantine}(queue, msg_id, message, reason, deliveries) VALUES ('integrity', 1, '{{\"seed\":{seed}}}', 'poison', 1)"

                    db.Execute
                        $"INSERT INTO {supervision}(supervisor, child_id, kind, strategy, reason, at) VALUES ('integrity', 'child', 'Started', 'OneForOne', 'seed-{seed}', CURRENT_TIMESTAMP)"

                    let protectedFields =
                        [ "action_quarantine"; "supervision_event" ]
                        |> List.forall (fun name ->
                            let table = db.Table name

                            db.Columns name
                            |> List.forall (fun field -> db.Refuses $"UPDATE {table} SET {field} = {field}"))

                    db.Execute
                        $"INSERT INTO {quarantine}(queue, msg_id, message, reason, deliveries) VALUES ('integrity', 1, '{{}}', 'duplicate', 1) ON CONFLICT DO NOTHING"

                    db.Execute $"DELETE FROM {quarantine}"
                    db.Execute $"DELETE FROM {supervision}"

                    protectedFields
                    && db.Scalar $"SELECT count(*) FROM {quarantine}" = "0"
                    && db.Scalar $"SELECT count(*) FROM {supervision}" = "0"
                finally
                    db.Dispose()
        ))

let private driverProperty =
    testCase "integrity failures are never transient retry candidates" (fun () ->
        Check.One(
            Config.QuickThrowOnFailure,
            fun (seed: int) ->
                [ "23000"; "23502"; "23503"; "23505"; "23514" ]
                |> List.forall (fun code ->
                    not (Db.isTransient (PostgresException($"guard-{seed}", "ERROR", "ERROR", code))))
        ))

let tests =
    testList
        "Postgres database integrity"
        [ lifecycleTests fresh
          ownershipRollbackTests fresh
          immutableTests fresh
          upsertTests fresh
          auditProperties
          driverProperty ]

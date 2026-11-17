module ByzantineSystems.Automata.Storage.Postgres.Tests.IntegrityProperties

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.DatabaseIntegrity
open ByzantineSystems.Automata.Storage.LifecycleModel
open ByzantineSystems.Automata.Storage.Postgres.Tests.TestContext
open Npgsql

let private fresh () : Database =
    (reset ()).GetAwaiter().GetResult()
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
            with :? PostgresException ->
                true
      Scalar = scalar
      Snapshot =
        fun () ->
            [ "command", "command_id"
              "command_error", "command_id"
              "command_error_domain", "command_id"
              "command_error_machine", "command_id"
              "command_error_replay", "command_id"
              "command_correction", "command_id" ]
            |> List.map (fun (name, order) -> read $"SELECT * FROM {schemaValue}.{name} ORDER BY {order}")
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

let tests = lifecycleTests fresh

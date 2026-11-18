module ByzantineSystems.Automata.PayloadBench

open System
open System.Diagnostics
open System.Text
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Resilience
open Microsoft.Extensions.Logging.Abstractions
open Npgsql

type Entity = class end
type State = { Value: int; Payload: string }
type Event = { Delta: int; Payload: string }
type Store = IMachineStore<EntityId<Entity>, State, Event, string, string>

let private require =
    function
    | Ok value -> value
    | Error error -> failwithf "benchmark operation failed: %A" error

let private run connectionString label beliefs emitActions =
    task {
        let profile = BeliefModel.value beliefs
        let actionProfile = if emitActions then "actions" else "plain"
        let name = $"bench_g_{label}_{profile}_{actionProfile}"
        let schema = SchemaName.tryCreate name |> require
        let queue = name + "_q"
        use source = DataSource.create connectionString

        let execute sql =
            use connection = source.OpenConnection()
            use command = new NpgsqlCommand(sql, connection)
            command.ExecuteNonQuery() |> ignore

        let scalar sql =
            use connection = source.OpenConnection()
            use command = new NpgsqlCommand(sql, connection)
            Convert.ToInt64(command.ExecuteScalar())

        execute $"DROP SCHEMA IF EXISTS {name}_temporal CASCADE; DROP SCHEMA IF EXISTS {name} CASCADE"

        Migrator.migrate
            NullLogger.Instance
            { ConnectionString = connectionString
              Schema = schema
              Beliefs = beliefs }
        |> require
        |> ignore

        source.Clear()

        execute
            $"SELECT {name}.ensure_action_queue('{queue}'); SELECT pgmq.purge_queue('{queue}'); DELETE FROM pgmq.a_{queue};
                  INSERT INTO {name}.machine_chart_version(machine_id,version,fingerprint) VALUES('bench',1,repeat('a',64));
                  INSERT INTO {name}.machine_maintenance(machine_id,action_queue) VALUES('bench','{queue}')"

        let context =
            PostgresContext.create source TransientPolicy.defaults ignore
            |> PostgresContext.withSchema schema

        let mutable encodedBytes = 0L

        let countCodec (codec: Codec<'T>) =
            { codec with
                Encode =
                    fun value ->
                        codec.Encode value
                        |> Result.map (fun json ->
                            encodedBytes <- encodedBytes + int64 (Encoding.UTF8.GetByteCount json)
                            json) }

        let options =
            let original =
                MachineStoreOptions.forEntityId<Entity, State, Event, string, string>
                    context
                    (ActionDelivery.Deliver(ActionQueueName.create queue))

            { original with
                StateCodec = countCodec original.StateCodec
                EventCodec = countCodec original.EventCodec
                ErrorCodec = countCodec original.ErrorCodec }

        let store: Store =
            match beliefs with
            | BeliefModel.Snapshot -> PostgresMachineStore(options) :> Store
            | BeliefModel.Bitemporal -> PostgresBitemporalMachineStore(MachineStoreOptions.bitemporal options) :> Store

        let inbox = store :> ICommandInbox<EntityId<Entity>, Event>

        let processor =
            store :> ICommandProcessorStore<EntityId<Entity>, State, Event, string, string>

        let reader = store :> IStateReader<EntityId<Entity>, State, Event, string>
        let actions = store :> IActionQueue<EntityId<Entity>, string>
        let ct = CancellationToken.None
        let machine = machineId "bench"
        let version = ChartVersion.create 1
        let payload = String.replicate 1024 "x"
        let origin = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
        let event = { Delta = 1; Payload = payload }

        let measure operation warmup count (action: int -> Task<unit>) =
            task {
                for index in 0 .. warmup - 1 do
                    do! action index

                let times = ResizeArray<float>()
                let mutable allocated, bytes = 0L, 0L

                for index in warmup .. warmup + count - 1 do
                    let beforeBytes = encodedBytes
                    let beforeAllocation = GC.GetTotalAllocatedBytes(false)
                    let started = Stopwatch.GetTimestamp()
                    do! action index
                    times.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds)
                    allocated <- allocated + GC.GetTotalAllocatedBytes(false) - beforeAllocation
                    bytes <- bytes + encodedBytes - beforeBytes

                let sorted = times |> Seq.sort |> Seq.toArray

                let percentile p =
                    sorted[int (ceil (p * float count)) - 1]

                printfn
                    "%s,%s,%b,%s,%d,%.3f,%.3f,%d,%d"
                    label
                    profile
                    emitActions
                    operation
                    count
                    (percentile 0.50)
                    (percentile 0.95)
                    (allocated / int64 count)
                    (bytes / int64 count)
            }

        let submit entity key =
            task {
                let! outcome =
                    inbox.SubmitAsync(
                        { MachineId = machine
                          EntityId = entity
                          IdempotencyKey = key
                          ChartVersion = version
                          Kind = CommandKind.Event
                          Event = event
                          VisibleAt = None
                          ReceivedAt = None
                          Audit = AuditContext.empty },
                        ct
                    )

                require outcome |> ignore

                let! claimed =
                    inbox.ClaimAsync(machine, ClaimBatch.create 1, LeaseDuration.create (TimeSpan.FromMinutes 1.), ct)

                return require claimed |> List.exactlyOne
            }

        let entity: EntityId<Entity> = entityId "commits"
        let mutable state = { Value = 0; Payload = payload }
        let mutable epoch = Epoch.initial
        let mutable lastCommand = CommandId.ofInt64 1L
        // Submission and claim are included in this end-to-end sample; each sample finalizes one command.
        do!
            measure "submit-claim-commit" 50 200 (fun index ->
                task {
                    let! held = submit entity (string index)
                    let next = { state with Value = state.Value + 1 }

                    let draft =
                        { MachineId = machine
                          EntityId = entity
                          Event = event
                          Actions = if emitActions then [ payload ] else []
                          FromState = state
                          ToState = next
                          HandledBy = stateId "root"
                          Exited = []
                          Entered = []
                          Status = Running
                          EffectiveAt = origin.AddSeconds(float index) }

                    let! outcome = processor.CommitAsync(held.Work.CommandId, held.Token, epoch, draft, ct)
                    require outcome |> ignore
                    state <- next
                    epoch <- Epoch.next epoch
                    lastCommand <- held.Work.CommandId
                })

        do!
            measure "result" 10 100 (fun _ ->
                task {
                    let! result = processor.TryGetResultAsync(lastCommand, ct)
                    require result |> ignore
                })

        do!
            measure "history-50" 10 50 (fun _ ->
                task {
                    let! result = reader.HistoryAsync(machine, entity, Page.create 50, ct)
                    require result |> ignore
                })

        do!
            measure "submit-claim-reject" 10 50 (fun index ->
                task {
                    let! held = submit (entityId $"failure-{index}") "failure"

                    let! result =
                        processor.RejectAsync(held.Work.CommandId, held.Token, CommandFailure.Machine payload, ct)

                    require result |> ignore
                })

        if emitActions then
            do!
                measure "claim-complete-action" 10 100 (fun _ ->
                    task {
                        let! result =
                            actions.ClaimAsync(
                                machine,
                                ClaimBatch.create 1,
                                LeaseDuration.create (TimeSpan.FromMinutes 1.),
                                ct
                            )

                        let held = require result |> List.exactlyOne
                        let! result = actions.CompleteAsync(held, ct)
                        require result |> ignore
                    })

        match store with
        | :? ICorrectionStore<EntityId<Entity>, State> as corrections ->
            for length in [ 1; 8; 32 ] do
                let cut = origin.AddHours 1.

                let beliefs =
                    [ for index in 0 .. length - 1 ->
                          { Asserted =
                              { State = { state with Value = index }
                                Status = Running
                                Epoch = epoch }
                            ValidFrom = cut.AddSeconds(float index)
                            CommandId = lastCommand
                            ChartVersion = version } ]

                do!
                    measure $"operator-correct-{length}" 5 20 (fun _ ->
                        task {
                            let! result = corrections.CorrectAsync(machine, entity, epoch, cut, beliefs, ct)
                            require result |> ignore
                        })
        | _ -> ()

        printfn
            "storage,%s,%s,%b,transition_row_bytes,%d"
            label
            profile
            emitActions
            (scalar $"SELECT sum(pg_column_size(t)) FROM {name}.transition t")

        // Failed commands need a newer sibling to become retention candidates.
        // Do not age immutable requests after submission: infinity is an explicit
        // benchmark cutoff. Every sample must remove a full batch, never a no-op.
        for index in 0..59 do
            let! result =
                inbox.SubmitAsync(
                    { MachineId = machine
                      EntityId = entityId $"failure-{index}"
                      IdempotencyKey = "successor"
                      ChartVersion = version
                      Kind = CommandKind.Event
                      Event = event
                      VisibleAt = None
                      ReceivedAt = None
                      Audit = AuditContext.empty },
                    ct
                )

            require result |> ignore

        do!
            measure "purge-commands-25" 0 12 (fun _ ->
                task {
                    let purged = scalar $"SELECT {name}.purge_commands('bench', 'infinity', 25)"

                    if purged <> 25L then
                        invalidOp $"purge sample removed {purged} commands instead of 25"
                })

        execute
            $"SELECT pgmq.drop_queue('{queue}'); DROP SCHEMA IF EXISTS {name}_temporal CASCADE; DROP SCHEMA {name} CASCADE"
    }

[<EntryPoint>]
let main argv =
    let connection = Environment.GetEnvironmentVariable "AUTOMATA_BENCH_DB"

    if String.IsNullOrWhiteSpace connection then
        invalidOp "Set AUTOMATA_BENCH_DB to a disposable benchmark database."

    let label = if argv.Length = 1 then argv[0] else "after"

    printfn
        "revision,profile,actions,operation,samples,p50_ms,p95_ms,process_allocated_bytes_per_op,codec_encoded_bytes_per_op"

    for profile in [ BeliefModel.Snapshot; BeliefModel.Bitemporal ] do
        for actions in [ false; true ] do
            run connection label profile actions
            |> fun task -> task.GetAwaiter().GetResult()

    0

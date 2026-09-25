namespace ByzantineSystems.Automata.Storage.Sqlite

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Schema
open FsToolkit.ErrorHandling
open SqlHydra.Query

/// <summary>What the chart registry needs.</summary>
type ChartRegistryOptions = { Context: SqliteContext }

/// <summary>
/// The SQLite chart registry.
///
/// Registration is an insert that does nothing on conflict, followed by a read, in one write
/// transaction. The file has one writer, so the read cannot race a concurrent registration the
/// way PostgreSQL's must allow for.
/// </summary>
type SqliteChartRegistry(options: ChartRegistryOptions) =

    let context = options.Context

    let parse (value: string) : Result<ChartFingerprint, StoreError> =
        // A string literal rather than nameof: the type has a private single case of the same
        // name, and nameof binds to the constructor, which is not accessible here.
        ChartFingerprint.tryCreate value
        |> Result.mapError (Db.decodeFailure "ChartFingerprint")

    interface IChartRegistry with

        member _.Register(identity, ct) =
            let machineId = MachineId.value identity.MachineId
            let version = ChartVersion.value identity.Version
            let fingerprint = ChartFingerprint.value identity.Fingerprint

            Db.protect
                context.Resilience
                (fun token ->
                    Db.writeTransaction
                        context.ConnectionString
                        context.Clock
                        (fun conn transaction now cancel ->
                            backgroundTask {
                                let! inserted =
                                    Statement.execute
                                        conn
                                        transaction
                                        (SqlResources.get "chart" "register")
                                        [ "@machine_id", box machineId
                                          "@version", box version
                                          "@fingerprint", box fingerprint
                                          "@now", box now ]
                                        cancel

                                if inserted = 1 then
                                    return Ok ChartRegistration.Registered
                                else
                                    let! stored =
                                        Statement.tryOne
                                            conn
                                            transaction
                                            (SqlResources.get "chart" "by_version")
                                            [ "@machine_id", box machineId; "@version", box version ]
                                            (fun reader -> Row.string reader "fingerprint")
                                            cancel

                                    match stored with
                                    | None ->
                                        return
                                            Error(
                                                Db.decodeFailure
                                                    (nameof ChartRegistration)
                                                    "a chart version was neither inserted nor found"
                                            )
                                    | Some stored when stored = fingerprint -> return Ok ChartRegistration.Matched
                                    | Some stored -> return parse stored |> Result.map ChartRegistration.Mismatched
                            })
                        token)
                ct

        member _.TryGet(machineId, version, ct) =
            let machine = MachineId.value machineId
            let number = int64 (ChartVersion.value version)

            Db.query
                context.Resilience
                context.ReadConnectionString
                (fun query token ->
                    backgroundTask {
                        let! stored =
                            selectTask query {
                                for v in main.fsm_machine_chart_version do
                                    where (v.machine_id = machine && v.version = number)
                                    select v.fingerprint
                                    tryHead
                                    cancel token
                            }

                        return stored |> Option.traverseResult parse
                    })
                ct

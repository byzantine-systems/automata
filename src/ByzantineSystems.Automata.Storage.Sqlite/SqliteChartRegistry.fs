namespace ByzantineSystems.Automata.Storage.Sqlite

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
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

    let register = Statement.load "chart" "register"

    let byVersion = Statement.load "chart" "by_version"

    let parse (value: string) : Result<ChartFingerprint, StoreError> =
        // A string literal rather than nameof: the type has a private single case of the same
        // name, and nameof binds to the constructor, which is not accessible here.
        ChartFingerprint.tryCreate value
        |> Result.mapError (Db.decodeFailure "ChartFingerprint")

    interface IChartRegistry with

        member _.Register(identity, ct) =
            let key =
                [ "@machine_id", Param.Text(MachineId.value identity.MachineId)
                  "@version", Param.Integer(int64 (ChartVersion.value identity.Version)) ]

            let fingerprint = ChartFingerprint.value identity.Fingerprint

            Sql.write
                context
                (sqliteWrite {
                    let! now = Sql.now

                    let! inserted =
                        Sql.execute
                            register
                            (key @ [ "@fingerprint", Param.Text fingerprint; "@now", Param.Integer now ])

                    if inserted = 1 then
                        return ChartRegistration.Registered
                    else
                        let! stored =
                            Sql.one (nameof ChartRegistration) byVersion key (fun reader ->
                                Row.string reader "fingerprint")

                        if stored = fingerprint then
                            return ChartRegistration.Matched
                        else
                            let! parsed = parse stored
                            return ChartRegistration.Mismatched parsed
                })
                ct

        member _.TryGet(machineId, version, ct) =
            let machine = MachineId.value machineId
            let number = int64 (ChartVersion.value version)

            Sql.read
                context
                (sqliteRead {
                    let! stored =
                        Sql.select (fun query token ->
                            selectTask query {
                                for v in main.fsm_machine_chart_version do
                                    where (v.machine_id = machine && v.version = number)
                                    select v.fingerprint
                                    tryHead
                                    cancel token
                            })

                    return! stored |> Option.traverseResult parse
                })
                ct

namespace ByzantineSystems.Automata.Storage.Sqlite

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal

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

    let register = StatementKey.create "chart" "register" |> Sql.prepareProgram

    let byVersion = StatementKey.create "chart" "by_version" |> Sql.prepareProgram

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

            Sql.writeProgramWith
                context
                (fun now ->
                    store {
                        let! inserted =
                            StoreProgram.executeCount
                                register
                                (key @ [ "@fingerprint", Param.Text fingerprint; "@now", Param.Integer now ])

                        if inserted = 1 then
                            return ChartRegistration.Registered
                        else
                            let! stored =
                                StoreProgram.queryOne byVersion key (fun reader -> Ok(Row.string reader "fingerprint"))

                            if stored = fingerprint then
                                return ChartRegistration.Matched
                            else
                                let! parsed = parse stored
                                return ChartRegistration.Mismatched parsed
                    })
                ct

        member _.TryGet(machineId, version, ct) =
            Sql.readProgram
                context
                (StoreProgram.tryQueryOne
                    byVersion
                    [ "@machine_id", Param.Text(MachineId.value machineId)
                      "@version", Param.Integer(int64 (ChartVersion.value version)) ]
                    (fun reader -> parse (Row.string reader "fingerprint")))
                ct

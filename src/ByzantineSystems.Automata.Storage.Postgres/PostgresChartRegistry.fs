namespace ByzantineSystems.Automata.Storage.Postgres

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open FsToolkit.ErrorHandling

/// <summary>What the chart registry needs. Only a data source: it reads and writes text.</summary>
type ChartRegistryOptions = { Context: PostgresContext }

/// <summary>
/// The PostgreSQL record of which chart structure each declared version belongs to.
///
/// Registration is the first thing a process should do for a machine and the last chance to
/// notice that a chart was edited without bumping its version. After that, commands pin the
/// version and a foreign key holds them to one that exists.
/// </summary>
type PostgresChartRegistry(options: ChartRegistryOptions) =

    let context = options.Context

    let register = Statement.load "chart" "register"

    let byVersion = Statement.load "chart" "by_version"

    /// A stored fingerprint that no longer parses is a corrupt row, reported as the contract's
    /// serialization failure rather than as an exception out of the store. The shape constraint
    /// on the column should make this unreachable; if the two ever disagree, nothing else
    /// reports it.
    let parse (value: string) : Result<ChartFingerprint, StoreError> =
        // A string literal rather than nameof: the type has a private single case of the same
        // name, and nameof binds to the constructor, which is not accessible here.
        ChartFingerprint.tryCreate value
        |> Result.mapError (Db.decodeFailure "ChartFingerprint")

    let key (machineId: MachineId) (version: ChartVersion) =
        [ "machine_id", Param.Text(MachineId.value machineId)
          "version", Param.Int(ChartVersion.value version) ]

    interface IChartRegistry with

        member _.Register(identity, ct) =
            Sql.run
                context
                (postgres {
                    let! registration, stored =
                        Sql.one
                            (nameof ChartRegistration)
                            register
                            (key identity.MachineId identity.Version
                             @ [ "fingerprint", Param.Text(ChartFingerprint.value identity.Fingerprint) ])
                            (fun reader -> Row.string reader "registration", Row.string reader "stored_fingerprint")

                    match registration with
                    | "registered" -> return ChartRegistration.Registered
                    | "matched" -> return ChartRegistration.Matched
                    | "mismatched" ->
                        let! fingerprint = parse stored
                        return ChartRegistration.Mismatched fingerprint
                    | other ->
                        return!
                            Op.fail (
                                Db.decodeFailure
                                    (nameof ChartRegistration)
                                    $"unknown chart registration outcome {other}"
                            )
                })
                ct

        member _.TryGet(machineId, version, ct) =
            Sql.run
                context
                (postgres {
                    let! stored =
                        Sql.tryOne "ChartFingerprint" byVersion (key machineId version) (fun reader ->
                            Row.string reader "fingerprint")

                    return! stored |> Option.traverseResult parse
                })
                ct

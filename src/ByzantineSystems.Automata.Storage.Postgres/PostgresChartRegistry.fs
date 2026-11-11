namespace ByzantineSystems.Automata.Storage.Postgres

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open FsToolkit.ErrorHandling

/// <summary>What the chart registry needs. Only a data source: it reads and writes text.</summary>
type ChartRegistryOptions = { Context: PostgresContext }

/// <summary>
/// The registration outcome <c>fsm.register_chart</c> reports, before the stored fingerprint
/// joins it. A tag union rather than <see cref="T:ByzantineSystems.Automata.Storage.ChartRegistration" />
/// itself, because the payload of <c>Mismatched</c> is a column's value, not something this side
/// encodes.
/// </summary>
type private RegistrationTag =
    | Registered
    | Matched
    | Mismatched

/// <summary>
/// The PostgreSQL record of which chart structure each declared version belongs to.
///
/// Registration is the first thing a process should do for a machine and the last chance to
/// notice that a chart was edited without bumping its version. After that, commands pin the
/// version and a foreign key holds them to one that exists.
/// </summary>
type PostgresChartRegistry(options: ChartRegistryOptions) =

    let context = options.Context

    let register = StatementKey.create "chart" "register" |> Sql.prepareProgram

    let byVersion = StatementKey.create "chart" "by_version" |> Sql.prepareProgram

    let registrationTag: EnumCodec<RegistrationTag> =
        EnumCodec.create
            "chart registration outcome"
            [ RegistrationTag.Registered
              RegistrationTag.Matched
              RegistrationTag.Mismatched ]
            (function
             | RegistrationTag.Registered -> "registered"
             | RegistrationTag.Matched -> "matched"
             | RegistrationTag.Mismatched -> "mismatched")

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
            let registrationRow reader =
                result {
                    let! tag = EnumCodec.read Row.string registrationTag "registration" reader

                    match tag with
                    | RegistrationTag.Registered -> return ChartRegistration.Registered
                    | RegistrationTag.Matched -> return ChartRegistration.Matched
                    | RegistrationTag.Mismatched ->
                        let! stored = parse (Row.string reader "stored_fingerprint")
                        return ChartRegistration.Mismatched stored
                }

            Sql.runProgram
                context
                (StoreProgram.queryOne
                    register
                    (key identity.MachineId identity.Version
                     @ [ "fingerprint", Param.Text(ChartFingerprint.value identity.Fingerprint) ])
                    registrationRow)
                ct

        member _.TryGet(machineId, version, ct) =
            Sql.readProgram
                context
                (StoreProgram.tryQueryOne byVersion (key machineId version) (fun reader ->
                    parse (Row.string reader "fingerprint")))
                ct

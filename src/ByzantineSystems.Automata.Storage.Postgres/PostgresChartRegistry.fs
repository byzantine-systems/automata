namespace ByzantineSystems.Automata.Storage.Postgres

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql

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

    let dataSource = options.Context.DataSource

    /// Every statement goes through the context's pipeline, which is where transient driver
    /// failures are retried and classified. Bound once here so no call site can forget it.
    let protect work ct = Db.protect options.Context.Resilience work ct

    /// A stored fingerprint that no longer parses is a corrupt row, reported as the contract's
    /// serialization failure rather than as an exception out of the store. The shape constraint
    /// on the column should make this unreachable; if the two ever disagree, nothing else
    /// reports it.
    let parse (value: string) : Result<ChartFingerprint, StoreError> =
        // A string literal rather than nameof: the type has a private single case of the same
        // name, and nameof binds to the constructor, which is not accessible here.
        ChartFingerprint.tryCreate value
        |> Result.mapError (Db.decodeFailure "ChartFingerprint")

    interface IChartRegistry with

        member _.Register(identity, ct) =
            protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "chart" "register", conn)

                        cmd.Parameters.AddWithValue("machine_id", MachineId.value identity.MachineId)
                        |> ignore

                        cmd.Parameters.AddWithValue("version", ChartVersion.value identity.Version)
                        |> ignore

                        cmd.Parameters.AddWithValue("fingerprint", ChartFingerprint.value identity.Fingerprint)
                        |> ignore

                        use! reader = cmd.ExecuteReaderAsync token
                        let! hasRow = reader.ReadAsync token

                        if not hasRow then
                            return
                                Error(
                                    Db.decodeFailure
                                        (nameof ChartRegistration)
                                        "register_chart_version returned no row"
                                )
                        else
                            match Row.string reader "registration" with
                            | "registered" -> return Ok ChartRegistration.Registered
                            | "matched" -> return Ok ChartRegistration.Matched
                            | "mismatched" ->
                                return
                                    Row.string reader "stored_fingerprint"
                                    |> parse
                                    |> Result.map ChartRegistration.Mismatched
                            | other ->
                                return
                                    Error(
                                        Db.decodeFailure
                                            (nameof ChartRegistration)
                                            $"unknown chart registration outcome {other}"
                                    )
                    })
                ct

        member _.TryGet(machineId, version, ct) =
            protect
                (fun token ->
                    task {
                        use! conn = dataSource.OpenConnectionAsync(token).AsTask()
                        use cmd = new NpgsqlCommand(SqlResources.get "chart" "by_version", conn)

                        cmd.Parameters.AddWithValue("machine_id", MachineId.value machineId) |> ignore

                        cmd.Parameters.AddWithValue("version", ChartVersion.value version) |> ignore

                        use! reader = cmd.ExecuteReaderAsync token
                        let! hasRow = reader.ReadAsync token

                        if not hasRow then
                            return Ok None
                        else
                            return Row.string reader "fingerprint" |> parse |> Result.map Some
                    })
                ct

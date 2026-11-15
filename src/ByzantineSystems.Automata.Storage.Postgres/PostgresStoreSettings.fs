namespace ByzantineSystems.Automata.Storage.Postgres

open System
open Microsoft.Extensions.Options

/// <summary>
/// How a machine's work listener is placed. A mirror of <see cref="T:ByzantineSystems.Automata.Storage.Postgres.ListenerConnection" />
/// that configuration can hold, because configuration carries the NAME of a connection string
/// rather than the secret itself.
/// </summary>
type ListenerMode =
    /// <summary>Listen on the store's own pooled data source.</summary>
    | SameDataSource
    /// <summary>No listener; polling only.</summary>
    | Off
    /// <summary>Listen on a dedicated connection, by the name of its connection string.</summary>
    | Direct of connectionStringName: string

/// <summary>The listener settings exactly as configuration binds them.</summary>
type PostgresListenerSettings() =

    /// <summary><c>same-data-source</c> (the default), <c>off</c>, or <c>direct</c>.</summary>
    member val Mode = "same-data-source" with get, set

    /// <summary>The named connection string <c>direct</c> mode listens on. Required by that mode alone.</summary>
    member val ConnectionStringName = "" with get, set

/// <summary>The PostgreSQL store settings exactly as configuration binds them.</summary>
type PostgresStoreSettings() =

    /// <summary>
    /// The name of the connection string this installation uses, resolved from the host's
    /// configuration. A name, not a secret: the application's configuration owns the value.
    /// </summary>
    member val ConnectionStringName = "BS_AUTOMATA_CONN" with get, set

    /// <summary>The schema that owns this installation. Validated by <see cref="M:ByzantineSystems.Automata.Storage.Postgres.SchemaName.tryCreate" />.</summary>
    member val Schema = "fsm" with get, set

    /// <summary>snapshot (the default) or bitemporal. Store type and installed profile must match.</summary>
    member val Beliefs = "snapshot" with get, set

    /// <summary>Where the work listener listens.</summary>
    member val Listener = PostgresListenerSettings() with get, set

/// <summary>Resolution and validation for <see cref="T:ByzantineSystems.Automata.Storage.Postgres.PostgresStoreSettings" />.</summary>
[<RequireQualifiedAccess>]
module PostgresStoreSettings =

    /// <summary>
    /// Validates the settings and resolves them to their typed form. Reads nothing but the
    /// settings themselves: no connection is opened, and no configuration is consulted, so a
    /// host can run this at startup before anything expensive exists.
    /// </summary>
    let tryResolve (settings: PostgresStoreSettings) : Result<SchemaName * ListenerMode, string list> =
        let failures = ResizeArray<string>()

        if String.IsNullOrWhiteSpace settings.ConnectionStringName then
            failures.Add "ConnectionStringName must be non-empty."

        match BeliefModel.tryCreate settings.Beliefs with
        | Ok _ -> ()
        | Error reason -> failures.Add reason

        let schema =
            match SchemaName.tryCreate settings.Schema with
            | Ok schema -> Some schema
            | Error error ->
                failures.Add $"Schema is not a name this store may own: %A{error}"
                None

        let listener =
            match settings.Listener.Mode with
            | "same-data-source" -> Some ListenerMode.SameDataSource
            | "off" -> Some ListenerMode.Off
            | "direct" ->
                if String.IsNullOrWhiteSpace settings.Listener.ConnectionStringName then
                    failures.Add "Listener.ConnectionStringName is required when the listener mode is direct."
                    None
                else
                    Some(ListenerMode.Direct settings.Listener.ConnectionStringName)
            | other ->
                failures.Add $"Listener.Mode must be same-data-source, off or direct, not '{other}'."
                None

        match schema, listener with
        | Some schema, Some listener when failures.Count = 0 -> Ok(schema, listener)
        | _ -> Error(List.ofSeq failures)

/// <summary>
/// Startup validation for the PostgreSQL store settings, so a bad schema name or a listener
/// half-configured stops the host before a data source is built or a connection opened.
/// </summary>
type PostgresStoreSettingsValidator() =

    interface IValidateOptions<PostgresStoreSettings> with

        member _.Validate(_, settings) =
            match PostgresStoreSettings.tryResolve settings with
            | Ok _ -> ValidateOptionsResult.Success
            | Error failures -> ValidateOptionsResult.Fail failures

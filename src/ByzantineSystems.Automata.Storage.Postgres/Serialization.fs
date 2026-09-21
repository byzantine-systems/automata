namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Text.Json.Serialization
open Npgsql
open ByzantineSystems.Automata.Core

/// <summary>Serialization helpers: System.Text.Json codecs and store options.</summary>
[<RequireQualifiedAccess>]
module Serialization =

    let private jsonOptions () : JsonSerializerOptions =
        JsonFSharpOptions.Default().WithUnionAdjacentTag().ToJsonSerializerOptions()

    let private protect error work =
        try
            Ok(work ())
        with exceptionRaised ->
            Error(error exceptionRaised)

    /// <summary>Builds a codec for one value, serializing unions with adjacent tags.</summary>
    let systemTextJson<'T> () : Codec<'T> =
        let options = jsonOptions ()

        let encode (value: 'T) =
            protect (fun error -> CodecError.EncodeError(typeof<'T>.Name, error)) (fun () ->
                JsonSerializer.Serialize(value, options))

        let decode (json: string) =
            protect (fun error -> CodecError.DecodeError(typeof<'T>.Name, error)) (fun () ->
                JsonSerializer.Deserialize<'T>(json, options))
            |> Result.bind (fun value ->
                match box value with
                | Null ->
                    JsonException $"JSON deserialized to null for {typeof<'T>.FullName}."
                    |> fun error -> Error(CodecError.DecodeError(typeof<'T>.Name, error))
                | NonNull _ -> Ok value)

        Codec.create encode decode

    /// <summary>Builds a codec for a list of values.</summary>
    let systemTextJsonList<'T> () : Codec<'T list> = systemTextJson<'T list> ()

/// <summary>
/// Options for a PostgreSQL-backed store: connection, codecs, and the key/state
/// projections that bridge the generic domain types to text columns.
/// </summary>
type StoreOptions<'EntityId, 'State, 'Event, 'Action> =
    {
        DataSource: NpgsqlDataSource
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        ActionCodec: Codec<'Action>
        ActionListCodec: Codec<'Action list>
        EntityIdEncode: 'EntityId -> string
        EntityIdDecode: string -> 'EntityId
        /// <summary>Root-to-leaf state ids for the current state, feeding the state_path GIN index.</summary>
        StatePath: 'State -> string[]
        TimeProvider: TimeProvider
    }

/// <summary>Builds a state-path projection (root-to-leaf state ids) from a chart.</summary>
[<RequireQualifiedAccess>]
module StatePath =

    let ofChart (chart: Chart<'State, 'Event, 'Action, 'Err>) : ('State -> string[]) =
        fun state ->
            let leaf = Chart.classifyOf chart state

            let parentOf (id: StateId) =
                Chart.tryNode chart id |> Option.bind (fun node -> node.Parent)

            Hierarchy.chain parentOf leaf
            |> List.rev
            |> List.map StateId.value
            |> List.toArray

/// <summary>Entity-key projections for the common <see cref="T:ByzantineSystems.Automata.Core.EntityId`1" /> case.</summary>
[<RequireQualifiedAccess>]
module EntityKey =

    let forEntityId<'entity> : (EntityId<'entity> -> string) * (string -> EntityId<'entity>) =
        EntityId.value, EntityId.create

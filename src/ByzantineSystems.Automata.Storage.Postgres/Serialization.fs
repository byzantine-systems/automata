namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Text.Json.Serialization
open Npgsql
open ByzantineSystems.Automata.Core
open FsToolkit.ErrorHandling

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
    /// Derives a list codec from an element codec by composing raw JSON.
    ///
    /// The two have to agree element for element, and not by coincidence. A commit writes the
    /// whole action list into <c>fsm.transition</c> as one document and, in the same statement,
    /// sends each element of that document to the queue; the dispatcher then decodes an element
    /// with the element codec. Building the list codec out of the element codec is what makes
    /// that agreement structural rather than a convention two call sites both have to remember.
    /// </summary>
    let listOf (element: Codec<'T>) : Codec<'T list> =
        let typeName = typeof<'T list>.Name

        let encode (values: 'T list) =
            values
            |> List.traverseResultM element.Encode
            |> Result.map (fun encoded -> "[" + String.Join(",", encoded) + "]")

        let decode (json: string) =
            protect (fun error -> CodecError.DecodeError(typeName, error)) (fun () ->
                use document = JsonDocument.Parse json

                document.RootElement.EnumerateArray() |> Seq.map _.GetRawText() |> List.ofSeq)
            |> Result.bind (List.traverseResultM element.Decode)

        Codec.create encode decode

/// <summary>Entity-key projections for the common <see cref="T:ByzantineSystems.Automata.Core.EntityId`1" /> case.</summary>
[<RequireQualifiedAccess>]
module EntityKey =

    /// <summary>
    /// Decoding returns a result rather than raising. <c>EntityId.create</c> rejects a blank id
    /// with <c>invalidArg</c>, which is right for application code constructing an id and wrong
    /// for a store reading a row: a corrupt row must surface as
    /// <c>StoreError.Serialization</c>, not as an <c>ArgumentException</c> thrown out of a store
    /// method that promised a result.
    /// </summary>
    let tryDecode<'entity> (value: string) : Result<EntityId<'entity>, string> =
        if String.IsNullOrWhiteSpace value then
            Error "An entity id read from storage was empty."
        else
            Ok(EntityId.create value)

    /// <summary>The encode and decode pair for an entity keyed by <c>EntityId</c>.</summary>
    let forEntityId<'entity> : (EntityId<'entity> -> string) * (string -> Result<EntityId<'entity>, string>) =
        EntityId.value, tryDecode<'entity>

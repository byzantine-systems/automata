namespace ByzantineSystems.Automata.Core

/// <summary>
/// Expected codec failures. The exception travels for diagnostics; the cases classify the
/// direction and carry the type name involved.
/// </summary>
type CodecError =
    | EncodeError of typeName: string * exn
    | DecodeError of typeName: string * exn

/// <summary>
/// A pair of total-to-<see cref="T:Microsoft.FSharp.Core.FSharpResult`2" /> serializers behind
/// which every representation detail hides. This project defines only the abstraction so that
/// it stays dependency-free; the FSharp.SystemTextJson implementation lives in
/// ByzantineSystems.Automata.Storage.Postgres, and a consumer can substitute their own.
/// </summary>
type Codec<'T> =
    { Encode: 'T -> Result<string, CodecError>
      Decode: string -> Result<'T, CodecError> }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Core.Codec`1" />.</summary>
[<RequireQualifiedAccess>]
module Codec =

    /// <summary>Builds a codec from an encode and a decode function.</summary>
    let create (encode: 'T -> Result<string, CodecError>) (decode: string -> Result<'T, CodecError>) : Codec<'T> =
        { Encode = encode; Decode = decode }

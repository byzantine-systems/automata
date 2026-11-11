module ByzantineSystems.Automata.Storage.Tests.EnumCodecTests

open System
open Expecto
open Microsoft.FSharp.Reflection
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal

/// Texts no shipped codec accepts, so every one of them must read back as an error. Case is part
/// of it: schema enum texts are case-sensitive, and a differently-cased copy must not decode.
let private unknownTexts = [ ""; "  "; "nope"; "READY"; "0" ]

/// Everything the contract claims about one codec, checked exhaustively over its finite universe:
/// the case list covers exactly the declared union cases, both directions agree on every case,
/// each case keeps its own text, and no unknown text decodes.
let private expectBijective (codec: EnumCodec<'T>) =
    let declared (value: 'T) =
        let case, _ = FSharpValue.GetUnionFields(value, typeof<'T>)
        case.Name

    let listed = codec |> EnumCodec.values |> List.map declared |> List.sort

    let defined =
        FSharpType.GetUnionCases typeof<'T>
        |> Array.map (fun case -> case.Name)
        |> List.ofArray
        |> List.sort

    Expect.equal listed defined "the table lists every declared case once"

    let cases = EnumCodec.values codec

    for value in cases do
        Expect.equal
            (EnumCodec.encode codec value |> EnumCodec.decode codec)
            (Ok value)
            "encode then decode returns the case"

    Expect.equal
        (cases |> List.map (EnumCodec.encode codec) |> List.distinct |> List.length)
        (List.length cases)
        "each case keeps its own text"

    for text in unknownTexts do
        Expect.isTrue (Result.isError (EnumCodec.decode codec text)) $"unknown text '%s{text}' does not decode"

/// A row as the read combinator needs it: a column-name-to-text map, which is enough to prove the
/// combinator threads the provider's accessor through the codec.
let private textOf (row: Map<string, string>) (column: string) : string = row[column]

[<Tests>]
let tests =
    testList
        "enum codec"
        [ testCase "the command status codec is bijective"
          <| fun _ -> expectBijective EnumCodec.commandStatus

          testCase "the lease outcome codec is bijective"
          <| fun _ -> expectBijective EnumCodec.leaseOutcome

          testCase "the divergence codec is bijective"
          <| fun _ -> expectBijective EnumCodec.divergence

          testCase "the instance status codec is bijective"
          <| fun _ -> expectBijective EnumCodec.instanceStatus

          testCase "the supervision kind codec is bijective"
          <| fun _ -> expectBijective EnumCodec.supervisionKind

          testCase "the supervision strategy codec is bijective"
          <| fun _ -> expectBijective EnumCodec.supervisionStrategy

          testCase "supervision codecs preserve the v1 stored spellings"
          <| fun _ ->
              Expect.equal
                  (EnumCodec.encode EnumCodec.supervisionKind SupervisionAuditKind.Started)
                  "Started"
                  "existing audit rows use the union case spelling"

              Expect.equal
                  (EnumCodec.encode EnumCodec.supervisionStrategy SupervisionAuditStrategy.OneForOne)
                  "OneForOne"
                  "existing audit rows use the union case spelling"

          testCase "read decodes a column through the codec"
          <| fun _ ->
              let read = EnumCodec.read textOf EnumCodec.commandStatus "status"

              Expect.equal
                  (read (Map [ "status", "ready" ]))
                  (Ok CommandStatus.Ready)
                  "a coded text reads back as its case"

              Expect.isTrue (Result.isError (read (Map [ "status", "nope" ]))) "an unknown column text is an error"

          testCase "a table that is not a bijection is refused"
          <| fun _ ->
              let encode (status: CommandStatus) =
                  match status with
                  | CommandStatus.Ready -> "ready"
                  | other -> $"%A{other}"

              let refuses (build: unit -> unit) =
                  Expect.throwsC build (fun error ->
                      Expect.stringContains error.Message "codec" "the refusal names the table")

              // The same case twice, through a duplicated entry.
              refuses (fun () ->
                  EnumCodec.create "command status" [ CommandStatus.Ready; CommandStatus.Ready ] encode
                  |> ignore)

              // Two cases sharing one text.
              refuses (fun () ->
                  EnumCodec.create "command status" [ CommandStatus.Ready; CommandStatus.Leased ] (fun _ -> "ready")
                  |> ignore)

              // A blank name, an empty table, and a blank case text.
              refuses (fun () -> EnumCodec.create "" [ CommandStatus.Ready ] encode |> ignore)

              refuses (fun () -> EnumCodec.create "command status" [] encode |> ignore)

              refuses (fun () ->
                  EnumCodec.create "command status" [ CommandStatus.Ready ] (fun _ -> " ")
                  |> ignore) ]

module ByzantineSystems.Automata.Core.Tests.CoreTypeTests

open System
open ByzantineSystems.Automata.Core
open Expecto

/// Phantom marker so entity ids can be tied to a domain type in tests.
type Order = class end

let private throwsArgument action =
    try
        action ()
        false
    with :? ArgumentException ->
        true

let private identifierInputs raw =
    [ null; ""; " "; "\t\r\n"; raw; $"  %s{raw}  " ]

let private stateIdLaw raw =
    let valid = not (String.IsNullOrWhiteSpace raw)

    match StateId.tryCreate raw with
    | Ok id ->
        valid
        && StateId.value id = raw.Trim()
        && StateId.create raw = StateId.create (raw.Trim())
    | Error _ -> not valid && throwsArgument (fun () -> StateId.create raw |> ignore)

let private machineIdLaw raw =
    let valid = not (String.IsNullOrWhiteSpace raw)

    match MachineId.tryCreate raw with
    | Ok id ->
        valid
        && MachineId.value id = raw.Trim()
        && MachineId.create raw = MachineId.create (raw.Trim())
    | Error _ -> not valid && throwsArgument (fun () -> MachineId.create raw |> ignore)

let private entityIdLaw raw =
    let valid = not (String.IsNullOrWhiteSpace raw)

    if valid then
        let id: EntityId<Order> = EntityId.create raw
        EntityId.value id = raw.Trim() && id = EntityId.create (raw.Trim())
    else
        throwsArgument (fun () ->
            let _: EntityId<Order> = EntityId.create raw
            ())

let epochTests =
    testList
        "Epoch"
        [ Property.property "ofUInt64 and value are inverse observations" (fun (value: uint64) ->
              Epoch.value (Epoch.ofUInt64 value) = value && Epoch.initial = Epoch.ofUInt64 0UL)

          Property.property "next increments and orders every non-maximum epoch" (fun (seed: uint64) ->
              let generated = if seed = UInt64.MaxValue then 0UL else seed

              [ 0UL; UInt64.MaxValue - 1UL; generated ]
              |> List.forall (fun value ->
                  let epoch = Epoch.ofUInt64 value
                  let next = Epoch.next epoch
                  Epoch.value next = value + 1UL && next > epoch)) ]

let identifierTests =
    testList
        "identifiers"
        [ Property.property "StateId accepts exactly nonblank text and observes trimming" (fun (raw: string) ->
              identifierInputs raw |> List.forall stateIdLaw)

          Property.property "MachineId accepts exactly nonblank text and observes trimming" (fun (raw: string) ->
              identifierInputs raw |> List.forall machineIdLaw)

          Property.property "EntityId accepts exactly nonblank text and observes trimming" (fun (raw: string) ->
              identifierInputs raw |> List.forall entityIdLaw)

          Property.property
              "identifier equality is equality of normalized observations"
              (fun (left: string, right: string) ->
                  if String.IsNullOrWhiteSpace left || String.IsNullOrWhiteSpace right then
                      true
                  else
                      let leftId = StateId.create left
                      let rightId = StateId.create right
                      (leftId = rightId) = (left.Trim() = right.Trim())) ]

let codecTests =
    testList
        "Codec"
        [ let intCodec: Codec<int> =
              Codec.create (fun i -> Ok(string i)) (fun s ->
                  match Int32.TryParse s with
                  | true, v -> Ok v
                  | false, _ -> Error(CodecError.DecodeError("Int32", ArgumentException($"not an int: {s}"))))

          Property.property "decode after encode is identity" (fun (value: int) ->
              Result.bind intCodec.Decode (intCodec.Encode value) = Ok value)

          test "decode failure is typed" {
              match intCodec.Decode "oops" with
              | Error(DecodeError(typeName, _)) -> Expect.equal "Int32" typeName "error carries the type name"
              | Error(EncodeError _) -> failtest "expected DecodeError, got EncodeError"
              | Ok _ -> failtest "expected failure, got success"
          }

          test "encode failure is typed" {
              let failing =
                  Codec.create
                      (fun (_: int) -> Error(CodecError.EncodeError("Int32", InvalidOperationException "nope")))
                      (fun s ->
                          match Int32.TryParse s with
                          | true, v -> Ok v
                          | false, _ -> Error(CodecError.DecodeError("Int32", ArgumentException s)))

              match failing.Encode 1 with
              | Error(EncodeError(typeName, _)) -> Expect.equal "Int32" typeName "error carries the type name"
              | Error(DecodeError _) -> failtest "expected EncodeError, got DecodeError"
              | Ok _ -> failtest "expected failure, got success"
          } ]

let tests = testList "core types" [ epochTests; identifierTests; codecTests ]

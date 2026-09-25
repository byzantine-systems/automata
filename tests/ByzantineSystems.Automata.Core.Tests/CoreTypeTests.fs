module ByzantineSystems.Automata.Core.Tests.CoreTypeTests

open System
open ByzantineSystems.Automata.Core
open Expecto

/// Phantom marker so entity ids can be tied to a domain type in tests.
type Order = class end

let epochTests =
    testList
        "Epoch"
        [ test "initial is zero" { Expect.equal 0UL (Epoch.value Epoch.initial) "a new instance starts at epoch 0" }
          test "next increments by one" {
              Expect.equal 1UL (Epoch.value (Epoch.next Epoch.initial)) "next advances the counter"
          }
          test "orders structurally" {
              Expect.isLessThan (Epoch.initial) (Epoch.next (Epoch.next Epoch.initial)) "epochs compare"
          }
          test "ofUInt64/value roundtrip" { Expect.equal 42UL (Epoch.value (Epoch.ofUInt64 42UL)) "storage roundtrip" } ]

let identifierTests =
    testList
        "identifiers"
        [ test "stateId roundtrips" {
              Expect.equal "active.processing" (StateId.value (stateId "active.processing")) "value survives"
          }
          test "stateId trims surrounding whitespace" {
              Expect.equal "idle" (StateId.value (StateId.create " idle ")) "whitespace is trimmed"
          }
          test "stateId rejects empty strings" {
              Expect.throws (fun _ -> StateId.create "" |> ignore) "empty input raises ArgumentException"
          }
          test "stateId rejects whitespace-only strings" {
              Expect.throws (fun _ -> StateId.create "   " |> ignore) "whitespace-only input raises"
          }
          test "machineId roundtrips" {
              Expect.equal "payments" (MachineId.value (machineId "payments")) "value survives"
          }
          test "entityId roundtrips with phantom type" {
              let orderId: EntityId<Order> = entityId "ORD-1"
              Expect.equal "ORD-1" (EntityId.value orderId) "value survives"
          }
          test "equal ids are equal" {
              Expect.isTrue ((stateId "a") = (stateId "a")) "same value, same id"
              Expect.isFalse ((stateId "a") = (stateId "b")) "different value, different id"
          } ]

let codecTests =
    testList
        "Codec"
        [ let intCodec: Codec<int> =
              Codec.create (fun i -> Ok(string i)) (fun s ->
                  match Int32.TryParse s with
                  | true, v -> Ok v
                  | false, _ -> Error(CodecError.DecodeError("Int32", ArgumentException($"not an int: {s}"))))

          test "roundtrips" {
              Expect.equal (Ok 42) (Result.bind intCodec.Decode (intCodec.Encode 42)) "encode then decode"
          }

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

let transitionTests =
    testList
        "transition"
        [ test "snapshot carries state, epoch, and status" {
              let snapshot: Snapshot<string> =
                  { State = "idle"
                    Epoch = Epoch.initial
                    Status = Running }

              Expect.equal ("idle", 0UL, Running) (snapshot.State, Epoch.value snapshot.Epoch, snapshot.Status) ""
          }
          // A committed transition and a commit receipt are no longer declared here. What the
          // chart decides is a TransitionDraft and what the database answers with is a
          // CommittedTransition, both in Storage, because everything that distinguishes them is
          // a value only the database can supply. Their tests live with them.
          test "epoch advances by one and starts at zero" {
              Expect.equal 0UL (Epoch.value Epoch.initial) "an entity that has never committed"
              Expect.equal 1UL (Epoch.value (Epoch.next Epoch.initial)) "the first commit"
          } ]

let errorTests =
    testList
        "errors"
        [ test "machine error exposes concurrency detail" {
              match MachineError.Store(Concurrency(Epoch.initial, Epoch.next Epoch.initial)) with
              | Store(Concurrency(expected, actual)) ->
                  Expect.equal (0UL, 1UL) (Epoch.value expected, Epoch.value actual) "expected and actual epochs"
              | Store _
              | Transition _
              | Timeout _
              | CircuitOpen _
              | MachineError.Rejected _ -> failtest "unexpected case"
          }
          test "transition error carries unhandled detail" {
              match TransitionError<string>.Unhandled(stateId "idle", "Cancel") with
              | Unhandled(state, event) ->
                  Expect.equal ("idle", "Cancel") (StateId.value state, event) "state and event names"
              | TransitionError.Rejected _
              | GuardFailed _
              | UnknownState _
              | TargetMismatch _ -> failtest "unexpected case"
          }
          test "machine lifecycle rejection is structured" {
              match MachineError<string>.Rejected(MachineRejection.InstanceNotRunning InstanceStatus.Terminated) with
              | MachineError.Rejected(MachineRejection.InstanceNotRunning InstanceStatus.Terminated) -> ()
              | _ -> failtest "expected a typed terminated-instance rejection"
          } ]

let tests =
    testList "core types" [ epochTests; identifierTests; codecTests; transitionTests; errorTests ]

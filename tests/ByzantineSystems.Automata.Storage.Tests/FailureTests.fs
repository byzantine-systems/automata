module ByzantineSystems.Automata.Storage.Tests.FailureTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Expecto

let private codec = Codec.create Ok Ok

let private row encoded =
    let f = FailureEncoding.fields encoded

    { Kind = Some f.Kind
      Domain = if f.Kind = "domain" then Some f.Domain else None
      Machine = if f.Kind = "machine" then Some f.Reason else None
      Replay =
        if f.Kind = "replay" then
            Some(f.ReplayKind, f.Version, f.Epoch, f.Reason, f.Budget)
        else
            None }

let tests =
    testList
        "relational failures"
        [ Property.property
              "every constructor and payload survives the column projection"
              (fun (payload: string, version: byte, epoch: uint32, budget: int) ->
                  let payload = if isNull payload then "" else payload

                  let cases =
                      [ CommandFailure.Domain payload
                        CommandFailure.Machine payload
                        CommandFailure.Replay(ReplayError.UnknownChartVersion(ChartVersion.create (1 + int version)))
                        CommandFailure.Replay(ReplayError.Diverged(Epoch.ofUInt64 (uint64 epoch), payload))
                        CommandFailure.Replay(ReplayError.BudgetExceeded budget)
                        CommandFailure.Replay ReplayError.HistoryPurged ]

                  cases
                  |> List.forall (fun failure ->
                      FailureEncoding.encode codec failure
                      |> Result.bind (row >> FailureEncoding.decode codec) = Ok failure))
          Property.property "missing and incompatible children never decode" (fun (text: string) ->
              let text = if isNull text then "" else text

              let missing =
                  { Kind = Some "domain"
                    Domain = None
                    Machine = None
                    Replay = None }

              let extra =
                  { missing with
                      Domain = Some text
                      Machine = Some text }

              Result.isError (FailureEncoding.decode codec missing)
              && Result.isError (FailureEncoding.decode codec extra))
          testCase "library failures never invoke the application codec" (fun () ->
              let failCodec: Codec<string> =
                  Codec.create (fun _ -> failtest "encode called") (fun _ -> failtest "decode called")

              for failure in [ CommandFailure.Machine ""; CommandFailure.Replay ReplayError.HistoryPurged ] do
                  let actual =
                      FailureEncoding.encode failCodec failure
                      |> Result.bind (row >> FailureEncoding.decode failCodec)

                  Expect.equal actual (Ok failure) "the library owns these fields") ]

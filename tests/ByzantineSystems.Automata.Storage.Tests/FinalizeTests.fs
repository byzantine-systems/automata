module ByzantineSystems.Automata.Storage.Tests.FinalizeTests

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Expecto

let tests =
    testList
        "draft encoding"
        [ Property.property
              "only initial and correction origins invoke the source codec"
              (fun (source: int, destination: int) ->
                  let calls = ResizeArray<int>()

                  let states =
                      Codec.create
                          (fun value ->
                              calls.Add value
                              Ok(string value))
                          (fun _ -> Ok 0)

                  let actions: Codec<int list> = Codec.create (fun _ -> Ok "[]") (fun _ -> Ok [])

                  let draft: TransitionDraft<string, int, int, int> =
                      { MachineId = machineId "encoding"
                        EntityId = "e"
                        Event = 0
                        Actions = []
                        FromState = source
                        ToState = destination
                        HandledBy = stateId "root"
                        Exited = []
                        Entered = []
                        Status = Running
                        EffectiveAt = System.DateTimeOffset.UnixEpoch }

                  let check expected expectedOrigin expectedCalls =
                      calls.Clear()

                      match DraftEncoding.encode states actions expected draft with
                      | Ok encoded -> encoded.Origin = expectedOrigin && List.ofSeq calls = expectedCalls
                      | Error _ -> false

                  check (Some Epoch.initial) (CommitOrigin.Initial(string source)) [ source; destination ]
                  && check (Some(Epoch.next Epoch.initial)) CommitOrigin.Continuing [ destination ]
                  && check None (CommitOrigin.Initial(string source)) [ source; destination ]) ]

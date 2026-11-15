module ByzantineSystems.Automata.Storage.Tests.TapShapeTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Storage
open Expecto

let private contracts =
    [ typeof<ICommandInbox<obj, obj>>,
      set
          [ "SubmitAsync"
            "ClaimAsync"
            "RescheduleAsync"
            "ExtendLeaseAsync"
            "TryGetAsync"
            "TryFindAsync" ]
      typeof<IStateReader<obj, obj, obj, obj>>, set [ "TryGetSnapshotAsync"; "HistoryAsync" ]
      typeof<ICommandProcessorStore<obj, obj, obj, obj, obj>>,
      set [ "CommitAsync"; "RejectAsync"; "DeadLetterAsync"; "TryGetResultAsync" ]
      typeof<IActionQueue<obj, obj>>, set [ "ClaimAsync"; "CompleteAsync"; "RescheduleAsync"; "AbandonAsync" ]
      typeof<ITemporalReader<obj, obj>>, set [ "ValidAtAsync"; "AsOfAsync" ]
      typeof<ICorrectionStore<obj, obj>>, set [ "CorrectAsync" ]
      typeof<ITransitionLog<obj, obj, obj, obj>>, set [ "SuffixAsync" ]
      typeof<IPriorState<obj, obj>>, set [ "BeforeAsync" ]
      typeof<ICorrectionCommitter<obj, obj, obj, obj>>, set [ "CommitCorrectionAsync" ]
      typeof<IStoreBoot>, set [ "BootAsync" ]
      typeof<IWorkNotifications>, set [ "ListenAsync" ]
      typeof<IDatabaseMaintenance>,
      set
          [ "NotifyPendingAsync"
            "RunAsync"
            "DetectDriftAsync"
            "RepairDriftAsync"
            "ScheduleAsync"
            "UnscheduleAsync" ]
      typeof<IChartRegistry>, set [ "RegisterAsync"; "TryGetAsync" ]
      typeof<ISupervisionEventStore>, set [ "RecordAsync" ] ]

let private taskMethods =
    contracts
    |> List.collect (fun (contract, _) -> contract.GetMethods() |> Array.toList)
    |> List.filter (fun methodInfo -> typeof<Task>.IsAssignableFrom methodInfo.ReturnType)

let tests =
    testList
        "TAP surface"
        [ testCase "public task members are the exact TAP contract" (fun () ->
              for contract, expected in contracts do
                  let actual =
                      contract.GetMethods()
                      |> Array.filter (fun methodInfo -> typeof<Task>.IsAssignableFrom methodInfo.ReturnType)
                      |> Array.map _.Name
                      |> Set.ofArray

                  Expect.equal actual expected $"{contract.Name} exposes exactly its documented asynchronous members"

              Expect.equal taskMethods.Length 33 "the complete TAP surface is guarded")

          testCase "cancellation tokens are last and named cancellationToken" (fun () ->
              let invalid =
                  taskMethods
                  |> List.choose (fun methodInfo ->
                      let parameters = methodInfo.GetParameters()

                      let tokens =
                          parameters
                          |> Array.indexed
                          |> Array.filter (fun (_, parameter) -> parameter.ParameterType = typeof<CancellationToken>)

                      match tokens with
                      | [| index, parameter |] when
                          index = parameters.Length - 1 && parameter.Name = "cancellationToken"
                          ->
                          None
                      | _ -> Some $"{methodInfo.DeclaringType.Name}.{methodInfo.Name}")

              Expect.isEmpty invalid "the token is the final, consistently named parameter") ]

module ByzantineSystems.Automata.Storage.Tests.StoreProgramTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Expecto
open FsCheck
open FsToolkit.ErrorHandling

type private Instruction =
    | Execute of StatementKey
    | QueryOne of StatementKey
    | TryQueryOne of StatementKey
    | QueryRows of StatementKey

type private ProgramStep =
    | ExecuteStep
    | QueryStep

let private canceled<'T> (cancellationToken: CancellationToken) =
    Task.FromCanceled<Result<'T, StoreError>>(cancellationToken)

let private failed<'T> (failureAt: StatementKey option) (key: StatementKey) : Task<Result<'T, StoreError>> option =
    match failureAt with
    | Some failedKey when failedKey = key ->
        Some(Task.FromResult(Error(StoreError.Unexpected(InvalidOperationException(StatementKey.value key)))))
    | _ -> None

type private RecordingInterpreter
    (
        rows: Map<string, string> list,
        ?failureAt: StatementKey,
        ?changedRows: int,
        ?queryBatches: Map<string, string> list list
    ) =
    let instructions = ResizeArray<Instruction>()
    let mutable remaining = rows
    let mutable remainingBatches = defaultArg queryBatches [ rows ]

    member _.Instructions = instructions |> Seq.toList

    interface IStoreInterpreter<Map<string, string>, Map<string, string>> with
        member _.ExecuteAsync(key, _, cancellationToken) =
            instructions.Add(Execute key)

            if cancellationToken.IsCancellationRequested then
                canceled cancellationToken
            else
                failed<int> failureAt key
                |> Option.defaultWith (fun () -> Task.FromResult(Ok(defaultArg changedRows 1)))

        member _.QueryOneAsync<'T>(key, _, read, cancellationToken) =
            instructions.Add(QueryOne key)

            if cancellationToken.IsCancellationRequested then
                canceled cancellationToken
            else
                match failed<'T> failureAt key with
                | Some failure -> failure
                | None ->
                    match remaining with
                    | row :: rest ->
                        remaining <- rest
                        Task.FromResult(read row)
                    | [] ->
                        Task.FromResult(
                            Error(
                                StoreError.Serialization(
                                    nameof Map<string, string>,
                                    InvalidOperationException("no row")
                                )
                            )
                        )

        member _.TryQueryOneAsync<'T>(key, _, read, cancellationToken) =
            instructions.Add(TryQueryOne key)

            if cancellationToken.IsCancellationRequested then
                canceled cancellationToken
            else
                match failed<'T option> failureAt key with
                | Some failure -> failure
                | None ->
                    match remaining with
                    | row :: rest ->
                        remaining <- rest
                        Task.FromResult(read row |> Result.map Some)
                    | [] -> Task.FromResult(Ok None)

        member _.QueryRowsAsync<'T>(key, _, read, cancellationToken) =
            instructions.Add(QueryRows key)

            if cancellationToken.IsCancellationRequested then
                canceled cancellationToken
            else
                match failed<'T list> failureAt key with
                | Some failure -> failure
                | None ->
                    let batch =
                        match remainingBatches with
                        | batch :: rest ->
                            remainingBatches <- rest
                            batch
                        | [] -> []

                    let result =
                        List.foldBack
                            (fun row values -> Result.map2 (fun head tail -> head :: tail) (read row) values)
                            batch
                            (Ok [])

                    remaining <- []
                    Task.FromResult result

let private parameters = Map.empty<string, string>
let private first = StatementKey.create "command" "first"
let private second = StatementKey.create "command" "second"
let private third = StatementKey.create "command" "third"

let private readValue row =
    row
    |> Map.tryFind "value"
    |> Result.requireSome (StoreError.Serialization("value", InvalidOperationException("missing")))

let private readLeaseOutcome row =
    match Map.tryFind "outcome" row with
    | Some "updated" -> Ok LeaseUpdateOutcome.Updated
    | Some "lease_lost" -> Ok LeaseUpdateOutcome.LeaseLost
    | value -> Error(StoreError.Serialization("outcome", InvalidOperationException($"%A{value}")))

let private readCommandId row =
    readValue row |> Result.map (int64 >> CommandId.ofInt64)

let private keyAt index = StatementKey.create "generated" $"step-%d{index}"

let private expectedInstruction index = function
    | ExecuteStep -> Execute(keyAt index)
    | QueryStep -> QueryOne(keyAt index)

let rec private programFrom outcome = function
    | [] -> StoreProgram.ok outcome
    | (index, step) :: rest ->
        store {
            match step with
            | ExecuteStep -> do! StoreProgram.executeDiscard (keyAt index) parameters
            | QueryStep ->
                let! _ = StoreProgram.queryOne (keyAt index) parameters readValue
                ()

            return! programFrom outcome rest
        }

let private rowsFor steps =
    steps
    |> List.choose (function
        | QueryStep -> Some(Map [ "value", "read" ])
        | ExecuteStep -> None)

let private run interpreter program =
    StoreProgram.run interpreter CancellationToken.None program
    |> fun task -> task.GetAwaiter().GetResult()

let tests =
    testList
        "store program"
        [ Property.property "instructions are cold and execute sequentially" (fun (steps: ProgramStep list, outcome: int) ->
              let indexed = List.indexed steps
              let interpreter = RecordingInterpreter(rowsFor steps)
              let program = programFrom outcome indexed
              let cold = List.isEmpty interpreter.Instructions
              let result = run interpreter program

              cold
              && result = Ok outcome
              && interpreter.Instructions = (indexed |> List.map (fun (index, step) -> expectedInstruction index step)))

          Property.property "a typed error stops the remaining program" (fun (generated: ProgramStep list, position: uint32) ->
              let steps =
                  match generated with
                  | [] -> [ ExecuteStep ]
                  | values -> values

              let indexed = List.indexed steps
              let failureIndex = int (position % uint32 steps.Length)
              let failureKey = keyAt failureIndex
              let interpreter = RecordingInterpreter(rowsFor steps, failureAt = failureKey)
              let result = programFrom () indexed |> run interpreter
              let expected = indexed |> List.take (failureIndex + 1) |> List.map (fun (index, step) -> expectedInstruction index step)

              match result with
              | Error(StoreError.Unexpected error) ->
                  error.Message = StatementKey.value failureKey && interpreter.Instructions = expected
              | _ -> false)

          testTask "row decode errors stop the remaining program" {
              let interpreter = RecordingInterpreter([ Map.empty ])

              let program =
                  store {
                      let! _ = StoreProgram.queryOne first parameters readValue
                      return! StoreProgram.executeDiscard second parameters
                  }

              let! result = StoreProgram.run interpreter CancellationToken.None program

              Expect.isError result "a reader error is a program error"
              Expect.equal interpreter.Instructions [ QueryOne first ] "nothing runs after decode failure"
          }

          testTask "cancellation flows to the interpreter" {
              let cancellation = new CancellationTokenSource()
              cancellation.Cancel()
              let interpreter = RecordingInterpreter([])
              let program = StoreProgram.executeDiscard first parameters

              let running = StoreProgram.run interpreter cancellation.Token program

              let! canceled =
                  task {
                      try
                          let! _ = running
                          return false
                      with :? OperationCanceledException ->
                          return true
                  }

              Expect.isTrue canceled "the program remains canceled"
              Expect.equal interpreter.Instructions [ Execute first ] "the instruction observes caller cancellation"
          }

          testTask "lease updates normalize affected-row evidence" {
              let updated = RecordingInterpreter([], changedRows = 1)
              let lost = RecordingInterpreter([], changedRows = 0)
              let program = CommandInboxProgram.leaseUpdate (ChangedRows(first, parameters))

              let! updatedResult = StoreProgram.run updated CancellationToken.None program
              let! lostResult = StoreProgram.run lost CancellationToken.None program

              Expect.equal updatedResult (Ok LeaseUpdateOutcome.Updated) "one changed row retains the lease"
              Expect.equal lostResult (Ok LeaseUpdateOutcome.LeaseLost) "no changed row loses the lease"
          }

          testTask "lease updates preserve returned outcome evidence" {
              let interpreter = RecordingInterpreter([ Map [ "outcome", "lease_lost" ] ])

              let program =
                  CommandInboxProgram.leaseUpdate (ReturnedOutcome(first, parameters, readLeaseOutcome))

              let! result = StoreProgram.run interpreter CancellationToken.None program

              Expect.equal result (Ok LeaseUpdateOutcome.LeaseLost) "the returned outcome is authoritative"
              Expect.equal interpreter.Instructions [ QueryOne first ] "returned evidence uses one query"
          }

          testTask "existing submissions short circuit checked writes" {
              let commandId = CommandId.ofInt64 17L
              let interpreter = RecordingInterpreter([])

              let program =
                  CheckedSubmission(
                      StoreProgram.ok (Some commandId),
                      StoreProgram.fail (StoreError.NotFound "should not check registration"),
                      StoreError.NotFound "unregistered",
                      StoreProgram.fail (StoreError.NotFound "should not insert"),
                      None
                  )
                  |> CommandInboxProgram.submit

              let! result = StoreProgram.run interpreter CancellationToken.None program

              Expect.equal result (Ok(AlreadySubmitted commandId)) "the original command wins"
              Expect.isEmpty interpreter.Instructions "no write runs for an existing key"
          }

          testTask "new correction submissions run checked steps in order" {
              let commandId = CommandId.ofInt64 23L
              let interpreter = RecordingInterpreter([ Map [ "value", "23" ] ])

              let registered =
                  StoreProgram.executeCount first parameters |> StoreProgram.map ((=) 1)

              let insert = StoreProgram.queryOne second parameters readCommandId

              let correction _ =
                  StoreProgram.executeDiscard third parameters

              let program =
                  CheckedSubmission(
                      StoreProgram.ok None,
                      registered,
                      StoreError.NotFound "unregistered",
                      insert,
                      Some correction
                  )
                  |> CommandInboxProgram.submit

              let! result = StoreProgram.run interpreter CancellationToken.None program

              Expect.equal result (Ok(Accepted commandId)) "the inserted correction is accepted"

              Expect.equal
                  interpreter.Instructions
                  [ Execute first; QueryOne second; Execute third ]
                  "registration, insert, and correction detail writes stay sequential"
          } ]

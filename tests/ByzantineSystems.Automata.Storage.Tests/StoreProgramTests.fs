module ByzantineSystems.Automata.Storage.Tests.StoreProgramTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open Expecto
open FsToolkit.ErrorHandling

type private Instruction =
    | Execute of StatementKey
    | QueryOne of StatementKey
    | TryQueryOne of StatementKey
    | QueryRows of StatementKey

let private canceled<'T> (cancellationToken: CancellationToken) =
    Task.FromCanceled<Result<'T, StoreError>>(cancellationToken)

let private failed<'T> (failureAt: StatementKey option) (key: StatementKey) : Task<Result<'T, StoreError>> option =
    match failureAt with
    | Some failedKey when failedKey = key ->
        Some(Task.FromResult(Error(StoreError.Unexpected(InvalidOperationException(StatementKey.value key)))))
    | _ -> None

type private RecordingInterpreter(rows: Map<string, string> list, ?failureAt: StatementKey, ?changedRows: int) =
    let instructions = ResizeArray<Instruction>()
    let mutable remaining = rows

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
                    let result =
                        List.foldBack
                            (fun row values -> Result.map2 (fun head tail -> head :: tail) (read row) values)
                            remaining
                            (Ok [])

                    remaining <- []
                    Task.FromResult result

let private parameters = Map.empty<string, string>
let private first = StatementKey.create "command" "first"
let private second = StatementKey.create "command" "second"

let private readValue row =
    row
    |> Map.tryFind "value"
    |> Result.requireSome (StoreError.Serialization("value", InvalidOperationException("missing")))

let private readLeaseOutcome row =
    match Map.tryFind "outcome" row with
    | Some "updated" -> Ok LeaseUpdateOutcome.Updated
    | Some "lease_lost" -> Ok LeaseUpdateOutcome.LeaseLost
    | value -> Error(StoreError.Serialization("outcome", InvalidOperationException($"%A{value}")))

let tests =
    testList
        "store program"
        [ testTask "instructions are cold and execute sequentially" {
              let interpreter = RecordingInterpreter([ Map [ "value", "read" ] ])

              let program =
                  store {
                      do! StoreProgram.executeDiscard first parameters
                      let! value = StoreProgram.queryOne second parameters readValue
                      return value
                  }

              Expect.isEmpty interpreter.Instructions "constructing a program performs no work"

              let! result = StoreProgram.run interpreter CancellationToken.None program

              Expect.equal result (Ok "read") "the query result remains typed"
              Expect.equal interpreter.Instructions [ Execute first; QueryOne second ] "instructions retain order"
          }

          testTask "a typed error stops the remaining program" {
              let interpreter = RecordingInterpreter([], failureAt = first)

              let program =
                  store {
                      do! StoreProgram.executeDiscard first parameters
                      return! StoreProgram.executeDiscard second parameters
                  }

              let! result = StoreProgram.run interpreter CancellationToken.None program

              match result with
              | Error(StoreError.Unexpected error) ->
                  Expect.equal error.Message (StatementKey.value first) "the interpreter owns error translation"
              | other -> failtestf "expected an interpreter error, got %A" other

              Expect.equal interpreter.Instructions [ Execute first ] "nothing runs after an error"
          }

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
          } ]

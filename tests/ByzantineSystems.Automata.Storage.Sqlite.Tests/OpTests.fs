module ByzantineSystems.Automata.Storage.Sqlite.Tests.OpTests

// The effect lives in the Storage library, internal to it and the bundled providers. Nothing
// below touches a database: the session is a counter that each step bumps, which is enough to
// see what ran and how often.

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Internal
open Expecto

type private Counter() =
    member val Steps = 0 with get, set

let private unit = OpBuilder<Counter>()

/// A step that records it ran and answers with its value.
let private step (value: 'T) : Op<Counter, 'T> =
    Op.ofDriver (fun counter _ ->
        counter.Steps <- counter.Steps + 1
        Task.FromResult(Ok value))

let private refusal = StoreError.NotFound "nothing"

let private runOn (counter: Counter) (op: Op<Counter, 'T>) =
    Op.run op counter CancellationToken.None

let tests =
    testList
        "Op"
        [ testTask "an error stops the unit where it happens" {
              let counter = Counter()

              let! outcome =
                  unit {
                      let! first = step 1
                      let! (_: int) = Op.fail refusal
                      let! second = step 2
                      return first + second
                  }
                  |> runOn counter

              Expect.equal outcome (Error refusal) "the error is the answer"
              Expect.equal counter.Steps 1 "nothing after the error ran"
          }

          testTask "a unit is cold, and runs again each time it is started" {
              let counter = Counter()

              let twice =
                  unit {
                      let! value = step 1
                      return value
                  }

              Expect.equal counter.Steps 0 "building it ran nothing"
              let! _ = runOn counter twice
              let! _ = runOn counter twice
              Expect.equal counter.Steps 2 "each start ran the body once"
          }

          testTask "let! binds a plain result, and its error stops the unit" {
              let counter = Counter()

              let! outcome =
                  unit {
                      let! value = Ok 41
                      let! _ = (Error refusal: Result<int, StoreError>)
                      return! step (value + 1)
                  }
                  |> runOn counter

              Expect.equal outcome (Error refusal) "the result's error is the answer"
              Expect.equal counter.Steps 0 "the step after it never ran"
          }

          testTask "return! forwards a unit and a plain result alike" {
              let counter = Counter()
              let! fromUnit = unit { return! step 7 } |> runOn counter
              let! fromResult = unit { return! (Ok 8: Result<int, StoreError>) } |> runOn counter
              Expect.equal fromUnit (Ok 7) "a unit"
              Expect.equal fromResult (Ok 8) "a result"
          }

          testTask "a conditional step without an else runs only when asked" {
              let counter = Counter()

              let conditional (wanted: bool) =
                  unit {
                      if wanted then
                          do! step () |> Op.discard

                      return! step "done"
                  }

              let! skipped = conditional false |> runOn counter
              Expect.equal skipped (Ok "done") "the rest still runs"
              Expect.equal counter.Steps 1 "only the final step ran"

              let! taken = conditional true |> runOn counter
              Expect.equal taken (Ok "done") "and with the step"
              Expect.equal counter.Steps 3 "both steps ran"
          }

          testTask "cancellation stays cancellation" {
              let source = new CancellationTokenSource()
              source.Cancel()

              let cancelled: Op<Counter, unit> =
                  Op.ofDriver (fun _ ct ->
                      task {
                          do! Task.Delay(Timeout.Infinite, ct)
                          return Ok()
                      })

              let started =
                  unit {
                      let! _ = step 1
                      do! cancelled
                  }

              let attempt = Op.run started (Counter()) source.Token

              try
                  let! _ = attempt
                  failtest "a cancelled unit must not complete"
              with :? OperationCanceledException ->
                  ()

              Expect.isTrue attempt.IsCanceled "the task is cancelled, not faulted into a store error"
              source.Dispose()
          } ]

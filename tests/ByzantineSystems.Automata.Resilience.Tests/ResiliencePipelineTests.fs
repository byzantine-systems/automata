module ByzantineSystems.Automata.Resilience.Tests.ResiliencePipelineTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Resilience
open Expecto
open Polly
open Polly.CircuitBreaker
open Polly.Timeout

/// Stands in for a driver failure that is worth another attempt: a dropped socket, a pool
/// timeout, a server still coming back up.
exception Transient of string

/// Stands in for a server that answered, and answered no. Repeating it produces the same no.
exception Permanent of string

let isTransient (error: exn) =
    match error with
    | Transient _ -> true
    | _ -> false

let fastPolicy: TransientPolicy =
    { TransientPolicy.defaults with
        MaxAttempts = 3
        BaseDelay = TimeSpan.FromMilliseconds 1.
        MaxDelay = TimeSpan.FromMilliseconds 5. }

let pipelineOf policy =
    TransientPolicy.toPipeline policy "test" isTransient ignore

let expectSingleValidationError expected (policy: TransientPolicy) =
    match TransientPolicy.validate policy with
    | Error errors -> Expect.equal [ expected ] errors "the expected validation error is returned"
    | Ok _ -> failtestf "expected %A" expected

/// An operation whose responses follow a script, counting executions as it goes.
let scripted (outcomes: Result<int, exn> list) =
    let calls = ref 0

    let operation (_: CancellationToken) : Task<int> =
        task {
            let index = calls.Value
            calls.Value <- index + 1

            return
                match
                    (if index < outcomes.Length then
                         outcomes[index]
                     else
                         List.last outcomes)
                with
                | Ok value -> value
                | Error error -> raise error
        }

    operation, calls

let run (pipeline: ResiliencePipeline) operation =
    ResiliencePipeline.executeTask pipeline operation CancellationToken.None

let validationTests =
    testList
        "TransientPolicy.validate"
        [ test "the defaults are valid" {
              match TransientPolicy.validate TransientPolicy.defaults with
              | Ok _ -> ()
              | Error errors -> failtestf "the defaults should validate, but reported %A" errors
          }
          test "attempts below one are rejected" {
              { TransientPolicy.defaults with
                  MaxAttempts = 0 }
              |> expectSingleValidationError (MaxAttemptsBelowOne 0)
          }
          test "a delay cap below the seed is rejected" {
              let policy =
                  { TransientPolicy.defaults with
                      BaseDelay = TimeSpan.FromSeconds 5.
                      MaxDelay = TimeSpan.FromSeconds 1. }

              policy
              |> expectSingleValidationError (MaxDelayBelowBaseDelay(TimeSpan.FromSeconds 5., TimeSpan.FromSeconds 1.))
          }
          test "a total budget below one attempt is rejected" {
              let policy =
                  { TransientPolicy.defaults with
                      AttemptTimeout = TimeSpan.FromSeconds 30.
                      TotalTimeout = TimeSpan.FromSeconds 5. }

              policy
              |> expectSingleValidationError (
                  TotalTimeoutBelowAttemptTimeout(TimeSpan.FromSeconds 5., TimeSpan.FromSeconds 30.)
              )
          }
          test "every defect is reported, not just the first" {
              let policy =
                  { TransientPolicy.defaults with
                      MaxAttempts = 0
                      BaseDelay = TimeSpan.FromSeconds -1. }

              match TransientPolicy.validate policy with
              | Error errors -> Expect.isGreaterThan errors.Length 1 "accumulates rather than short-circuits"
              | Ok _ -> failtest "an invalid policy validated"
          } ]

let retryTests =
    testList
        "retry"
        [ testTask "a transient failure is repeated until it succeeds" {
              let operation, calls =
                  scripted [ Error(Transient "socket"); Error(Transient "socket"); Ok 42 ]

              let! value = run (pipelineOf fastPolicy) operation
              Expect.equal 42 value "the eventual success is returned"
              Expect.equal 3 calls.Value "two retries after the first attempt"
          }
          testTask "a permanent failure is not repeated" {
              let operation, calls = scripted [ Error(Permanent "constraint violated") ]

              let! outcome =
                  task {
                      try
                          let! _ = run (pipelineOf fastPolicy) operation
                          return "completed"
                      with Permanent _ ->
                          return "raised"
                  }

              Expect.equal "raised" outcome "the failure reaches the caller"
              Expect.equal 1 calls.Value "a call that cannot succeed on repeat is made once"
          }
          testTask "attempts stop at the configured maximum" {
              let operation, calls = scripted [ Error(Transient "socket") ]

              let! _ =
                  task {
                      try
                          let! _ = run (pipelineOf fastPolicy) operation
                          return ()
                      with Transient _ ->
                          return ()
                  }

              Expect.equal 3 calls.Value "MaxAttempts counts total executions including the first"
          }
          testTask "a single-attempt policy never retries" {
              let operation, calls = scripted [ Error(Transient "socket") ]

              let single = { fastPolicy with MaxAttempts = 1 }

              let! _ =
                  task {
                      try
                          let! _ = run (pipelineOf single) operation
                          return ()
                      with Transient _ ->
                          return ()
                  }

              Expect.equal 1 calls.Value "one execution"
          } ]

let timeoutTests =
    testList
        "timeout"
        [ testTask "an attempt that overruns its budget is retried" {
              let calls = ref 0

              let policy =
                  { fastPolicy with
                      AttemptTimeout = TimeSpan.FromMilliseconds 20.
                      TotalTimeout = TimeSpan.FromSeconds 5. }

              let operation (token: CancellationToken) =
                  task {
                      let index = calls.Value
                      calls.Value <- index + 1

                      // The first attempt honours the token it is handed, which is the only
                      // reason a timeout can interrupt it at all.
                      if index = 0 then
                          do! Task.Delay(TimeSpan.FromSeconds 5., token)

                      return 7
                  }

              let! value = run (pipelineOf policy) operation
              Expect.equal 7 value "the second attempt succeeds"
              Expect.equal 2 calls.Value "the timed-out attempt was retried"
          }
          testTask "an overall budget covering every attempt eventually gives up" {
              let policy =
                  { fastPolicy with
                      MaxAttempts = 100
                      AttemptTimeout = TimeSpan.FromMilliseconds 20.
                      TotalTimeout = TimeSpan.FromMilliseconds 100. }

              let operation (token: CancellationToken) =
                  task {
                      do! Task.Delay(TimeSpan.FromSeconds 5., token)
                      return 0
                  }

              let! outcome =
                  task {
                      try
                          let! _ = run (pipelineOf policy) operation
                          return "completed"
                      with :? TimeoutRejectedException ->
                          return "timed out"
                  }

              Expect.equal "timed out" outcome "the total budget bounds the whole operation"
          } ]

let breakerTests =
    testList
        "circuit breaker"
        [ testTask "the circuit opens once the failure ratio is reached" {
              let policy =
                  { fastPolicy with
                      MaxAttempts = 1
                      Breaker =
                          Some
                              { FailureRatio = 0.5
                                MinimumThroughput = 2
                                SamplingDuration = TimeSpan.FromSeconds 10.
                                BreakDuration = TimeSpan.FromSeconds 30. } }

              let pipeline = pipelineOf policy
              let operation, _ = scripted [ Error(Transient "down") ]

              let attempt () =
                  task {
                      try
                          let! _ = run pipeline operation
                          return "ok"
                      with
                      | :? BrokenCircuitException -> return "open"
                      | Transient _ -> return "failed"
                  }

              let! first = attempt ()
              let! second = attempt ()
              let! third = attempt ()

              Expect.equal "failed" first "the first failure passes through"
              Expect.equal "failed" second "the second failure passes through"
              Expect.equal "open" third "the breaker refuses the third before making the call"
          }
          testTask "a permanent failure does not count against the dependency" {
              let policy =
                  { fastPolicy with
                      MaxAttempts = 1
                      Breaker =
                          Some
                              { FailureRatio = 0.5
                                MinimumThroughput = 2
                                SamplingDuration = TimeSpan.FromSeconds 10.
                                BreakDuration = TimeSpan.FromSeconds 30. } }

              let pipeline = pipelineOf policy
              let operation, calls = scripted [ Error(Permanent "constraint violated") ]

              let attempt () =
                  task {
                      try
                          let! _ = run pipeline operation
                          return "ok"
                      with
                      | :? BrokenCircuitException -> return "open"
                      | Permanent _ -> return "failed"
                  }

              let! _ = attempt ()
              let! _ = attempt ()
              let! third = attempt ()

              Expect.equal "failed" third "a rejected statement says nothing about the store's health"
              Expect.equal 3 calls.Value "every call was actually made"
          } ]

let telemetryTests =
    testList
        "telemetry"
        [ testTask "retries are reported to the sink" {
              let events = ResizeArray()

              let sink event =
                  lock events (fun () -> events.Add event)

              let pipeline = TransientPolicy.toPipeline fastPolicy "test" isTransient sink
              let operation, _ = scripted [ Error(Transient "socket"); Ok 1 ]
              let! _ = ResiliencePipeline.executeTask pipeline operation CancellationToken.None

              let retries =
                  events
                  |> Seq.filter (function
                      | RetryScheduled _ -> true
                      | _ -> false)
                  |> Seq.length

              Expect.equal 1 retries "one retry was scheduled"
          }
          testTask "a throwing sink cannot change what the pipeline does" {
              let pipeline =
                  TransientPolicy.toPipeline fastPolicy "test" isTransient (fun _ ->
                      failwith "an observer that misbehaves")

              let operation, calls = scripted [ Error(Transient "socket"); Ok 5 ]
              let! value = ResiliencePipeline.executeTask pipeline operation CancellationToken.None
              Expect.equal 5 value "the retry still happened"
              Expect.equal 2 calls.Value "and so did both attempts"
          } ]

let cancellationTests =
    testList
        "cancellation"
        [ testTask "caller cancellation is never retried" {
              let cancellation = new CancellationTokenSource()
              let calls = ref 0

              let operation (token: CancellationToken) =
                  task {
                      calls.Value <- calls.Value + 1
                      cancellation.Cancel()
                      token.ThrowIfCancellationRequested()
                      return 0
                  }

              let! outcome =
                  task {
                      try
                          let! _ = ResiliencePipeline.executeTask (pipelineOf fastPolicy) operation cancellation.Token

                          return "completed"
                      with :? OperationCanceledException ->
                          return "cancelled"
                  }

              Expect.equal "cancelled" outcome "cancellation propagates"
              Expect.equal 1 calls.Value "a cancelled call is not repeated"
          } ]

/// The outcome API is where store failures become values, so it must never throw, whatever the
/// pipeline is made of and however the work fails.
let outcomeTests =
    let outcomeOf (pipeline: ResiliencePipeline) (operation: CancellationToken -> Task<int>) : Task<Outcome<int>> =
        ResiliencePipeline.executeOutcome pipeline operation CancellationToken.None

    testList
        "executeOutcome"
        [ testTask "the empty pipeline hands back an exception thrown before the work's task exists" {
              let operation (_: CancellationToken) : Task<int> = raise (Permanent "synchronous")
              let! (outcome: Outcome<int>) = outcomeOf Polly.ResiliencePipeline.Empty operation

              match outcome.Exception with
              | Permanent _ -> ()
              | other -> failtestf "expected the work's exception, got %A" other
          }

          testTask "the empty pipeline hands back an exception the work's task faulted with" {
              let operation, _ = scripted [ Error(Permanent "asynchronous") ]
              let! (outcome: Outcome<int>) = outcomeOf Polly.ResiliencePipeline.Empty operation

              match outcome.Exception with
              | Permanent _ -> ()
              | other -> failtestf "expected the work's exception, got %A" other

              Expect.stringContains
                  outcome.Exception.StackTrace
                  "ResiliencePipelineTests.operation"
                  "with the stack trace of the throw"
          }

          testTask "retries still see failures that arrive as outcomes" {
              let operation, calls = scripted [ Error(Transient "once"); Ok 7 ]
              let! (outcome: Outcome<int>) = outcomeOf (pipelineOf fastPolicy) operation
              Expect.equal outcome.Result 7 "the second attempt's value"
              Expect.equal calls.Value 2 "after one retry"
          }

          testTask "an overrun attempt is a timeout, not the work's cancellation" {
              let policy =
                  { fastPolicy with
                      MaxAttempts = 1
                      AttemptTimeout = TimeSpan.FromMilliseconds 20.
                      TotalTimeout = TimeSpan.FromSeconds 5. }

              let operation (token: CancellationToken) : Task<int> =
                  task {
                      do! Task.Delay(Timeout.Infinite, token)
                      return 0
                  }

              let! (outcome: Outcome<int>) = outcomeOf (pipelineOf policy) operation
              Expect.isTrue (outcome.Exception :? TimeoutRejectedException) "the strategy's own exception"
          }

          testTask "the caller's cancellation is a cancellation" {
              let source = new CancellationTokenSource()
              source.Cancel()

              let operation (token: CancellationToken) : Task<int> =
                  task {
                      do! Task.Delay(Timeout.Infinite, token)
                      return 0
                  }

              let! (outcome: Outcome<int>) =
                  ResiliencePipeline.executeOutcome Polly.ResiliencePipeline.Empty operation source.Token

              Expect.isTrue (outcome.Exception :? OperationCanceledException) "reported, not thrown"
              source.Dispose()
          } ]

let tests =
    testList
        "resilience"
        [ validationTests
          retryTests
          timeoutTests
          breakerTests
          telemetryTests
          cancellationTests
          outcomeTests ]

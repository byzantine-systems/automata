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

type private ValidationInput =
    { Attempts: byte
      BaseDelay: byte
      MaxDelay: byte
      AttemptTimeout: byte
      TotalTimeout: byte
      HasBreaker: bool
      FailureRatio: byte
      MinimumThroughput: byte
      SamplingDuration: byte
      BreakDuration: byte }

let private durations =
    [| TimeSpan.FromSeconds -1.
       TimeSpan.Zero
       TimeSpan.FromTicks 1L
       TimeSpan.FromMilliseconds 9.999
       TimeSpan.FromMilliseconds 10.
       TimeSpan.FromMilliseconds 500.
       TimeSpan.FromDays 1.
       TimeSpan.FromDays(1.).Add(TimeSpan.FromTicks 1L) |]

let private pick (values: 'T array) seed = values[int seed % values.Length]

let private expectedValidationErrors (policy: TransientPolicy) =
    let minimumTimeout = TimeSpan.FromMilliseconds 10.
    let minimumBreakerDuration = TimeSpan.FromMilliseconds 500.
    let maximumDuration = TimeSpan.FromDays 1.

    [ if policy.MaxAttempts < 1 then
          MaxAttemptsBelowOne policy.MaxAttempts

      if policy.BaseDelay < TimeSpan.Zero then
          NegativeBaseDelay policy.BaseDelay

      if policy.MaxDelay < policy.BaseDelay then
          MaxDelayBelowBaseDelay(policy.BaseDelay, policy.MaxDelay)

      if policy.AttemptTimeout <= TimeSpan.Zero then
          AttemptTimeoutNotPositive policy.AttemptTimeout
      elif policy.AttemptTimeout < minimumTimeout || policy.AttemptTimeout > maximumDuration then
          AttemptTimeoutOutOfRange policy.AttemptTimeout

      if policy.TotalTimeout <= TimeSpan.Zero then
          TotalTimeoutNotPositive policy.TotalTimeout
      else
          if policy.TotalTimeout < minimumTimeout || policy.TotalTimeout > maximumDuration then
              TotalTimeoutOutOfRange policy.TotalTimeout

          if policy.TotalTimeout < policy.AttemptTimeout then
              TotalTimeoutBelowAttemptTimeout(policy.TotalTimeout, policy.AttemptTimeout)

      match policy.Breaker with
      | None -> ()
      | Some breaker ->
          if
              Double.IsNaN breaker.FailureRatio
              || breaker.FailureRatio <= 0.0
              || breaker.FailureRatio > 1.0
          then
              BreakerFailureRatioOutOfRange breaker.FailureRatio

          if breaker.MinimumThroughput < 2 then
              BreakerMinimumThroughputBelowTwo breaker.MinimumThroughput

          if breaker.SamplingDuration <= TimeSpan.Zero then
              BreakerSamplingDurationNotPositive breaker.SamplingDuration
          elif
              breaker.SamplingDuration < minimumBreakerDuration
              || breaker.SamplingDuration > maximumDuration
          then
              BreakerSamplingDurationOutOfRange breaker.SamplingDuration

          if breaker.BreakDuration <= TimeSpan.Zero then
              BreakerBreakDurationNotPositive breaker.BreakDuration
          elif breaker.BreakDuration < minimumBreakerDuration || breaker.BreakDuration > maximumDuration then
              BreakerBreakDurationOutOfRange breaker.BreakDuration ]

let private sameValidationError expected actual =
    match expected, actual with
    | BreakerFailureRatioOutOfRange left, BreakerFailureRatioOutOfRange right ->
        (Double.IsNaN left && Double.IsNaN right) || left = right
    | _ -> expected = actual

let private sameValidationErrors expected actual =
    List.length expected = List.length actual && List.forall2 sameValidationError expected actual

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

          Property.property "validation reports exactly every structural defect" (fun input ->
              let attempts = pick [| Int32.MinValue; -1; 0; 1; 2; Int32.MaxValue |] input.Attempts
              let ratios = [| Double.NaN; Double.NegativeInfinity; -1.; 0.; 0.5; 1.; 1.1; Double.PositiveInfinity |]
              let throughput = pick [| Int32.MinValue; -1; 0; 1; 2; Int32.MaxValue |] input.MinimumThroughput

              let breaker =
                  if input.HasBreaker then
                      Some
                          { FailureRatio = pick ratios input.FailureRatio
                            MinimumThroughput = throughput
                            SamplingDuration = pick durations input.SamplingDuration
                            BreakDuration = pick durations input.BreakDuration }
                  else
                      None

              let policy =
                  { TransientPolicy.defaults with
                      MaxAttempts = attempts
                      BaseDelay = pick durations input.BaseDelay
                      MaxDelay = pick durations input.MaxDelay
                      AttemptTimeout = pick durations input.AttemptTimeout
                      TotalTimeout = pick durations input.TotalTimeout
                      Breaker = breaker }

              match expectedValidationErrors policy, TransientPolicy.validate policy with
              | [], Ok validated -> validated = policy
              | expected, Error actual -> sameValidationErrors expected actual
              | _ -> false) ]

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

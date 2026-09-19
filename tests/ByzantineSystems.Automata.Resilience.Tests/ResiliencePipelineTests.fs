module ByzantineSystems.Automata.Resilience.Tests.ResiliencePipelineTests

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Resilience
open Expecto
open Polly
open Polly.Retry
open Polly.Simmy

let receipt: CommitReceipt =
    { IdempotencyKey = "pay-1"
      Epoch = Epoch.ofUInt64 1UL
      OccurredAt = DateTimeOffset(2026, 9, 19, 12, 0, 0, TimeSpan.Zero) }

let unavailable (message: string) : MachineError<string> =
    Store(Unavailable(InvalidOperationException message))

let rejection: MachineError<string> =
    Transition(TransitionError.Rejected "invalid amount")

let unhandled: MachineError<string> =
    Transition(Unhandled(stateId "idle", "UnknownEvent"))

let conflict (expected: Epoch) (actual: Epoch) : MachineError<string> = Store(Concurrency(expected, actual))

let fastConfig: RetryConfig<string> =
    { RetryConfig.defaults with
        MaxAttempts = 3
        BaseDelay = TimeSpan.FromMilliseconds 1.
        MaxDelay = TimeSpan.FromMilliseconds 5. }

let expectSingleValidationError expected (config: RetryConfig<string>) =
    match RetryConfig.validate config with
    | Error errors -> Expect.equal [ expected ] errors "the expected validation error is returned"
    | Ok _ -> failtestf "expected %A" expected

/// One shared operation whose responses follow a script; also counts executions.
let scripted (outcomes: Result<CommitReceipt, MachineError<string>> list) =
    let calls = ref 0

    let operation (_: CancellationToken) : Task<Result<CommitReceipt, MachineError<string>>> =
        task {
            let index = !calls
            incr calls

            return
                if index < outcomes.Length then
                    outcomes[index]
                else
                    List.last outcomes
        }

    operation, calls

let validationTests =
    testList
        "RetryConfig.validate"
        [ test "defaults are valid" {
              Expect.isOk (RetryConfig.validate RetryConfig.defaults) "the default policy passes"
          }

          test "each defect is reported" {
              { RetryConfig.defaults with
                  MaxAttempts = 0 }
              |> expectSingleValidationError (MaxAttemptsBelowOne 0)

              { RetryConfig.defaults with
                  MaxDelay = TimeSpan.Zero }
              |> expectSingleValidationError (MaxDelayBelowBaseDelay(TimeSpan.FromMilliseconds 200., TimeSpan.Zero))

              { RetryConfig.defaults with
                  TotalTimeout = TimeSpan.FromSeconds 1. }
              |> expectSingleValidationError (
                  TotalTimeoutBelowAttemptTimeout(TimeSpan.FromSeconds 1., TimeSpan.FromSeconds 5.)
              )
          }

          test "breaker parameters are all validated" {
              let config =
                  { RetryConfig.defaults with
                      Breaker =
                          Some
                              { FailureRatio = 1.5
                                MinimumThroughput = 1
                                SamplingDuration = TimeSpan.Zero
                                BreakDuration = TimeSpan.Zero } }

              match RetryConfig.validate config with
              | Error errors ->
                  Expect.contains errors (BreakerFailureRatioOutOfRange 1.5) "ratio out of range"
                  Expect.contains errors (BreakerMinimumThroughputBelowTwo 1) "throughput below two"
                  Expect.contains errors (BreakerSamplingDurationNotPositive TimeSpan.Zero) "sampling not positive"
                  Expect.contains errors (BreakerBreakDurationNotPositive TimeSpan.Zero) "break not positive"
              | Ok _ -> failtest "expected breaker defects"
          }

          test "defects accumulate" {
              let config =
                  { fastConfig with
                      MaxAttempts = 0
                      BaseDelay = TimeSpan.FromSeconds -1. }

              match RetryConfig.validate config with
              | Error errors -> Expect.isGreaterThanOrEqual errors.Length 2 "multiple defects at once"
              | Ok _ -> failtest "expected accumulated defects"
          }

          test "Polly 8.8 timing bounds are validated before pipeline construction" {
              let tooShort = TimeSpan.FromMilliseconds 1.
              let breakerTooShort = TimeSpan.FromMilliseconds 100.

              let config =
                  { fastConfig with
                      AttemptTimeout = tooShort
                      TotalTimeout = tooShort
                      Breaker =
                          Some
                              { FailureRatio = 0.5
                                MinimumThroughput = 2
                                SamplingDuration = breakerTooShort
                                BreakDuration = breakerTooShort } }

              match RetryConfig.validate config with
              | Error errors ->
                  Expect.contains
                      errors
                      (AttemptTimeoutOutOfRange tooShort)
                      "attempt timeout matches Polly's lower bound"

                  Expect.contains errors (TotalTimeoutOutOfRange tooShort) "total timeout matches Polly's lower bound"

                  Expect.contains
                      errors
                      (BreakerSamplingDurationOutOfRange breakerTooShort)
                      "sampling duration matches Polly's lower bound"

                  Expect.contains
                      errors
                      (BreakerBreakDurationOutOfRange breakerTooShort)
                      "break duration matches Polly's lower bound"
              | Ok _ -> failtest "expected Polly timing defects"
          } ]

let retryTests =
    testList
        "pipeline retry semantics"
        [ testTask "a transient failure is retried and succeeds" {
              let operation, calls = scripted [ Error(unavailable "db down"); Ok receipt ]
              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Ok receipt) outcome "the second attempt commits"
              Expect.equal 2 !calls "exactly two executions"
          }

          testTask "MaxAttempts bounds total executions" {
              let operation, calls = scripted [ Error(unavailable "still down") ]
              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.isError outcome "the pipeline gives up"
              Expect.equal 3 !calls "attempts = MaxAttempts"
          }

          testTask "a single attempt executes exactly once" {
              let operation, calls = scripted [ Error(unavailable "down") ]
              let config = { fastConfig with MaxAttempts = 1 }
              let pipeline = RetryConfig.toPipeline config ignore

              let! _ = Retry.execute pipeline operation CancellationToken.None

              Expect.equal 1 !calls "no retries with MaxAttempts = 1"
          }

          testTask "a domain rejection is returned directly" {
              let operation, calls = scripted [ Error rejection ]
              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Error rejection) outcome "the rejection is the outcome"
              Expect.equal 1 !calls "domain rejections are never retried"
          }

          testTask "an unhandled event is returned directly" {
              let operation, calls = scripted [ Error unhandled ]
              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Error unhandled) outcome "unhandled is the outcome"
              Expect.equal 1 !calls "unhandled events are never retried"
          }

          testTask "an optimistic conflict is retried" {
              let operation, calls =
                  scripted [ Error(conflict Epoch.initial (Epoch.next Epoch.initial)); Ok receipt ]

              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Ok receipt) outcome "the re-read attempt commits"
              Expect.equal 2 !calls "the conflict consumed one retry"
          }

          testTask "a typed timeout result is not an attempt-timeout retry signal" {
              let timeout = MachineError.Timeout(TimeSpan.FromSeconds 1.)
              let operation, calls = scripted [ Error timeout; Ok receipt ]
              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Error timeout) outcome "only Polly's inner TimeoutRejectedException triggers retry"
              Expect.equal 1 !calls "typed timeout results are returned directly"
          }

          testTask "caller cancellation propagates and is not retried" {
              let cts = new CancellationTokenSource()
              let calls = ref 0

              let operation (token: CancellationToken) : Task<Result<CommitReceipt, MachineError<string>>> =
                  task {
                      incr calls
                      cts.Cancel()
                      return! Task.FromCanceled<Result<CommitReceipt, MachineError<string>>>(token)
                  }

              let pipeline = RetryConfig.toPipeline fastConfig ignore

              let! observed =
                  task {
                      try
                          let! _ = Retry.execute pipeline operation cts.Token
                          return Error "expected cancellation"
                      with :? OperationCanceledException ->
                          return Ok()
                  }

              Expect.isOk observed "the send surfaced cancellation"
              Expect.equal 1 !calls "cancellation is never retried"
          }

          testTask "an attempt timeout reports the attempt budget" {
              let config =
                  { fastConfig with
                      MaxAttempts = 1
                      AttemptTimeout = TimeSpan.FromMilliseconds 50.
                      TotalTimeout = TimeSpan.FromSeconds 30. }

              let pipeline = RetryConfig.toPipeline config ignore

              let operation (token: CancellationToken) =
                  task {
                      do! Task.Delay(TimeSpan.FromSeconds 10., token)
                      return Ok receipt
                  }

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              match outcome with
              | Error(Timeout budget) ->
                  Expect.equal (TimeSpan.FromMilliseconds 50.) budget "the fired budget is reported"
              | other -> failtestf "expected a timeout, got %A" other
          }

          testTask "a total timeout reports the total budget" {
              let config =
                  { fastConfig with
                      MaxAttempts = 5
                      AttemptTimeout = TimeSpan.FromMilliseconds 60.
                      TotalTimeout = TimeSpan.FromMilliseconds 100. }

              let pipeline = RetryConfig.toPipeline config ignore

              let operation (token: CancellationToken) =
                  task {
                      do! Task.Delay(TimeSpan.FromSeconds 10., token)
                      return Ok receipt
                  }

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              match outcome with
              | Error(Timeout budget) ->
                  Expect.equal (TimeSpan.FromMilliseconds 100.) budget "the total budget is reported"
              | other -> failtestf "expected a timeout, got %A" other
          } ]

let breakerTests =
    testList
        "circuit breaker"
        [ let breakerConfig () =
              { fastConfig with
                  MaxAttempts = 1
                  Breaker =
                      Some
                          { FailureRatio = 0.5
                            MinimumThroughput = 2
                            SamplingDuration = TimeSpan.FromMinutes 1.
                            BreakDuration = TimeSpan.FromSeconds 30. } }

          testTask "the breaker opens after enough outages and fails fast" {
              let config = breakerConfig ()
              let pipeline = RetryConfig.toPipeline config ignore
              let calls = ref 0

              let operation (_: CancellationToken) =
                  task {
                      incr calls
                      return Error(unavailable "db down")
                  }

              let! first = Retry.execute pipeline operation CancellationToken.None
              let! second = Retry.execute pipeline operation CancellationToken.None
              Expect.isError first "first outage is an error"
              Expect.isError second "second outage is an error"
              Expect.equal 2 !calls "both executed"

              let! third = Retry.execute pipeline operation CancellationToken.None

              match third with
              | Error(CircuitOpen _) -> Expect.equal 2 !calls "an open circuit does not execute the callback"
              | other -> failtestf "expected an open circuit, got %A" other
          }

          testTask "domain rejections do not open the breaker" {
              let config = breakerConfig ()
              let pipeline = RetryConfig.toPipeline config ignore
              let calls = ref 0

              let operation (_: CancellationToken) =
                  task {
                      incr calls

                      return
                          if !calls = 1 then
                              Error rejection
                          else
                              Error(unavailable "db down")
                  }

              let! first = Retry.execute pipeline operation CancellationToken.None
              Expect.equal (Error rejection) first "the rejection returns directly"
              Expect.equal 1 !calls "no retry for a rejection"

              let! _ = Retry.execute pipeline operation CancellationToken.None
              let! third = Retry.execute pipeline operation CancellationToken.None

              match third with
              | Error(CircuitOpen _) -> Expect.equal 2 !calls "the rejection was not classified as a breaker failure"
              | other -> failtestf "expected an open circuit, got %A" other
          }

          testTask "typed timeout results do not count as breaker outages" {
              let config = breakerConfig ()
              let pipeline = RetryConfig.toPipeline config ignore
              let calls = ref 0

              let operation (_: CancellationToken) =
                  task {
                      incr calls
                      return Error(MachineError.Timeout(TimeSpan.FromSeconds 1.))
                  }

              let! _ = Retry.execute pipeline operation CancellationToken.None
              let! _ = Retry.execute pipeline operation CancellationToken.None
              let! third = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Error(MachineError.Timeout(TimeSpan.FromSeconds 1.))) third "the callback still executes"
              Expect.equal 3 !calls "the circuit remains closed"
          }

          testTask "pipeline events are relayed to the sink" {
              let events = ResizeArray<PipelineEvent>()
              let sink event = lock events (fun _ -> events.Add event)
              let config = { fastConfig with MaxAttempts = 2 }
              let pipeline = RetryConfig.toPipeline config sink
              let operation, _ = scripted [ Error(unavailable "down"); Ok receipt ]

              let! _ = Retry.execute pipeline operation CancellationToken.None

              let retryEvent =
                  lock events (fun _ ->
                      events
                      |> Seq.tryFind (function
                          | RetryScheduled _ -> true
                          | _ -> false))

              match retryEvent with
              | Some(RetryScheduled(attempt, _)) -> Expect.equal 1 attempt "the first retry is scheduled"
              | other -> failtestf "expected a retry event, got %A" other
          } ]

let chaosTests =
    testList
        "chaos injection"
        [ testTask "an injected transient outcome is retried without invoking the operation" {
              let calls = ref 0
              let injections = ref 0

              let retry = RetryStrategyOptions<Result<CommitReceipt, MachineError<string>>>()
              retry.MaxRetryAttempts <- 1
              retry.Delay <- TimeSpan.FromMilliseconds 1.

              retry.ShouldHandle <-
                  fun (args: RetryPredicateArguments<Result<CommitReceipt, MachineError<string>>>) ->
                      let handled =
                          (box args.Outcome.Result = null)
                          || match args.Outcome.Result with
                             | Error(Store(Unavailable _)) -> true
                             | _ -> false

                      ValueTask<bool>(handled)

              let inject =
                  Func<Result<CommitReceipt, MachineError<string>>>(fun () ->
                      lock injections (fun _ ->
                          incr injections

                          if !injections = 1 then
                              Error(unavailable "chaos")
                          else
                              Ok receipt))

              let pipeline =
                  ResiliencePipelineBuilder<Result<CommitReceipt, MachineError<string>>>()
                      .AddRetry(retry)
                      .AddChaosOutcome(1.0, inject)
                      .Build()

              let operation (_: CancellationToken) =
                  task {
                      incr calls
                      return Ok receipt
                  }

              let! outcome = Retry.execute pipeline operation CancellationToken.None

              Expect.equal (Ok receipt) outcome "the retry recovered from the injected outcome"
              Expect.equal 0 !calls "chaos bypasses the operation entirely"
              Expect.equal 2 !injections "each attempt got an injected outcome"
          } ]

let tests =
    testList "resilience pipeline" [ validationTests; retryTests; breakerTests; chaosTests ]

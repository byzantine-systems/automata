namespace ByzantineSystems.Automata.Resilience

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open Polly
open Polly.CircuitBreaker
open Polly.Retry
open Polly.Timeout

/// <summary>
/// Abbreviation for the value every machine pipeline carries: a committed receipt, or the
/// machine-level error that explains why there is none.
/// </summary>
type PipelineResult<'Err> = Result<CommitReceipt, MachineError<'Err>>

/// <summary>
/// Circuit-breaker parameters. All four are required when a breaker is configured:
/// Polly would otherwise use defaults unrelated to this workload.
/// </summary>
type BreakerConfig =
    { FailureRatio: double
      MinimumThroughput: int
      SamplingDuration: TimeSpan
      BreakDuration: TimeSpan }

/// <summary>Structural defects in a <see cref="T:ByzantineSystems.Automata.Resilience.RetryConfig`1" />.</summary>
type ConfigError =

    /// <summary>MaxAttempts counts total executions including the first; it must be at least 1.</summary>
    | MaxAttemptsBelowOne of attempts: int

    /// <summary>The backoff seed must not be negative.</summary>
    | NegativeBaseDelay of TimeSpan

    /// <summary>The delay cap must not be below the seed.</summary>
    | MaxDelayBelowBaseDelay of baseDelay: TimeSpan * maxDelay: TimeSpan

    /// <summary>Each attempt needs a positive budget.</summary>
    | AttemptTimeoutNotPositive of TimeSpan

    /// <summary>Polly requires each attempt budget to be between 10 milliseconds and 24 hours.</summary>
    | AttemptTimeoutOutOfRange of TimeSpan

    /// <summary>The overall budget must be positive.</summary>
    | TotalTimeoutNotPositive of TimeSpan

    /// <summary>Polly requires the overall budget to be between 10 milliseconds and 24 hours.</summary>
    | TotalTimeoutOutOfRange of TimeSpan

    /// <summary>The overall budget must cover at least one attempt.</summary>
    | TotalTimeoutBelowAttemptTimeout of totalTimeout: TimeSpan * attemptTimeout: TimeSpan

    /// <summary>Polly requires a ratio in (0, 1].</summary>
    | BreakerFailureRatioOutOfRange of ratio: double

    /// <summary>Polly requires a sampling throughput of at least 2.</summary>
    | BreakerMinimumThroughputBelowTwo of minimumThroughput: int

    /// <summary>The failure-sampling window must be positive.</summary>
    | BreakerSamplingDurationNotPositive of TimeSpan

    /// <summary>Polly requires the sampling window to be between 500 milliseconds and 24 hours.</summary>
    | BreakerSamplingDurationOutOfRange of TimeSpan

    /// <summary>The break duration must be positive.</summary>
    | BreakerBreakDurationNotPositive of TimeSpan

    /// <summary>Polly requires the break duration to be between 500 milliseconds and 24 hours.</summary>
    | BreakerBreakDurationOutOfRange of TimeSpan

/// <summary>
/// Short-horizon resilience policy for one machine/store pair. Polly governs in-process
/// retry (milliseconds to seconds); durable, long-horizon retry is a separate mechanism
/// owned by the runtime. <c>MaxAttempts</c> counts total executions including the first;
/// Polly's <c>MaxRetryAttempts</c> means retries after the first call, so the pipeline is
/// configured with <c>MaxAttempts - 1</c>.
/// </summary>
type RetryConfig<'Err> =
    {
        MaxAttempts: int
        BaseDelay: TimeSpan
        MaxDelay: TimeSpan
        Backoff: DelayBackoffType
        UseJitter: bool
        AttemptTimeout: TimeSpan
        TotalTimeout: TimeSpan
        Breaker: BreakerConfig option
        /// <summary>Decides the final disposition of a failed send after the pipeline finishes.</summary>
        Classify: MachineError<'Err> -> Disposition
    }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Resilience.RetryConfig`1" />.</summary>
[<RequireQualifiedAccess>]
module RetryConfig =

    /// <summary>
    /// A safe default policy: three attempts, jittered exponential backoff, and a
    /// classification that defers transient infrastructure failures, returns domain
    /// rejections to the caller, and escalates anything unclassified.
    /// </summary>
    let defaults<'Err> : RetryConfig<'Err> =
        { MaxAttempts = 3
          BaseDelay = TimeSpan.FromMilliseconds 200.
          MaxDelay = TimeSpan.FromSeconds 30.
          Backoff = DelayBackoffType.Exponential
          UseJitter = true
          AttemptTimeout = TimeSpan.FromSeconds 5.
          TotalTimeout = TimeSpan.FromSeconds 30.
          Breaker = None
          Classify =
            fun error ->
                match error with
                | Store(Unavailable _)
                | Store(Concurrency _) -> Defer
                | Timeout _
                | CircuitOpen _ -> Defer
                | Transition(TransitionError.Rejected _)
                | Transition(Unhandled _) -> Reject
                | _ -> Escalate }

    /// <summary>Accumulates every structural defect rather than stopping at the first.</summary>
    let validate (config: RetryConfig<'Err>) : Result<RetryConfig<'Err>, ConfigError list> =
        let minimumTimeout = TimeSpan.FromMilliseconds 10.
        let minimumBreakerDuration = TimeSpan.FromMilliseconds 500.
        let maximumDuration = TimeSpan.FromDays 1.

        let retryErrors =
            [ if config.MaxAttempts < 1 then
                  MaxAttemptsBelowOne config.MaxAttempts

              if config.BaseDelay < TimeSpan.Zero then
                  NegativeBaseDelay config.BaseDelay

              if config.MaxDelay < config.BaseDelay then
                  MaxDelayBelowBaseDelay(config.BaseDelay, config.MaxDelay)

              if config.AttemptTimeout <= TimeSpan.Zero then
                  AttemptTimeoutNotPositive config.AttemptTimeout
              elif
                  config.AttemptTimeout < minimumTimeout
                  || config.AttemptTimeout > maximumDuration
              then
                  AttemptTimeoutOutOfRange config.AttemptTimeout

              if config.TotalTimeout <= TimeSpan.Zero then
                  TotalTimeoutNotPositive config.TotalTimeout
              else
                  if config.TotalTimeout < minimumTimeout || config.TotalTimeout > maximumDuration then
                      TotalTimeoutOutOfRange config.TotalTimeout

                  if config.TotalTimeout < config.AttemptTimeout then
                      TotalTimeoutBelowAttemptTimeout(config.TotalTimeout, config.AttemptTimeout) ]

        let breakerErrors =
            config.Breaker
            |> Option.map (fun breaker ->
                [ if
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
                  elif
                      breaker.BreakDuration < minimumBreakerDuration
                      || breaker.BreakDuration > maximumDuration
                  then
                      BreakerBreakDurationOutOfRange breaker.BreakDuration ])
            |> Option.defaultValue []

        match retryErrors @ breakerErrors with
        | [] -> Ok config
        | errors -> Error errors

    /// <summary>
    /// Builds the shared operation pipeline for one machine/store pair, outermost first:
    /// total timeout, then retry, then the breaker, then the per-attempt timeout. Retries
    /// only transient store failures (unavailability, optimistic conflicts) and attempt
    /// timeouts; the breaker counts only dependency outages and attempt timeouts. Caller
    /// cancellation is never retried. Build once and share: the breaker's state belongs to
    /// the pipeline. Pass <c>ignore</c> when no event sink is wanted.
    /// </summary>
    /// <exception cref="T:System.InvalidOperationException">The configuration is invalid.</exception>
    let toPipeline (config: RetryConfig<'Err>) (onEvent: PipelineEventSink) : ResiliencePipeline<PipelineResult<'Err>> =
        match validate config with
        | Error errors -> invalidOp $"cannot build a retry pipeline from an invalid configuration: {errors}"
        | Ok config ->
            let notify event =
                // Telemetry is explicitly best effort. A broken observer must not alter
                // retry, timeout, or circuit-breaker behavior.
                TaskOutcome.captureSync (fun () -> onEvent event) |> ignore

            /// Transient outcomes the retry strategy may repeat: infrastructure
            /// unavailability, optimistic concurrency conflicts, and attempt timeouts.
            let isTransient (error: MachineError<'Err>) =
                match error with
                | Store(Unavailable _)
                | Store(Concurrency _) -> true
                | _ -> false

            /// Failures the breaker counts as dependency health: outages and attempt
            /// timeouts only. Domain rejections and optimistic conflicts are not the
            /// store's fault and must not open the circuit.
            let isOutage (error: MachineError<'Err>) =
                match error with
                | Store(Unavailable _) -> true
                | _ -> false

            let handles (outcome: Outcome<PipelineResult<'Err>>) (classify: MachineError<'Err> -> bool) : bool =
                match outcome.Exception with
                | null ->
                    match outcome.Result with
                    | Error error -> classify error
                    | Ok _ -> false
                | :? TimeoutRejectedException -> true
                | _ -> false

            let retry = RetryStrategyOptions<PipelineResult<'Err>>()

            retry.ShouldHandle <-
                fun (args: RetryPredicateArguments<PipelineResult<'Err>>) ->
                    ValueTask<bool>(handles args.Outcome isTransient)

            retry.OnRetry <-
                fun (args: OnRetryArguments<PipelineResult<'Err>>) ->
                    notify (RetryScheduled(args.AttemptNumber + 1, args.RetryDelay))
                    ValueTask()

            retry.MaxRetryAttempts <- config.MaxAttempts - 1
            retry.BackoffType <- config.Backoff
            retry.Delay <- config.BaseDelay
            retry.MaxDelay <- config.MaxDelay
            retry.UseJitter <- config.UseJitter

            let totalTimeout = TimeoutStrategyOptions(Timeout = config.TotalTimeout)

            totalTimeout.OnTimeout <-
                fun (args: OnTimeoutArguments) ->
                    notify (TotalTimedOut args.Timeout)
                    ValueTask()

            let attemptTimeout = TimeoutStrategyOptions(Timeout = config.AttemptTimeout)

            attemptTimeout.OnTimeout <-
                fun (args: OnTimeoutArguments) ->
                    notify (AttemptTimedOut args.Timeout)
                    ValueTask()

            let builder = ResiliencePipelineBuilder<PipelineResult<'Err>>()
            builder.AddTimeout(totalTimeout) |> ignore

            if config.MaxAttempts > 1 then
                builder.AddRetry(retry) |> ignore

            config.Breaker
            |> Option.iter (fun breakerConfig ->
                let breaker = CircuitBreakerStrategyOptions<PipelineResult<'Err>>()

                breaker.ShouldHandle <-
                    fun (args: CircuitBreakerPredicateArguments<PipelineResult<'Err>>) ->
                        ValueTask<bool>(handles args.Outcome isOutage)

                breaker.OnOpened <-
                    fun (args: OnCircuitOpenedArguments<PipelineResult<'Err>>) ->
                        notify (CircuitOpened args.BreakDuration)
                        ValueTask()

                breaker.OnClosed <-
                    fun (_: OnCircuitClosedArguments<PipelineResult<'Err>>) ->
                        notify CircuitClosed
                        ValueTask()

                breaker.FailureRatio <- breakerConfig.FailureRatio
                breaker.MinimumThroughput <- breakerConfig.MinimumThroughput
                breaker.SamplingDuration <- breakerConfig.SamplingDuration
                breaker.BreakDuration <- breakerConfig.BreakDuration
                builder.AddCircuitBreaker(breaker) |> ignore)

            builder.AddTimeout(attemptTimeout) |> ignore
            builder.Build()

/// <summary>Typed execution against a pipeline built by <c>RetryConfig.toPipeline</c>.</summary>
[<RequireQualifiedAccess>]
module Retry =

    /// <summary>
    /// Runs one operation under the pipeline and normalises every strategy signal into
    /// <see cref="T:ByzantineSystems.Automata.Core.MachineError`1" /> values: attempt and
    /// total timeouts become <c>Timeout</c> carrying the budget that fired, and an open
    /// breaker becomes <c>CircuitOpen</c> with its retry hint. Polly's <c>ValueTask</c>
    /// interop and pooled contexts stay private to this module. Caller cancellation
    /// propagates as cancellation and is never converted into an error. An unexpected
    /// exception from the operation propagates unchanged; classifying those is the
    /// runtime supervisor's job, not the pipeline's.
    /// </summary>
    let execute
        (pipeline: ResiliencePipeline<PipelineResult<'Err>>)
        (operation: CancellationToken -> Task<PipelineResult<'Err>>)
        (ct: CancellationToken)
        : Task<PipelineResult<'Err>> =
        task {
            let context = ResilienceContextPool.Shared.Get(ct)

            let callback (ctx: ResilienceContext) (_: obj) : ValueTask<Outcome<PipelineResult<'Err>>> =
                ValueTask<Outcome<PipelineResult<'Err>>>(
                    task {
                        let! operationOutcome = operation ctx.CancellationToken |> TaskOutcome.capture

                        return
                            match operationOutcome with
                            | Ok result -> Outcome.FromResult result
                            | Error error -> Outcome.FromException<PipelineResult<'Err>>(error)
                    }
                )

            let! execution =
                pipeline.ExecuteOutcomeAsync(callback, context, null).AsTask()
                |> TaskOutcome.capture

            // Return the pooled context before propagating any unexpected pipeline fault.
            ResilienceContextPool.Shared.Return(context)

            match execution with
            | Error(CanceledBy ct) -> return! Task.FromCanceled<PipelineResult<'Err>>(ct)
            | Error unexpected -> return raise unexpected
            | Ok pipelineOutcome ->
                match pipelineOutcome.Exception with
                | null -> return pipelineOutcome.Result
                | :? TimeoutRejectedException as timeout -> return Error(MachineError.Timeout timeout.Timeout)
                | :? BrokenCircuitException as broken ->
                    let retryAfter =
                        if broken.RetryAfter.HasValue then
                            Some broken.RetryAfter.Value
                        else
                            None

                    return Error(CircuitOpen retryAfter)
                | CanceledBy ct -> return! Task.FromCanceled<PipelineResult<'Err>>(ct)
                | unexpected -> return raise unexpected
        }

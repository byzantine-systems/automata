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
/// Circuit-breaker parameters. All four are required when a breaker is configured:
/// Polly would otherwise use defaults unrelated to this workload.
/// </summary>
type BreakerConfig =
    { FailureRatio: double
      MinimumThroughput: int
      SamplingDuration: TimeSpan
      BreakDuration: TimeSpan }

/// <summary>Structural defects in a <see cref="T:ByzantineSystems.Automata.Resilience.TransientPolicy" />.</summary>
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
/// Short-horizon resilience for the I/O one machine performs: a socket that dropped, a
/// database that is restarting, an attempt that overran its budget. Milliseconds to seconds.
///
/// Durable retry is not here and must not be. A command that fails is rescheduled in the
/// inbox, with a backoff the database computes, and it keeps its place in its entity's order
/// while it waits. That is a different mechanism with a different horizon, and the two were
/// previously entangled in one record: this type used to carry a <c>Classify</c> field
/// deciding a failed send's final disposition, which is a domain decision the command
/// processor owns.
///
/// <c>MaxAttempts</c> counts total executions including the first; Polly's
/// <c>MaxRetryAttempts</c> means retries after the first call, so the pipeline is configured
/// with <c>MaxAttempts - 1</c>.
/// </summary>
type TransientPolicy =
    { MaxAttempts: int
      BaseDelay: TimeSpan
      MaxDelay: TimeSpan
      Backoff: DelayBackoffType
      UseJitter: bool
      AttemptTimeout: TimeSpan
      TotalTimeout: TimeSpan
      Breaker: BreakerConfig option }

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Resilience.TransientPolicy" />.</summary>
[<RequireQualifiedAccess>]
module TransientPolicy =

    /// <summary>Three attempts, jittered exponential backoff, no breaker.</summary>
    let defaults: TransientPolicy =
        { MaxAttempts = 3
          BaseDelay = TimeSpan.FromMilliseconds 200.
          MaxDelay = TimeSpan.FromSeconds 30.
          Backoff = DelayBackoffType.Exponential
          UseJitter = true
          AttemptTimeout = TimeSpan.FromSeconds 5.
          TotalTimeout = TimeSpan.FromSeconds 30.
          Breaker = None }

    /// <summary>Accumulates every structural defect rather than stopping at the first.</summary>
    let validate (policy: TransientPolicy) : Result<TransientPolicy, ConfigError list> =
        let minimumTimeout = TimeSpan.FromMilliseconds 10.
        let minimumBreakerDuration = TimeSpan.FromMilliseconds 500.
        let maximumDuration = TimeSpan.FromDays 1.

        let retryErrors =
            [ if policy.MaxAttempts < 1 then
                  MaxAttemptsBelowOne policy.MaxAttempts

              if policy.BaseDelay < TimeSpan.Zero then
                  NegativeBaseDelay policy.BaseDelay

              if policy.MaxDelay < policy.BaseDelay then
                  MaxDelayBelowBaseDelay(policy.BaseDelay, policy.MaxDelay)

              if policy.AttemptTimeout <= TimeSpan.Zero then
                  AttemptTimeoutNotPositive policy.AttemptTimeout
              elif
                  policy.AttemptTimeout < minimumTimeout
                  || policy.AttemptTimeout > maximumDuration
              then
                  AttemptTimeoutOutOfRange policy.AttemptTimeout

              if policy.TotalTimeout <= TimeSpan.Zero then
                  TotalTimeoutNotPositive policy.TotalTimeout
              else
                  if policy.TotalTimeout < minimumTimeout || policy.TotalTimeout > maximumDuration then
                      TotalTimeoutOutOfRange policy.TotalTimeout

                  if policy.TotalTimeout < policy.AttemptTimeout then
                      TotalTimeoutBelowAttemptTimeout(policy.TotalTimeout, policy.AttemptTimeout) ]

        let breakerErrors =
            policy.Breaker
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
        | [] -> Ok policy
        | errors -> Error errors


    /// <summary>
    /// Builds the shared pipeline for one store, outermost first: total timeout, then retry,
    /// then the breaker, then the per-attempt timeout.
    ///
    /// The pipeline belongs around the driver call, not around a function that already answers
    /// with <c>Result</c>: an <c>Error</c> is a perfectly successful return as far as Polly is
    /// concerned, so a pipeline wrapped outside the conversion retries nothing. That is why
    /// this is not generic. It runs inside the store adapter, where failure is still an
    /// exception, and the adapter converts afterwards.
    ///
    /// <paramref name="isTransient" /> names the driver failures worth repeating. It is a
    /// parameter because only the store knows which of its exceptions mean "again in a moment"
    /// and which mean "never": this assembly deliberately has no reference to any driver. An
    /// attempt timeout is always retried, and only <paramref name="isTransient" /> opens the
    /// breaker, because a call that overran its own budget says something about the call while
    /// an unreachable dependency says something about the dependency.
    ///
    /// Build once and share: the breaker's state belongs to the pipeline, and one rebuilt per
    /// call has a circuit that can never break. Pass <c>ignore</c> when no event sink is
    /// wanted.
    /// </summary>
    /// <exception cref="T:System.InvalidOperationException">The policy is invalid.</exception>
    let toPipeline
        (policy: TransientPolicy)
        (name: string)
        (isTransient: exn -> bool)
        (onEvent: PipelineEventSink)
        : ResiliencePipeline =
        match validate policy with
        | Error errors -> invalidOp $"cannot build a resilience pipeline from an invalid policy: {errors}"
        | Ok policy ->
            let notify event =
                // Telemetry is explicitly best effort. A broken observer must not alter
                // retry, timeout, or circuit-breaker behavior.
                TaskOutcome.captureSync (fun () -> onEvent event) |> ignore

            // The strategies throw two of these themselves, so both need an answer here rather
            // than falling through to the caller's classifier, which has never heard of them.
            let retryable (error: exn) =
                match error with
                // An attempt that overran its own budget: the next one gets a fresh budget.
                | :? TimeoutRejectedException -> true
                // The breaker sits inside retry, so an open circuit arrives here. Repeating
                // against it would burn every remaining attempt on a call that is refused
                // before it is made, and the breaker reopens on its own schedule regardless.
                | :? BrokenCircuitException -> false
                | :? OperationCanceledException -> false
                | other -> isTransient other

            let opensCircuit (error: exn) =
                match error with
                | :? OperationCanceledException -> false
                | other -> isTransient other

            let retry = RetryStrategyOptions()

            retry.ShouldHandle <-
                fun (args: RetryPredicateArguments<obj>) -> ValueTask<bool>(retryable args.Outcome.Exception)

            retry.OnRetry <-
                fun (args: OnRetryArguments<obj>) ->
                    notify (RetryScheduled(args.AttemptNumber + 1, args.RetryDelay))
                    ValueTask()

            retry.MaxRetryAttempts <- policy.MaxAttempts - 1
            retry.BackoffType <- policy.Backoff
            retry.Delay <- policy.BaseDelay
            retry.MaxDelay <- policy.MaxDelay
            retry.UseJitter <- policy.UseJitter

            let totalTimeout = TimeoutStrategyOptions(Timeout = policy.TotalTimeout)

            totalTimeout.OnTimeout <-
                fun (args: OnTimeoutArguments) ->
                    notify (TotalTimedOut args.Timeout)
                    ValueTask()

            let attemptTimeout = TimeoutStrategyOptions(Timeout = policy.AttemptTimeout)

            attemptTimeout.OnTimeout <-
                fun (args: OnTimeoutArguments) ->
                    notify (AttemptTimedOut args.Timeout)
                    ValueTask()

            // Named so it is identifiable in Polly's telemetry, where an unnamed pipeline is
            // indistinguishable from every other unnamed pipeline in the process.
            let builder = ResiliencePipelineBuilder(Name = name)

            // Ordering is the whole meaning of a pipeline, and the first strategy added is the
            // outermost. Total timeout covers every attempt and the sleeps between them; retry
            // sits outside the breaker so the breaker counts individual call failures and,
            // once open, stops the retry hammering a database that is already down; the
            // per-attempt timeout is innermost and is what makes a hung call retryable.
            builder.AddTimeout(totalTimeout) |> ignore

            if policy.MaxAttempts > 1 then
                builder.AddRetry(retry) |> ignore

            policy.Breaker
            |> Option.iter (fun breakerConfig ->
                let breaker = CircuitBreakerStrategyOptions()

                breaker.ShouldHandle <-
                    fun (args: CircuitBreakerPredicateArguments<obj>) ->
                        ValueTask<bool>(opensCircuit args.Outcome.Exception)

                breaker.OnOpened <-
                    fun (args: OnCircuitOpenedArguments<obj>) ->
                        notify (CircuitOpened args.BreakDuration)
                        ValueTask()

                breaker.OnClosed <-
                    fun (_: OnCircuitClosedArguments<obj>) ->
                        notify CircuitClosed
                        ValueTask()

                breaker.FailureRatio <- breakerConfig.FailureRatio
                breaker.MinimumThroughput <- breakerConfig.MinimumThroughput
                breaker.SamplingDuration <- breakerConfig.SamplingDuration
                breaker.BreakDuration <- breakerConfig.BreakDuration
                builder.AddCircuitBreaker(breaker) |> ignore)

            builder.AddTimeout(attemptTimeout) |> ignore
            builder.Build()

/// <summary>
/// The <c>ValueTask</c> interop Polly's fluent API requires, kept in one place rather than at
/// every call site.
/// </summary>
[<RequireQualifiedAccess>]
module ResiliencePipeline =

    /// <summary>
    /// Runs a <c>Task</c>-returning function through a pipeline and hands back a <c>Task</c>.
    ///
    /// The function receives the token the pipeline supplies, never one captured from outside:
    /// that token is how the attempt timeout cancels a call that has overrun, so a callback
    /// ignoring it cannot be timed out.
    /// </summary>
    let executeTask
        (pipeline: ResiliencePipeline)
        (work: CancellationToken -> Task<'T>)
        (ct: CancellationToken)
        : Task<'T> =
        pipeline.ExecuteAsync((fun token -> ValueTask<'T>(work token)), ct).AsTask()

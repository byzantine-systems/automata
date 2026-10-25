module ByzantineSystems.Automata.Storage.Tests.ArgumentTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto

let private bounded count value = int (uint32 value % uint32 count)

let private bounded64 count value = int64 (uint64 value % uint64 count)

let private throwsArgument action =
    try
        action ()
        false
    with :? ArgumentException ->
        true

let private claimBatchLaw raw =
    let valid = raw >= 1 && raw <= 1000

    match ClaimBatch.tryCreate raw with
    | Ok batch ->
        valid
        && ClaimBatch.value batch = raw
        && ClaimBatch.value (ClaimBatch.create raw) = raw
    | Error _ -> not valid && throwsArgument (fun () -> ClaimBatch.create raw |> ignore)

let private leaseDurationLaw raw =
    let valid = raw > TimeSpan.Zero && raw <= LeaseDuration.maximum

    match LeaseDuration.tryCreate raw with
    | Ok lease ->
        valid
        && LeaseDuration.value lease = raw
        && LeaseDuration.value (LeaseDuration.create raw) = raw
    | Error _ -> not valid && throwsArgument (fun () -> LeaseDuration.create raw |> ignore)

let private reasonLaw raw =
    let valid = not (String.IsNullOrWhiteSpace raw)

    match Reason.tryCreate raw with
    | Ok reason ->
        valid
        && Reason.value reason = raw.Trim()
        && Reason.value (Reason.create raw) = raw.Trim()
    | Error _ -> not valid && throwsArgument (fun () -> Reason.create raw |> ignore)

let private tryPage (build: unit -> Page) : Result<Page, unit> =
    try
        Ok(build ())
    with :? ArgumentException ->
        Error()

let private pageLaw epoch n =
    let valid = n >= 1

    let check build cursor =
        match tryPage build with
        | Ok page -> valid && Page.limit page = n && Page.cursor page = cursor
        | Error _ -> not valid && throwsArgument (fun () -> build () |> ignore)

    check (fun () -> Page.create n) None
    && check (fun () -> Page.after epoch n) (Some epoch)

let private backoffLaw (baseDelay, ceiling) =
    let valid =
        baseDelay > TimeSpan.Zero
        && ceiling >= baseDelay
        && ceiling <= LeaseDuration.maximum

    match Backoff.tryCreate baseDelay ceiling with
    | Ok backoff ->
        valid
        && Backoff.baseDelay backoff = baseDelay
        && Backoff.ceiling backoff = ceiling
        && Backoff.create baseDelay ceiling = backoff
    | Error _ ->
        not valid
        && throwsArgument (fun () -> Backoff.create baseDelay ceiling |> ignore)

[<Tests>]
let tests =
    testList
        "constrained arguments"
        [ Property.property "ClaimBatch accepts exactly 1 through 1000 and round-trips" (fun (seed: int) ->
              [ Int32.MinValue
                -1
                0
                1
                1000
                1001
                Int32.MaxValue
                seed
                1 + bounded 1000 seed ]
              |> List.forall claimBatchLaw)

          Property.property
              "LeaseDuration accepts exactly the positive provider-safe range and round-trips"
              (fun (seed: int64) ->
                  let maximumTicks = LeaseDuration.maximum.Ticks
                  let generatedValid = TimeSpan.FromTicks(1L + bounded64 maximumTicks seed)

                  [ TimeSpan.MinValue
                    TimeSpan.FromTicks -1L
                    TimeSpan.Zero
                    TimeSpan.FromTicks 1L
                    LeaseDuration.maximum - TimeSpan.FromTicks 1L
                    LeaseDuration.maximum
                    LeaseDuration.maximum + TimeSpan.FromTicks 1L
                    TimeSpan.MaxValue
                    TimeSpan.FromTicks seed
                    generatedValid ]
                  |> List.forall leaseDurationLaw)

          Property.property "Reason accepts exactly nonblank text and observes trimming" (fun (raw: string) ->
              [ null; ""; " "; "\t\r\n"; raw; $"  %s{raw}  " ] |> List.forall reasonLaw)

          Property.property "Backoff accepts exactly ordered provider-safe envelopes" (fun (a: int64, b: int64) ->
              let maximumTicks = LeaseDuration.maximum.Ticks
              let baseTicks = 1L + bounded64 maximumTicks a
              let ceilingTicks = baseTicks + bounded64 (maximumTicks - baseTicks + 1L) b

              [ TimeSpan.Zero, TimeSpan.FromTicks 1L
                TimeSpan.FromTicks 1L, TimeSpan.Zero
                TimeSpan.FromTicks 1L, LeaseDuration.maximum
                TimeSpan.FromTicks 1L, LeaseDuration.maximum + TimeSpan.FromTicks 1L
                LeaseDuration.maximum, LeaseDuration.maximum
                LeaseDuration.maximum + TimeSpan.FromTicks 1L, LeaseDuration.maximum + TimeSpan.FromTicks 1L
                TimeSpan.FromTicks a, TimeSpan.FromTicks b
                TimeSpan.FromTicks baseTicks, TimeSpan.FromTicks ceilingTicks ]
              |> List.forall backoffLaw)

          Property.property "Page accepts exactly positive limits and observes them" (fun (seed: int) ->
              let epoch = Epoch.ofUInt64 (uint64 (bounded 1000 seed) + 1UL)

              [ Int32.MinValue; -1; 0; 1; 2; seed; Int32.MaxValue ]
              |> List.forall (pageLaw epoch)) ]

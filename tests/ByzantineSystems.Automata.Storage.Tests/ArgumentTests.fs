module ByzantineSystems.Automata.Storage.Tests.ArgumentTests

open System
open ByzantineSystems.Automata.Storage
open Expecto

let private expectError result message =
    Expect.isTrue (Result.isError result) message

[<Tests>]
let tests =
    testList
        "constrained arguments"
        [ testCase "claim batches accept only the inclusive supported range"
          <| fun _ ->
              expectError (ClaimBatch.tryCreate -1) "negative batches are rejected"
              expectError (ClaimBatch.tryCreate 0) "empty batches are rejected"
              Expect.equal (ClaimBatch.create 1 |> ClaimBatch.value) 1 "the lower bound is accepted"
              Expect.equal (ClaimBatch.create 1000 |> ClaimBatch.value) 1000 "the upper bound is accepted"
              expectError (ClaimBatch.tryCreate 1001) "batches above the upper bound are rejected"

          testCase "claim batch create rejects invalid input"
          <| fun _ ->
              Expect.throwsT<ArgumentException>
                  (fun () -> ClaimBatch.create 0 |> ignore)
                  "the throwing constructor reports invalid input"

          testCase "lease durations accept only the inclusive supported range"
          <| fun _ ->
              expectError (LeaseDuration.tryCreate (TimeSpan.FromTicks -1L)) "negative durations are rejected"
              expectError (LeaseDuration.tryCreate TimeSpan.Zero) "zero durations are rejected"

              Expect.equal
                  (LeaseDuration.create (TimeSpan.FromTicks 1L) |> LeaseDuration.value)
                  (TimeSpan.FromTicks 1L)
                  "the smallest positive duration is accepted"

              Expect.equal
                  (LeaseDuration.create LeaseDuration.maximum |> LeaseDuration.value)
                  LeaseDuration.maximum
                  "the upper bound is accepted"

              expectError
                  (LeaseDuration.tryCreate (LeaseDuration.maximum + TimeSpan.FromTicks 1L))
                  "durations above the upper bound are rejected"

          testCase "lease duration create rejects invalid input"
          <| fun _ ->
              Expect.throwsT<ArgumentException>
                  (fun () -> LeaseDuration.create TimeSpan.Zero |> ignore)
                  "the throwing constructor reports invalid input"

          testCase "reasons reject blank text and trim accepted text"
          <| fun _ ->
              expectError (Reason.tryCreate null) "null reasons are rejected"
              expectError (Reason.tryCreate "") "empty reasons are rejected"
              expectError (Reason.tryCreate "   ") "whitespace reasons are rejected"
              Expect.equal (Reason.create "  failed  " |> Reason.value) "failed" "accepted reasons are trimmed"

          testCase "reason create rejects invalid input"
          <| fun _ ->
              Expect.throwsT<ArgumentException>
                  (fun () -> Reason.create " " |> ignore)
                  "the throwing constructor reports invalid input"

          testCase "backoff ceilings fit the provider millisecond range"
          <| fun _ ->
              Expect.isOk
                  (Backoff.tryCreate (TimeSpan.FromTicks 1L) LeaseDuration.maximum)
                  "the upper bound is accepted"

              expectError
                  (Backoff.tryCreate (TimeSpan.FromTicks 1L) (LeaseDuration.maximum + TimeSpan.FromTicks 1L))
                  "ceilings above the upper bound are rejected"

              expectError
                  (Backoff.tryCreate (TimeSpan.FromSeconds 2.0) (TimeSpan.FromSeconds 1.0))
                  "the existing envelope ordering remains enforced" ]

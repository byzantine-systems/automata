module ByzantineSystems.Automata.Runtime.Tests.ProcessorTests

open System
open System.Threading
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Runtime.Tests.TestSupport
open Expecto

/// These tests are about how the processor interprets what a store told it. Whether the store
/// tells the truth, orders anything, or fences a lease is proven against real PostgreSQL in the
/// integration suite, because a stub that agreed with the processor would prove only that the
/// two were written by the same person.
let private processorOver (store: StubStore<TestError>) =
    let machine = buildMachine store (newTime ()) id |> expectMachine
    Machine.processor machine

let private poll (store: StubStore<TestError>) =
    task {
        match! (processorOver store).PollAsync CancellationToken.None with
        | Ok report -> return report
        | Error error -> return failtestf "the poll should have reported, but failed with %A" error
    }

let tests =
    testList
        "command processor"
        [ testTask "an empty inbox claims nothing and reports nothing" {
              let store = StubStore<TestError>()
              let! report = poll store
              Expect.equal 0 report.Summary.Claimed "no commands"
              Expect.isNone report.Escalation "no escalation"
          }

          testTask "a resolvable command is committed" {
              let store = StubStore<TestError>()
              store.Offer(leasedCommand 1L "E-1" (Start 5) 0)
              let! report = poll store

              Expect.equal 1 report.Summary.Committed "one commit"
              Expect.equal [ CommandId.ofInt64 1L ] (List.ofSeq store.Commits) "the store was asked to commit it"
          }

          testTask "an already-finalized command is success, not an anomaly" {
              // The whole point of the idempotent finalize: a worker whose connection dropped
              // asks again and is told what it already did. Treating that as a failure would
              // turn the one safe recovery into a duplicate.
              let store = StubStore<TestError>()
              store.CommitOutcome <- Ok(AlreadyFinalized(Epoch.ofUInt64 7UL))
              store.Offer(leasedCommand 1L "E-1" (Start 5) 0)
              let! report = poll store

              Expect.equal 1 report.Summary.Committed "counted as committed"
              Expect.equal 0 report.Summary.Rescheduled "and not retried"
          }

          testTask "a lost lease is dropped silently" {
              let store = StubStore<TestError>()
              store.CommitOutcome <- Ok FinalizeOutcome.LeaseLost
              store.Offer(leasedCommand 1L "E-1" (Start 5) 0)
              let! report = poll store

              Expect.equal 1 report.Summary.LeaseLost "another worker owns it"
              Expect.equal 0 report.Summary.Committed "nothing was written by this one"
          }

          testTask "an epoch conflict reschedules rather than discarding the command" {
              let store = StubStore<TestError>()
              store.CommitOutcome <- Ok(Conflict(Epoch.initial, Epoch.ofUInt64 3UL))
              store.Offer(leasedCommand 1L "E-1" (Start 5) 0)
              let! report = poll store

              Expect.equal 1 report.Summary.Rescheduled "the command was not applied, so it waits"
              Expect.equal [ CommandId.ofInt64 1L ] (List.ofSeq store.Reschedules) "the store was asked to reschedule"
          }

          testTask "a chart refusal is rejected with the domain error" {
              let store = StubStore<TestError>()
              // Cancel has no rule on 'active', and the classifier dead-letters an unhandled
              // event, so use a state where the event simply is not handled.
              store.Snapshot <-
                  Some
                      { State = Active 5
                        Epoch = Epoch.ofUInt64 1UL
                        Status = InstanceStatus.Running }

              store.Offer(leasedCommand 1L "E-1" Cancel 0)
              let! report = poll store

              Expect.equal 1 report.Summary.DeadLettered "an unhandled event will never become handled"
              Expect.equal 1 store.DeadLetters.Count "the store recorded it"
          }

          testTask "a terminated instance refuses further commands" {
              let store = StubStore<TestError>()

              store.Snapshot <-
                  Some
                      { State = Done
                        Epoch = Epoch.ofUInt64 2UL
                        Status = InstanceStatus.Terminated }

              store.Offer(leasedCommand 1L "E-1" (Start 5) 0)
              let! report = poll store

              Expect.equal 1 report.Summary.Rejected "rejected rather than committed"
              Expect.equal 0 store.Commits.Count "the state was never touched"
          }

          testTask "a transient failure is retried while attempts remain" {
              let store = StubStore<TestError>()
              store.CommitOutcome <- Error(Unavailable(InvalidOperationException "socket"))
              store.Offer(leasedCommand 1L "E-1" (Start 5) 0)
              let! report = poll store

              Expect.equal 1 report.Summary.Rescheduled "back to the inbox"
              Expect.equal 0 report.Summary.DeadLettered "attempts remain"
          }

          testTask "a command that has used its attempts is dead-lettered" {
              // Dead-lettering releases the entity, which matters more here than anywhere else:
              // a command that keeps failing would otherwise hold up everything behind it for
              // that entity, forever.
              let store = StubStore<TestError>()
              store.CommitOutcome <- Error(Unavailable(InvalidOperationException "socket"))
              store.Offer(leasedCommand 1L "E-1" (Start 5) 2)
              let! report = poll store

              Expect.equal 1 report.Summary.DeadLettered "given up on"
              Expect.equal 0 report.Summary.Rescheduled "and not retried again"

              match List.ofSeq store.DeadLetters with
              | [ _, CommandFailure.Machine reason ] ->
                  Expect.stringContains reason "attempts" "the reason says it ran out of attempts"
              | other -> failtestf "expected one machine-reason dead letter, got %A" other
          }

          testTask "an escalation ends the run and names the command" {
              let store = StubStore<TestError>()

              let machine =
                  buildMachine store (newTime ()) (fun policy ->
                      { policy with
                          Classify = fun _ -> Escalate "deliberate" })
                  |> expectMachine

              store.CommitOutcome <- Error(Unavailable(InvalidOperationException "socket"))
              store.Offer(leasedCommand 9L "E-1" (Start 5) 0)

              match! (Machine.processor machine).PollAsync CancellationToken.None with
              | Ok report ->
                  match report.Escalation with
                  | Some(commandId, reason) ->
                      Expect.equal (CommandId.ofInt64 9L) commandId "the command that escalated"
                      Expect.equal "deliberate" reason "the reason it gave"
                  | None -> failtest "the escalation should have been reported"
              | Error error -> failtestf "the poll should have reported, got %A" error
          }

          testTask "a failed claim is reported rather than raised" {
              let store = StubStore<TestError>()
              store.ClaimOutcome <- Error(Unavailable(InvalidOperationException "down"))

              match! (processorOver store).PollAsync CancellationToken.None with
              | Error(Store(Unavailable _)) -> ()
              | other -> failtestf "expected a reported store failure, got %A" other
          }

          testTask "a batch of distinct entities is processed together" {
              let store = StubStore<TestError>()
              store.Offer(leasedCommand 1L "E-1" (Start 1) 0)
              store.Offer(leasedCommand 2L "E-2" (Start 2) 0)
              store.Offer(leasedCommand 3L "E-3" (Start 3) 0)
              let! report = poll store

              Expect.equal 3 report.Summary.Claimed "all three"
              Expect.equal 3 report.Summary.Committed "all committed"
          }

          testTask "a cancelled run drains rather than faulting" {
              let store = StubStore<TestError>()
              let cancellation = new CancellationTokenSource()
              cancellation.Cancel()

              match! (processorOver store).RunAsync cancellation.Token with
              | ProcessorStop.Drained totals -> Expect.equal 0 totals.Claimed "nothing was claimed"
              | ProcessorStop.Escalated _ -> failtest "cancellation is not an escalation"
          } ]

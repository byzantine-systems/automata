module ByzantineSystems.Automata.Runtime.Tests.DispositionTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Runtime
open ByzantineSystems.Automata.Runtime.Tests.TestSupport
open Expecto

let private classify (error: MachineError<TestError>) = ProcessorPolicy.defaultClassify error

let tests =
    testList
        "disposition"
        [ test "an unreachable store is worth another attempt" {
              Expect.equal Retry (classify (Store(Unavailable(InvalidOperationException "socket")))) "transient"
          }
          test "a timeout is worth another attempt" {
              Expect.equal Retry (classify (Timeout(TimeSpan.FromSeconds 1.))) "transient"
          }
          test "an open circuit is worth another attempt" {
              Expect.equal Retry (classify (CircuitOpen None)) "transient"
          }
          test "a chart's own refusal is recorded in the application's vocabulary" {
              let refused = Refused "amount too large"

              Expect.equal
                  (Reject(CommandFailure.Domain refused))
                  (classify (Transition(TransitionError.Rejected refused)))
                  "the domain error travels through as data"
          }
          test "an unhandled event is dead-lettered rather than retried" {
              // Waiting will not give the chart a rule it does not have, and retrying forever
              // would hold up every later command for that entity.
              match classify (Transition(Unhandled(stateId "idle", "Cancel"))) with
              | DeadLetter(CommandFailure.Machine reason) ->
                  Expect.stringContains reason "idle" "the reason names the state that had no rule"
              | other -> failtestf "expected a machine-reason dead letter, got %A" other
          }
          test "a corrupt row is dead-lettered with a machine reason" {
              // There is no 'Err for "this row will not decode", and inventing one would be a
              // lie told in the application's own vocabulary.
              match classify (Store(Serialization("TestEvent", InvalidOperationException "bad json"))) with
              | DeadLetter(CommandFailure.Machine reason) ->
                  Expect.stringContains reason "TestEvent" "the reason names what would not read"
              | other -> failtestf "expected a machine-reason dead letter, got %A" other
          }
          test "a guard refusal is a rejection, not a dead letter" {
              match classify (Transition(GuardFailed(stateId "active", "not authorised"))) with
              | Reject(CommandFailure.Machine reason) ->
                  Expect.stringContains reason "not authorised" "the guard's own reason survives"
              | other -> failtestf "expected a machine-reason rejection, got %A" other
          }
          test "a terminated instance refuses further commands without escalating" {
              match classify (MachineError.Rejected(MachineRejection.InstanceNotRunning InstanceStatus.Terminated)) with
              | Reject _ -> ()
              | other -> failtestf "expected a rejection, got %A" other
          }
          test "a stopped machine is this worker's problem, not the command's" {
              match classify (MachineError.Rejected MachineRejection.Stopping) with
              | Escalate _ -> ()
              | other -> failtestf "expected an escalation, got %A" other
          } ]

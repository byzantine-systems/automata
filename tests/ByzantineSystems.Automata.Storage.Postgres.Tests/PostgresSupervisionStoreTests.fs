module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresSupervisionStoreTests

open System
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open TestContext

let private record child kind strategy reason at =
    { Supervisor = SupervisorName.create "orders"
      ChildId = SupervisedChildId.create child
      Kind = kind
      Strategy = strategy
      Reason = reason
      At = at }

let tests =
    testList
        "Postgres ISupervisionEventStore"
        [ testTask "record appends facts and listRecent returns the newest limited page" {
              do! reset ()
              let store = PostgresSupervisionStore(context ()) :> ISupervisionEventStore

              let started =
                  record
                      "payment-pump"
                      SupervisionAuditKind.Started
                      SupervisionAuditStrategy.OneForOne
                      "initial start"
                      startTime

              let restarted =
                  record
                      "payment-pump"
                      SupervisionAuditKind.Restarted
                      SupervisionAuditStrategy.RestForOne
                      "worker crashed: connection lost"
                      (startTime.AddSeconds 1.0)

              do! store.Record(started, noCancellation) |> mapTask (expectOkUnit "record started")

              do!
                  store.Record(restarted, noCancellation)
                  |> mapTask (expectOkUnit "record restarted")

              let! result = PostgresSupervisionQueries.listRecent (context ()) 1 noCancellation
              let actual = result |> expectOk "list recent"

              Expect.equal actual [ restarted ] "the query should preserve the typed record and apply its limit"
          } ]

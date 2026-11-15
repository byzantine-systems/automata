module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresSupervisionStoreTests

open System
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open FsCheck
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
        [ testCase "all audit constructors preserve text and equal-time insertion order" (fun () ->
              Check.One(
                  Config.QuickThrowOnFailure.WithMaxTest(12),
                  fun (text: string) ->
                      task {
                          do! reset ()
                          let store = PostgresSupervisionStore(context ()) :> ISupervisionEventStore
                          // PostgreSQL text excludes U+0000. Other representable Unicode is preserved.
                          let text = if isNull text then "" else text.Replace("\u0000", "")

                          let entries =
                              [ for kind in
                                    [ SupervisionAuditKind.Started
                                      SupervisionAuditKind.Restarted
                                      SupervisionAuditKind.Escalated
                                      SupervisionAuditKind.Stopped ] do
                                    for strategy in
                                        [ SupervisionAuditStrategy.OneForOne
                                          SupervisionAuditStrategy.OneForAll
                                          SupervisionAuditStrategy.RestForOne ] do
                                        yield record "child" kind strategy text startTime ]

                          for entry in entries do
                              let! saved = store.RecordAsync(entry, noCancellation)
                              expectOkUnit "record audit" saved

                          let! found =
                              PostgresSupervisionQueries.listRecent (context ()) (ClaimBatch.create 7) noCancellation

                          Expect.equal
                              (expectOk "recent" found)
                              (entries |> List.rev |> List.take 7)
                              "text, discriminants, ties, and limit"

                          return true
                      }
                      |> fun running -> running.GetAwaiter().GetResult()
              ))

          testTask "record appends facts and listRecent returns the newest limited page" {
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

              do!
                  store.RecordAsync(started, noCancellation)
                  |> mapTask (expectOkUnit "record started")

              do!
                  store.RecordAsync(restarted, noCancellation)
                  |> mapTask (expectOkUnit "record restarted")

              let! result = PostgresSupervisionQueries.listRecent (context ()) (ClaimBatch.create 1) noCancellation
              let actual = result |> expectOk "list recent"

              Expect.equal actual [ restarted ] "the query should preserve the typed record and apply its limit"
          } ]

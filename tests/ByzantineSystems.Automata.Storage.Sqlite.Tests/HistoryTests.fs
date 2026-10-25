module ByzantineSystems.Automata.Storage.Sqlite.Tests.HistoryTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.ContractProperties
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

type private Committed = CommittedTransition<Entity, TestState, TestEvent, TestAction>

let private entity name : Entity = entityId name
let private effectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)

/// Submits, claims and commits one command, which is the only way a transition gets into the log.
let private commit (fixture: Fixture) (name: string) (key: string) (toState: TestState) : Task =
    task {
        let! held = leased (inboxOf fixture) (entity name) key
        let! snapshot = (readerOf fixture).TryGetSnapshot(testMachine, entity name, noCancellation)

        let expected =
            snapshot
            |> expectOk "snapshot"
            |> Option.map _.Epoch
            |> Option.defaultValue Epoch.initial

        let! outcome =
            (processorOf fixture)
                .Commit(
                    held.Work.CommandId,
                    held.Token,
                    expected,
                    draft held.Work.EntityId Idle toState effectiveAt,
                    noCancellation
                )

        outcome |> expectOk "commit" |> ignore
    }

let private page (fixture: Fixture) (name: string) (paging: Page) : Task<Committed list> =
    task {
        let! rows = (readerOf fixture).History(testMachine, entity name, paging, noCancellation)
        return rows |> expectOk "history"
    }

// The shared history equations, over this provider's fixture: a fresh migrated file per
// generated case, and the fixture's frozen clock, so commit instants are exact rather than
// bounded.
let private contractHistoryEnv () : ContractHistoryEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let db = fixture "contract-history"

        let processor =
            SqliteCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = db.Context
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )

        { Inbox =
            SqliteCommandInbox<ContractEntityId, ContractEvent>(
                { Context = db.Context
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor = processor :> ContractProcessor
          Reader = processor :> ContractReader
          Now = fun () -> db.Clock.GetUtcNow() }

    { Machine = testMachine
      Version = version
      CommittedAtTolerance = TimeSpan.Zero
      Fresh = fun () -> Task.FromResult(stores ()) }

let tests =
    testList
        "IStateReader"
        [ ContractProperties.historyProperty "the shared history equations" 30 (contractHistoryEnv ())

          testTask "the snapshot table holds one row, replaced in place, stamped by the frozen clock" {
              let db = fixture "history"
              do! commit db "e1" "k1" (Active 1)
              do! commit db "e1" "k2" (Active 2)

              let! rows = page db "e1" (Page.create 10)

              Expect.equal (scalar db.Db "SELECT count(*) FROM fsm_entity_snapshot;") "1" "one row, replaced in place"
              Expect.equal (List.last rows).CommittedAt startTime "commit time, from the frozen clock"
          } ]

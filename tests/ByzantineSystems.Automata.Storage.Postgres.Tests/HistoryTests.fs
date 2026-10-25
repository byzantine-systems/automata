module ByzantineSystems.Automata.Storage.Postgres.Tests.HistoryTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Postgres
open ByzantineSystems.Automata.Storage.ContractProperties
open Expecto
open TestContext

// The shared history equations, over this provider: one truncate-and-reseed per generated case,
// and the database's own clock, so commit instants are bounded rather than predicted.
let private contractHistoryEnv () : ContractHistoryEnv =
    let encode, decode = EntityKey.forEntityId<ContractEntity>

    let stores () =
        let processor =
            PostgresCommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>(
                { Context = context ()
                  ActionQueue = actionQueue
                  StateCodec = Serialization.systemTextJson<ContractState> ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  ActionCodec = Serialization.systemTextJson<ContractAction list> ()
                  ErrorCodec = Serialization.systemTextJson<ContractError> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )

        { Inbox =
            PostgresCommandInbox<ContractEntityId, ContractEvent>(
                { Context = context ()
                  EventCodec = Serialization.systemTextJson<ContractEvent> ()
                  EntityIdEncode = encode
                  EntityIdDecode = decode }
            )
            :> ContractInbox
          Processor = processor :> ContractProcessor
          Reader = processor :> ContractReader
          Now = fun () -> DateTimeOffset.UtcNow }

    { Machine = machine
      Version = version
      CommittedAtTolerance = TimeSpan.FromSeconds 5.0
      Fresh =
        fun () ->
            task {
                do! reset ()
                return stores ()
            } }

let tests =
    testList
        "Postgres transition history"
        [ ContractProperties.historyProperty "the shared history equations" 12 (contractHistoryEnv ()) ]

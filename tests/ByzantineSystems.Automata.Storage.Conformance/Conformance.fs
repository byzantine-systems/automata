namespace ByzantineSystems.Automata.Storage

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.ContractProperties
open Expecto

/// <summary>
/// The provider-agnostic conformance suite: every contract rule is a named equation, stated once
/// here and checked against every store through its public interfaces. A provider supplies one
/// <see cref="T:ByzantineSystems.Automata.Storage.Conformance.StoreUnderTest" /> and calls
/// <see cref="M:ByzantineSystems.Automata.Storage.Conformance.tests" />; it keeps only what is
/// schema-, SQL- or file-specific in its own test project.
///
/// Capabilities a store may offer beyond the four required interfaces are not yet represented
/// here, because no equation in the current portfolio is optional: every store implements the
/// inbox, the reader, the processor and the action queue. The first capability-gated equation
/// (for example the PostgreSQL-only correction store) is where a
/// <c>Capabilities: Set&lt;Capability&gt;</c> field and a skip-rather-than-fail branch belong.
/// </summary>
[<RequireQualifiedAccess>]
module Conformance =

    /// Per-provider tuning for the generated properties. PostgreSQL drives the same traces with
    /// fewer cases, because each generated case round-trips the database and its clock.
    type Settings =
        { InboxCases: int
          InboxEndSize: int
          FinalizeCases: int
          RequestIdentityCases: int
          ArrivalOrderCases: int
          PoisonCases: int
          ReadyTokenCases: int
          ClaimExclusivityCases: int
          ActionCases: int
          RegistryCases: int
          HistoryCases: int }

    /// Everything one provider must supply to run the shared suite.
    type StoreUnderTest =
        { Name: string
          Inbox: ContractInboxEnv
          History: ContractHistoryEnv
          Action: ContractActionEnv
          Registry: unit -> Task<IChartRegistry>
          PoisonCommand: int -> int -> Task<PoisonBatchFixture>
          PoisonAction: int -> int -> Task<PoisonBatchFixture>
          Settings: Settings }

    /// The behavior classes the generated inbox traces are expected to reach. Owned here because
    /// the fake and the generator that can reach them live in the shared model, not in a provider.
    let private behaviorClasses =
        [ "resubmit"
          "key-reused"
          "empty-claim"
          "batch-limited"
          "blocked-sibling"
          "delay-not-yet"
          "delay-elapsed"
          "expiry-reclaimed"
          "reject-releases"
          "deadletter-releases"
          "retry-deferred"
          "renew-held"
          "commit-finalized"
          "commit-conflict"
          "commit-already"
          "commit-stale-fenced"
          "terminal-stale-commit"
          "ready-zero-token-fenced"
          "expired-unreclaimed"
          "reject-already"
          "deadletter-already"
          "stale-reject"
          "stale-deadletter"
          "stale-retry"
          "stale-renew"
          "lookup-hit"
          "lookup-miss"
          "result-pending"
          "result-committed"
          "result-rejected"
          "result-deadlettered"
          "result-missing" ]

    /// The whole suite, one named property per equation, as a single Expecto list a provider
    /// composes into its own test assembly.
    let tests (sut: StoreUnderTest) : Test =
        let s = sut.Settings

        testList
            sut.Name
            [ ContractProperties.registryProperty "the registry is an immutable finite map" s.RegistryCases sut.Registry

              ContractProperties.inboxProperty
                  "the inbox follows the shared state machine"
                  s.InboxCases
                  s.InboxEndSize
                  sut.Inbox

              ContractProperties.finalizeProperty "finalization obeys the shared equations" s.FinalizeCases sut.Inbox

              ContractProperties.requestIdentityProperty
                  "idempotency keys name immutable requests"
                  s.RequestIdentityCases
                  sut.Inbox

              ContractProperties.arrivalOrderProperty
                  "arrival time follows sequence order"
                  s.ArrivalOrderCases
                  sut.Inbox

              ContractProperties.poisonBatchProperty
                  "poison commands do not wedge a batch"
                  s.PoisonCases
                  sut.PoisonCommand

              ContractProperties.poisonBatchProperty
                  "poison actions do not wedge a batch"
                  s.PoisonCases
                  sut.PoisonAction

              ContractProperties.readyTokenProperty
                  "a ready command's sentinel token never authorizes finalization"
                  s.ReadyTokenCases
                  sut.Inbox

              ContractProperties.claimExclusivityProperty
                  "concurrent claims never overlap"
                  s.ClaimExclusivityCases
                  sut.Inbox

              ContractProperties.actionQueueProperty
                  "the action queue obeys the shared equations"
                  s.ActionCases
                  sut.Action

              ContractProperties.inboxReachability
                  "the generated inbox traces reach every behavior class"
                  sut.Inbox
                  behaviorClasses

              ContractProperties.historyProperty "the shared history equations" s.HistoryCases sut.History ]

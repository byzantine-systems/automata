namespace ByzantineSystems.Automata.DependencyInjection

open System.Collections.Concurrent
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// Keeps supervision audit records in memory.
///
/// This is not a store in the sense the rest of the library means one, and it is not a
/// substitute for the durable machine store: it holds one process's restart history so a host
/// that has not registered a durable audit writer still boots and can be inspected in a test.
/// Nothing depends on it surviving. A deployment that wants this history to outlive the process
/// registers <c>PostgresSupervisionStore</c> instead.
///
/// It lives here rather than in a storage assembly because it exists for the host-integration
/// layer's convenience, which is the only thing that requires an
/// <see cref="T:ByzantineSystems.Automata.Storage.ISupervisionEventStore" /> to be present at
/// all.
/// </summary>
type InMemorySupervisionStore() =

    let records = ConcurrentQueue<SupervisionRecord>()

    /// <summary>Every record written so far, oldest first.</summary>
    member _.Records: SupervisionRecord list = List.ofSeq records

    interface ISupervisionEventStore with

        member _.Record(record, _) =
            records.Enqueue record
            Task.FromResult(Ok())

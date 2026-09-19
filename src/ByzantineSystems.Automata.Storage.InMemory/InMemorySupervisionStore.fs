namespace ByzantineSystems.Automata.Storage.InMemory

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Storage

/// <summary>Thread-safe in-memory append-only supervision audit store.</summary>
type InMemorySupervisionStore() =

    let gate = obj ()
    let records = ResizeArray<SupervisionRecord>()

    /// <summary>Returns an occurrence-ordered snapshot of all recorded facts.</summary>
    member _.Recorded() : SupervisionRecord list =
        lock gate (fun () -> records |> Seq.toList)

    interface ISupervisionEventStore with

        member _.Record(record, ct) =
            ct.ThrowIfCancellationRequested()
            lock gate (fun () -> records.Add record)
            Ok() |> Task.FromResult

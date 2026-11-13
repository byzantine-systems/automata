/// <summary>
/// Shared assertions for the TAP entry contract every provider must satisfy on each of its
/// public members. Lives in the conformance library, so a rule stated once is checked against
/// PostgreSQL and SQLite alike.
/// </summary>
module StorageTapContracts

open System
open System.Threading
open System.Threading.Tasks
open Expecto

/// <summary>
/// Asserts the task-shape half of the pre-cancellation contract: a token already canceled on
/// entry answers a task canceled with that exact token. Provider tests pair this with poisoned
/// work and durable-state observations to prove that the operation body did not run.
/// </summary>
let canceledWithoutWork (label: string) (invoke: CancellationToken -> Task) =
    use source = new CancellationTokenSource()
    source.Cancel()

    let operation = invoke source.Token

    if operation.Status <> TaskStatus.Canceled then
        failtestf
            "%s: an already-canceled token must answer TaskStatus.Canceled, but the task is %O"
            label
            operation.Status

    try
        operation.GetAwaiter().GetResult() |> ignore
        failtestf "%s: a canceled task cannot run to completion" label
    with :? TaskCanceledException as error ->
        if not (error.CancellationToken.Equals source.Token) then
            failtestf "%s: the canceled task must carry the caller's own token" label

/// <summary>
/// Asserts the usage-error contract: a null argument throws <c>ArgumentNullException</c> at the
/// call site, synchronously, rather than faulting a task or failing later inside the work.
/// </summary>
let nullThrowsAtCallSite (label: string) (invoke: unit -> unit) =
    try
        invoke ()
        failtestf "%s: a null argument must throw ArgumentNullException, but the call returned" label
    with :? ArgumentNullException ->
        ()

/// Asserts the defect channel: an exception raised while starting valid work faults the returned
/// task instead of escaping synchronously from the public call.
let defectFaultsTask<'Error when 'Error :> exn> (label: string) (invoke: unit -> Task) =
    let operation =
        try
            invoke ()
        with error ->
            failtestf "%s: a defect escaped synchronously as %s" label (error.GetType().FullName)

    if operation.Status <> TaskStatus.Faulted then
        failtestf "%s: the defect must answer TaskStatus.Faulted, but the task is %O" label operation.Status

    try
        operation.GetAwaiter().GetResult() |> ignore
        failtestf "%s: a faulted task cannot run to completion" label
    with :? 'Error ->
        ()

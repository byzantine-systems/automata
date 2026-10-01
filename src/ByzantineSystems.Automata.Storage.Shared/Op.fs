namespace ByzantineSystems.Automata.Storage.Internal

// Compiled into each storage provider through a linked <Compile> item rather than shipped as a
// project of its own. Every type here is internal, so the two copies never meet: each provider
// assembly carries its own, and neither is part of any public surface.

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// A unit of database work: given a session and a cancellation token, it answers with a result
/// or a store error.
///
/// Cold. Building one touches nothing; only a runner, which owns the connection and the
/// resilience pipeline, supplies a session and starts it. That is what lets a retry run the whole
/// unit again from the start, on a fresh connection and inside a fresh transaction, rather than
/// resuming a half-finished one.
///
/// The case is private so that the only way to execute one is <c>Op.run</c>, and the only
/// callers of that are the runners in each provider's <c>Db.fs</c>.
/// </summary>
type internal Op<'Session, 'T> = private Op of ('Session -> CancellationToken -> Task<Result<'T, StoreError>>)

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Internal.Op`2" />.</summary>
[<RequireQualifiedAccess>]
module internal Op =

    /// <summary>
    /// Wraps driver work as a unit. For the primitives in each provider's <c>Db.fs</c> only: store
    /// code composes the primitives and never reaches the driver itself.
    /// </summary>
    let ofDriver (work: 'Session -> CancellationToken -> Task<Result<'T, StoreError>>) : Op<'Session, 'T> = Op work

    /// <summary>
    /// Starts a unit against a session. For the runners in each provider's <c>Db.fs</c> only: they
    /// own the session's lifetime, and nothing else may hold one.
    /// </summary>
    let run (Op work: Op<'Session, 'T>) (session: 'Session) (ct: CancellationToken) : Task<Result<'T, StoreError>> =
        work session ct

    let ok (value: 'T) : Op<'Session, 'T> =
        Op(fun _ _ -> Task.FromResult(Ok value))

    let fail (error: StoreError) : Op<'Session, 'T> =
        Op(fun _ _ -> Task.FromResult(Error error))

    let ofResult (result: Result<'T, StoreError>) : Op<'Session, 'T> = Op(fun _ _ -> Task.FromResult result)

    /// <summary>The session itself, for the primitives in each provider's <c>Db.fs</c>.</summary>
    let session<'Session> : Op<'Session, 'Session> =
        Op(fun session _ -> Task.FromResult(Ok session))

    /// <summary>
    /// Runs <paramref name="next" /> on the value <paramref name="op" /> produced, on the same
    /// session. An error stops the unit there: nothing after it runs.
    /// </summary>
    let bind (next: 'T -> Op<'Session, 'U>) (Op work: Op<'Session, 'T>) : Op<'Session, 'U> =
        Op(fun session ct ->
            backgroundTask {
                match! work session ct with
                | Ok value -> return! run (next value) session ct
                | Error error -> return Error error
            })

    let map (f: 'T -> 'U) (op: Op<'Session, 'T>) : Op<'Session, 'U> = bind (f >> ok) op

    /// <summary>Keeps the effect and drops the value, such as a statement's changed-row count.</summary>
    let discard (op: Op<'Session, 'T>) : Op<'Session, unit> = map ignore op

/// <summary>
/// The computation expression over <see cref="T:ByzantineSystems.Automata.Storage.Internal.Op`2" />,
/// fixed to one session type so that a unit written for one session cannot be run with another.
///
/// <c>let!</c> binds a unit or a plain <c>Result</c>, and an error from either stops the unit.
/// Deliberately absent: binding a <c>Task</c>, which is how a runner would be called from inside a
/// unit and take a second connection or a second write gate while holding the first; <c>and!</c>,
/// since one connection cannot run two commands at once; and exception handling, which belongs
/// to the runner's single translation from exceptions to results.
/// </summary>
type internal OpBuilder<'Session>() =

    member _.Return(value: 'T) : Op<'Session, 'T> = Op.ok value

    member _.ReturnFrom(op: Op<'Session, 'T>) : Op<'Session, 'T> = op

    member _.Zero() : Op<'Session, unit> = Op.ok ()

    member _.Bind(op: Op<'Session, 'T>, next: 'T -> Op<'Session, 'U>) : Op<'Session, 'U> = Op.bind next op

    /// <summary>Keeps the body unevaluated until a runner starts the unit, which is what makes it cold.</summary>
    member _.Delay(body: unit -> Op<'Session, 'T>) : Op<'Session, 'T> =
        Op.ofDriver (fun session ct -> Op.run (body ()) session ct)

    member _.Combine(first: Op<'Session, unit>, next: Op<'Session, 'T>) : Op<'Session, 'T> =
        Op.bind (fun () -> next) first

    member _.Source(op: Op<'Session, 'T>) : Op<'Session, 'T> = op

    member _.Source(result: Result<'T, StoreError>) : Op<'Session, 'T> = Op.ofResult result

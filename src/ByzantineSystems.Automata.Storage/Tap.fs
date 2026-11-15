namespace ByzantineSystems.Automata.Storage

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Internal

type private EncodedArgumentNullException(name: string) =
    inherit ArgumentNullException(name)

/// <summary>
/// The entry boundary every public store member shares, per TAP: usage errors are rejected
/// synchronously before any task exists, and a token already canceled on entry answers a
/// canceled task without doing any of the work. Explicit null checks come first. A null hidden
/// inside a struct entity id is visible only after encoding, behind the cancellation gate.
/// </summary>
[<RequireQualifiedAccess>]
module internal Tap =

    /// <summary>
    /// Rejects a null reference argument at the call site. Value-type arguments box to non-null
    /// and pass by construction, which is what makes one guard enough for constrained types and
    /// unconstrained entity ids alike.
    /// </summary>
    let requireArg (name: string) (value: 'T) : unit =
        match box value with
        | null -> nullArg name
        | _ -> ()

    /// <summary>
    /// Rejects a machine id that is the zero value rather than one a caller built. The box test
    /// of <see cref="M:ByzantineSystems.Automata.Storage.Tap.requireArg(System.String,System.Object)" />
    /// cannot see it: <c>MachineId</c> is a struct DU, and its zero value carries a null string
    /// that only <c>Unchecked.defaultof</c> can produce, because
    /// <see cref="M:ByzantineSystems.Automata.Core.MachineId.create(System.String)" /> refuses
    /// null text. This is the same usage error <c>requireArg</c> reports, found where a struct
    /// wrapper hides it.
    /// </summary>
    let requireMachineId (name: string) (value: MachineId) : unit =
        match box value with
        | null -> nullArg name
        | _ ->
            if isNull (MachineId.value value) then
                nullArg name

    /// <summary>
    /// Rejects an encoded entity key whose text is null, which is how a null entity id reaches
    /// the boundary: the generic id may be a struct DU carrying null (the zero
    /// <c>EntityId</c>), which the box test of <c>requireArg</c> passes by construction, or a
    /// reference the caller's encoder projected to null. Checked where an id is encoded, inside
    /// the started work, so a pre-canceled call still encodes nothing.
    /// </summary>
    let requireEntityKey (name: string) (key: EntityKey) : unit =
        match box key with
        | null -> raise (EncodedArgumentNullException name)
        | _ ->
            if isNull (EntityKey.value key) then
                raise (EncodedArgumentNullException name)

    /// <summary>
    /// Starts the work hot, or answers a canceled task carrying the caller's token without
    /// invoking it. Encoding, callbacks, connections and writes all sit behind this gate.
    /// </summary>
    let start (cancellationToken: CancellationToken) (work: unit -> Task<'T>) : Task<'T> =
        if cancellationToken.IsCancellationRequested then
            Task.FromCanceled<'T> cancellationToken
        else
            try
                work ()
            with
            | :? EncodedArgumentNullException -> reraise ()
            | error -> Task.FromException<'T> error

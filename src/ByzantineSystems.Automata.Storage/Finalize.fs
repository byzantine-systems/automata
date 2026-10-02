namespace ByzantineSystems.Automata.Storage.Internal

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>A committed draft with its four payloads already encoded, so the transaction only writes.</summary>
type internal EncodedDraft<'EntityId, 'State, 'Event, 'Action> =
    { Source: TransitionDraft<'EntityId, 'State, 'Event, 'Action>
      EventJson: string
      ActionsJson: string
      FromJson: string
      ToJson: string }

/// <summary>How a leased command ends. Each case carries exactly what that ending writes.</summary>
type internal Resolution<'EntityId, 'State, 'Event, 'Action> =
    /// <summary>The chart accepted the event: a transition at the epoch after <c>expected</c>.</summary>
    | Commit of expected: Epoch * draft: EncodedDraft<'EntityId, 'State, 'Event, 'Action>

    /// <summary>The chart refused the event. The encoded failure is recorded and no state moves.</summary>
    | Reject of error: string

    /// <summary>The machine gave up on the command. The encoded failure is recorded and no state moves.</summary>
    | DeadLetter of error: string

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Internal.Resolution`4" />.</summary>
[<RequireQualifiedAccess>]
module internal Resolution =

    /// <summary>The terminal status an ending writes, as both schemas spell it.</summary>
    let status (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>) : string =
        match resolution with
        | Commit _ -> "succeeded"
        | Reject _ -> "rejected"
        | DeadLetter _ -> "dead_letter"

    /// <summary>
    /// The epoch an ending reports when no transition answers for it. A failure moves no state, so
    /// it reports the initial epoch, as the PostgreSQL routine does; a commit reports the epoch it
    /// expected.
    /// </summary>
    let unmoved (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>) : Epoch =
        match resolution with
        | Commit(expected, _) -> expected
        | Reject _
        | DeadLetter _ -> Epoch.initial

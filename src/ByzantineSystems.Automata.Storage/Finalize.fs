namespace ByzantineSystems.Automata.Storage.Internal

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>Only a first commit supplies an origin; continuing commits use the locked snapshot.</summary>
[<RequireQualifiedAccess>]
type internal CommitOrigin =
    | Initial of string
    | Continuing

[<RequireQualifiedAccess>]
module internal CommitOrigin =
    let payload =
        function
        | CommitOrigin.Initial state -> Some state
        | CommitOrigin.Continuing -> None

/// <summary>A committed draft with its state and action payloads encoded before writing.</summary>
type internal EncodedDraft<'EntityId, 'State, 'Event, 'Action> =
    { Source: TransitionDraft<'EntityId, 'State, 'Event, 'Action>
      ActionsJson: string
      Origin: CommitOrigin
      ToJson: string }

/// <summary>Shared payload preparation. None denotes a correction with an explicit origin.</summary>
[<RequireQualifiedAccess>]
module internal DraftEncoding =
    let encode
        (stateCodec: Codec<'State>)
        (actionCodec: Codec<'Action list>)
        (expected: Epoch option)
        (draft: TransitionDraft<'EntityId, 'State, 'Event, 'Action>)
        =
        let origin =
            match expected with
            | Some epoch when epoch <> Epoch.initial -> Ok CommitOrigin.Continuing
            | Some _
            | None -> stateCodec.Encode draft.FromState |> Result.map CommitOrigin.Initial

        match actionCodec.Encode draft.Actions, origin, stateCodec.Encode draft.ToState with
        | Ok actions, Ok origin, Ok destination ->
            Ok
                { Source = draft
                  ActionsJson = actions
                  Origin = origin
                  ToJson = destination }
        | Error(EncodeError(name, error)), _, _
        | Error(DecodeError(name, error)), _, _
        | _, Error(EncodeError(name, error)), _
        | _, Error(DecodeError(name, error)), _
        | _, _, Error(EncodeError(name, error))
        | _, _, Error(DecodeError(name, error)) -> Error(StoreError.Serialization(name, error))

/// <summary>How a leased command ends. Each case carries exactly what that ending writes.</summary>
type internal Resolution<'EntityId, 'State, 'Event, 'Action> =
    /// <summary>The chart accepted the event: a transition at the epoch after <c>expected</c>.</summary>
    | Commit of expected: Epoch * draft: EncodedDraft<'EntityId, 'State, 'Event, 'Action>

    /// <summary>The chart refused the event. The encoded failure is recorded and no state moves.</summary>
    | Reject of error: EncodedFailure

    /// <summary>The machine gave up on the command. The encoded failure is recorded and no state moves.</summary>
    | DeadLetter of error: EncodedFailure

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Internal.Resolution`4" />.</summary>
[<RequireQualifiedAccess>]
module internal Resolution =

    /// <summary>The terminal status an ending writes.</summary>
    let status (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>) : CommandStatus =
        match resolution with
        | Commit _ -> CommandStatus.Succeeded
        | Reject _ -> CommandStatus.Rejected
        | DeadLetter _ -> CommandStatus.DeadLettered

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

/// <summary>Where a finalizing caller stands before any writes are permitted.</summary>
type internal FinalizationStanding<'Target> =
    | Unknown
    | FinishedByCaller of CommandStatus
    | FinishedByAnother
    | HeldByAnother
    | HeldByCaller of 'Target

/// <summary>Atomic routine or checked statement sequence, interpreted within one write session.</summary>
type internal FinalizationInstruction<'Params, 'Reader, 'Target> =
    | AtomicFinalization of StoreProgram<WriteAccess, 'Params, 'Reader, FinalizeOutcome>
    | CheckedFinalization of
        read: StoreProgram<WriteAccess, 'Params, 'Reader, FinalizationStanding<'Target>> *
        already: (CommandStatus -> StoreProgram<WriteAccess, 'Params, 'Reader, FinalizeOutcome>) *
        resolve: ('Target -> StoreProgram<WriteAccess, 'Params, 'Reader, FinalizeOutcome>) *
        missing: StoreError

[<RequireQualifiedAccess>]
module internal FinalizeProgram =
    let finish instruction =
        store {
            match instruction with
            | AtomicFinalization program -> return! program
            | CheckedFinalization(read, already, resolve, missing) ->
                match! read with
                | Unknown -> return! StoreProgram.fail missing
                | FinishedByCaller status -> return! already status
                | FinishedByAnother
                | HeldByAnother -> return FinalizeOutcome.LeaseLost
                | HeldByCaller target -> return! resolve target
        }

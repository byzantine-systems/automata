namespace ByzantineSystems.Automata.Runtime

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>What a correction replays from, and what it is attributed to.</summary>
type ReplayInput<'EntityId, 'State, 'Event> =
    {
        MachineId: MachineId
        EntityId: 'EntityId
        /// <summary>The belief in force just before the corrected instant.</summary>
        Start: 'State
        /// <summary>The entity's state now, which the correction's own transition starts from.</summary>
        Live: 'State
        /// <summary>The missed event.</summary>
        Missed: 'Event
        /// <summary>When it should have happened.</summary>
        At: DateTimeOffset
        /// <summary>The correction command, which the entity's final belief is attributed to.</summary>
        Correction: CommandId
        /// <summary>The version the correction is decided under.</summary>
        Current: ChartVersion
        /// <summary>The epoch the correction commits at.</summary>
        NextEpoch: Epoch
        Policy: CorrectionPolicy
    }

/// <summary>A replayed correction, ready to commit, or to show without committing.</summary>
type ReplayPlan<'EntityId, 'State, 'Event, 'Action> =
    {
        /// <summary>The missed event, decided at the corrected instant.</summary>
        Inserted: TransitionDraft<'EntityId, 'State, 'Event, 'Action>
        /// <summary>
        /// Each later event as it was decided, beside how it is decided now. The replayed drafts
        /// carry the actions replay would have emitted; none of them is.
        /// </summary>
        Replayed:
            (CommittedTransition<'EntityId, 'State, 'Event, 'Action> *
            TransitionDraft<'EntityId, 'State, 'Event, 'Action>) list
        /// <summary>Where the timeline stops, when the policy truncates at a divergence.</summary>
        Truncated: Epoch option
        /// <summary>What the store writes: the correction's transition and the new timeline.</summary>
        Commit: CorrectionCommit<'EntityId, 'State, 'Event, 'Action>
    }

/// <summary>
/// Re-deciding history after a missed event. Pure: it reads nothing and writes nothing, so a
/// correction can be previewed with exactly the code that commits it.
/// </summary>
[<RequireQualifiedAccess>]
module Replay =

    let private diverged (epoch: Epoch) (error: TransitionError<'Err>) =
        ReplayError.Diverged(epoch, $"%A{error}")

    let private chartFor catalog version =
        ChartCatalog.tryFind version catalog
        |> Option.map Ok
        |> Option.defaultValue (Error(ReplayError.UnknownChartVersion version))

    /// One event, decided against the state before it, under the chart that first decided it.
    /// An instance that already ended accepts nothing further, as the processor would say.
    let private redecide
        (input: ReplayInput<'EntityId, 'State, 'Event>)
        catalog
        (state: 'State, status: InstanceStatus)
        (original: CommittedTransition<_, _, _, _>)
        =
        match status with
        | InstanceStatus.Running ->
            chartFor catalog original.ChartVersion
            |> Result.bind (fun chart ->
                Chart.resolve chart state original.Draft.Event
                |> Result.map (
                    Draft.ofResolution
                        input.MachineId
                        input.EntityId
                        chart
                        state
                        original.Draft.Event
                        original.Draft.EffectiveAt
                )
                |> Result.mapError (diverged original.Epoch))
        | ended -> Error(ReplayError.Diverged(original.Epoch, $"the instance is %A{ended}"))

    /// Folds the suffix, stopping at the first divergence: as an error under Fail, as the end of
    /// the timeline under Truncate.
    let private replaySuffix
        (input: ReplayInput<'EntityId, 'State, 'Event>)
        catalog
        (start: TransitionDraft<_, _, _, _>)
        suffix
        =
        let rec fold (state, status) replayed remaining =
            match remaining with
            | [] -> Ok(List.rev replayed, None)
            | original :: rest ->
                match redecide input catalog (state, status) original, input.Policy.OnDivergence with
                | Ok draft, _ -> fold (draft.ToState, draft.Status) ((original, draft) :: replayed) rest
                | Error(ReplayError.Diverged(epoch, _)), Divergence.Truncate -> Ok(List.rev replayed, Some epoch)
                | Error error, _ -> Error error

        fold (start.ToState, start.Status) [] suffix

    let private belief (draft: TransitionDraft<_, 'State, _, _>) epoch commandId version : CorrectedBelief<'State> =
        { Asserted =
            { State = draft.ToState
              Status = draft.Status
              Epoch = epoch }
          ValidFrom = draft.EffectiveAt
          CommandId = commandId
          ChartVersion = version }

    /// Two beliefs starting at one instant are one belief: the later one, since it was decided
    /// after the other. The store accepts only strictly ascending beliefs.
    let private compact (beliefs: CorrectedBelief<'State> list) =
        beliefs
        |> List.fold
            (fun (kept: CorrectedBelief<'State> list) (next: CorrectedBelief<'State>) ->
                match kept with
                | previous :: rest when previous.ValidFrom = next.ValidFrom -> next :: rest
                | _ -> next :: kept)
            []
        |> List.rev

    /// The final belief is the entity's live one, and the next ordinary command expects its epoch
    /// to be the latest; so it is attributed to the correction, whichever event produced it.
    let private attributeLast (input: ReplayInput<_, 'State, _>) (beliefs: CorrectedBelief<'State> list) =
        match List.rev beliefs with
        | [] -> []
        | (last: CorrectedBelief<'State>) :: earlier ->
            let live =
                { last with
                    Asserted =
                        { last.Asserted with
                            Epoch = input.NextEpoch }
                    CommandId = input.Correction
                    ChartVersion = input.Current }

            List.rev (live :: earlier)

    /// <summary>
    /// Replays a missed event and every committed event after it.
    ///
    /// The missed event is decided against the belief in force just before its instant, under
    /// the current chart. Each later event is then re-decided, in effective-time order, under
    /// the chart version it was first decided by, and keeps its own effective time.
    /// </summary>
    let plan
        (catalog: ChartCatalog<'State, 'Event, 'Action, 'Err>)
        (input: ReplayInput<'EntityId, 'State, 'Event>)
        (suffix: CommittedTransition<'EntityId, 'State, 'Event, 'Action> list)
        : Result<ReplayPlan<'EntityId, 'State, 'Event, 'Action>, ReplayError> =
        let ordered =
            suffix
            |> List.sortBy (fun transition -> transition.Draft.EffectiveAt, Epoch.value transition.Epoch)

        if List.length ordered > input.Policy.ReplayLimit then
            Error(ReplayError.BudgetExceeded input.Policy.ReplayLimit)
        else
            chartFor catalog input.Current
            |> Result.bind (fun chart ->
                Chart.resolve chart input.Start input.Missed
                |> Result.map (
                    Draft.ofResolution input.MachineId input.EntityId chart input.Start input.Missed input.At
                )
                |> Result.mapError (diverged input.NextEpoch))
            |> Result.bind (fun inserted ->
                replaySuffix input catalog inserted ordered
                |> Result.map (fun (replayed, truncated) ->
                    let final =
                        replayed |> List.tryLast |> Option.map snd |> Option.defaultValue inserted

                    let beliefs =
                        belief inserted input.NextEpoch input.Correction input.Current
                        :: [ for original, draft in replayed ->
                                 belief draft original.Epoch original.CommandId original.ChartVersion ]
                        |> compact
                        |> attributeLast input

                    { Inserted = inserted
                      Replayed = replayed
                      Truncated = truncated
                      Commit =
                        { Transition =
                            { inserted with
                                FromState = input.Live
                                ToState = final.ToState
                                Status = final.Status
                                Actions = [] }
                          ValidFrom = input.At
                          Beliefs = beliefs } }))

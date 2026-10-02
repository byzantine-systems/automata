namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Postgres.Schema
open FsToolkit.ErrorHandling
open SqlHydra.Query

/// <summary>
/// Everything the processor needs to bridge the generic domain types to the columns of
/// <c>fsm.transition</c>, <c>fsm.instance_state</c> and <c>fsm.command_error</c>.
/// </summary>
type CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err> =
    {
        Context: PostgresContext
        /// <summary>
        /// The queue a commit enqueues its actions into, inside the commit's own transaction.
        /// The empty string means this machine delivers no actions, which is how a chart that
        /// emits none avoids requiring a queue to exist.
        /// </summary>
        ActionQueue: string
        StateCodec: Codec<'State>
        EventCodec: Codec<'Event>
        ActionCodec: Codec<'Action list>
        ErrorCodec: Codec<'Err>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>Translations between the processor's domain types and their column representations.</summary>
[<RequireQualifiedAccess>]
module private ProcessorMapping =

    let outcomeFromString (value: string) (epoch: int64) (expected: Epoch) : Result<FinalizeOutcome, StoreError> =
        match value with
        | "finalized" -> Ok(Finalized(Db.epochOf epoch))
        | "already_finalized" -> Ok(AlreadyFinalized(Db.epochOf epoch))
        | "conflict" -> Ok(Conflict(expected, Db.epochOf epoch))
        | "lease_lost" -> Ok FinalizeOutcome.LeaseLost
        | other -> Error(Db.decodeFailure (nameof FinalizeOutcome) $"unknown finalize outcome {other}")

/// <summary>
/// The PostgreSQL end of processing a command.
///
/// Every operation is one call to <c>fsm.finalize_command</c>, which is one transaction: the
/// transition, the belief, the result and the inbox row move together or not at all. Retrying
/// after an ambiguous failure is safe, because the routine answers a repeated call from the same
/// lease with what that lease already did.
/// </summary>
/// <summary>
/// A replay error as the error column stores it: a kind, and the fields that kind has. Written by
/// this library alone, so every field is required on the way back.
/// </summary>
[<RequireQualifiedAccess>]
module private ReplayMapping =

    let toJson (error: ReplayError) : string =
        let node = System.Text.Json.Nodes.JsonObject()

        match error with
        | ReplayError.UnknownChartVersion version ->
            node["kind"] <- "unknown_chart_version"
            node["version"] <- ChartVersion.value version
        | ReplayError.Diverged(epoch, reason) ->
            node["kind"] <- "diverged"
            node["epoch"] <- int64 (Epoch.value epoch)
            node["reason"] <- reason
        | ReplayError.BudgetExceeded limit ->
            node["kind"] <- "budget_exceeded"
            node["limit"] <- limit
        | ReplayError.HistoryPurged -> node["kind"] <- "history_purged"

        node.ToJsonString()

    let ofJson (element: JsonElement) : Result<ReplayError, StoreError> =
        let fail () =
            Error(Db.decodeFailure (nameof ReplayError) "an unreadable replay error")

        let field (name: string) (read: JsonElement -> 'T) =
            match element.TryGetProperty name with
            | true, value -> Ok(read value)
            | _ -> fail ()

        match element.TryGetProperty "kind" with
        | true, kind ->
            match kind.GetString() with
            | "unknown_chart_version" ->
                field "version" _.GetInt32()
                |> Result.map (ChartVersion.create >> ReplayError.UnknownChartVersion)
            | "diverged" ->
                Result.map2
                    (fun epoch reason -> ReplayError.Diverged(Db.epochOf epoch, reason))
                    (field "epoch" _.GetInt64())
                    (field "reason" _.GetString())
            | "budget_exceeded" -> field "limit" _.GetInt32() |> Result.map ReplayError.BudgetExceeded
            | "history_purged" -> Ok ReplayError.HistoryPurged
            | _ -> fail ()
        | _ -> fail ()

type PostgresCommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err>
    (options: CommandProcessorOptions<'EntityId, 'State, 'Event, 'Action, 'Err>) =

    let context = options.Context

    let finalizeCommand = Statement.load "command" "finalize"
    let finalizeCorrection = Statement.load "command" "finalize_correction"

    /// What a commit sends beyond the command's identity: the draft and its four encoded
    /// payloads, encoded before anything is sent.
    let encodeDraft
        (draft: TransitionDraft<'EntityId, 'State, 'Event, 'Action>)
        : Result<EncodedDraft<'EntityId, 'State, 'Event, 'Action>, StoreError> =
        match
            options.EventCodec.Encode draft.Event,
            options.ActionCodec.Encode draft.Actions,
            options.StateCodec.Encode draft.FromState,
            options.StateCodec.Encode draft.ToState
        with
        | Ok event, Ok actions, Ok fromState, Ok toState ->
            Ok
                { Source = draft
                  EventJson = event
                  ActionsJson = actions
                  FromJson = fromState
                  ToJson = toState }
        | Error error, _, _, _
        | _, Error error, _, _
        | _, _, Error error, _
        | _, _, _, Error error -> Error(Db.toStoreError error)

    let states (ids: StateId list) : string list = ids |> List.map StateId.value

    /// <summary>
    /// One call to <c>fsm.finalize_command</c>. Which half of its parameters is filled follows
    /// from how the command ends, so the two shapes cannot be half-filled or mixed: a commit
    /// sends its transition and no error, a failure sends its error and no transition.
    ///
    /// A failure writes no state, so there is nothing for a stale epoch to conflict with; it
    /// sends the initial epoch and the routine skips the check.
    /// </summary>
    let finalize
        (commandId: CommandId)
        (token: LeaseToken<CommandWork>)
        (resolution: Resolution<'EntityId, 'State, 'Event, 'Action>)
        : Op<PgSession, FinalizeOutcome> =
        let expected = Resolution.unmoved resolution

        let ending =
            match resolution with
            | Commit(_, encoded) ->
                let draft = encoded.Source

                [ "error", Param.JsonbOrNull None
                  "state", Param.JsonbOrNull(Some encoded.ToJson)
                  "instance_status", Param.TextOrNull(Some(Db.instanceStatusToString draft.Status))
                  "effective_at", Param.TimestampOrNull(Some draft.EffectiveAt)
                  "event", Param.JsonbOrNull(Some encoded.EventJson)
                  "actions", Param.JsonbOrNull(Some encoded.ActionsJson)
                  "from_state", Param.JsonbOrNull(Some encoded.FromJson)
                  "to_state", Param.JsonbOrNull(Some encoded.ToJson)
                  "handled_by", Param.TextOrNull(Some(StateId.value draft.HandledBy))
                  "exited", Param.TextArrayOrNull(Some(states draft.Exited))
                  "entered", Param.TextArrayOrNull(Some(states draft.Entered)) ]
            | Reject error
            | DeadLetter error ->
                [ "error", Param.JsonbOrNull(Some error)
                  "state", Param.JsonbOrNull None
                  "instance_status", Param.TextOrNull None
                  "effective_at", Param.TimestampOrNull None
                  "event", Param.JsonbOrNull None
                  "actions", Param.JsonbOrNull None
                  "from_state", Param.JsonbOrNull None
                  "to_state", Param.JsonbOrNull None
                  "handled_by", Param.TextOrNull None
                  "exited", Param.TextArrayOrNull None
                  "entered", Param.TextArrayOrNull None ]

        postgres {
            let! outcome, epoch =
                Sql.one
                    (nameof FinalizeOutcome)
                    finalizeCommand
                    ([ "command_id", Param.Bigint(CommandId.value commandId)
                       "lease_token", Param.Bigint(LeaseToken.value token)
                       "expected_epoch", Param.Bigint(int64 (Epoch.value expected))
                       "status", Param.Text(Resolution.status resolution)
                       "action_queue", Param.Text options.ActionQueue ]
                     @ ending)
                    (fun reader -> Row.string reader "outcome", Row.int64 reader "epoch")

            return! ProcessorMapping.outcomeFromString outcome epoch expected
        }

    /// <summary>
    /// The failure column is a tagged envelope rather than the bare domain error, because the
    /// two cases have to be told apart on the way back out. A domain error is the
    /// application's own value and is encoded by its codec; a machine reason is a string this
    /// library wrote. Reading a row and guessing which one it holds is not possible once they
    /// share a column, so the tag goes in.
    /// </summary>
    let encodeFailure (failure: CommandFailure<'Err>) : Result<string, StoreError> =
        match failure with
        | CommandFailure.Domain error ->
            options.ErrorCodec.Encode error
            |> Result.mapError Db.toStoreError
            |> Result.map (fun encoded -> $"""{{"domain":%s{encoded}}}""")
        | CommandFailure.Machine reason -> Ok $"""{{"machine":%s{JsonSerializer.Serialize reason}}}"""
        | CommandFailure.Replay error -> Ok $"""{{"replay":%s{ReplayMapping.toJson error}}}"""

    let readFailure (json: string) : Result<CommandFailure<'Err>, StoreError> =
        // The column is jsonb, so the server has already proved this parses. What it has not
        // proved is that the shape is one this version writes.
        use document = JsonDocument.Parse json
        let root = document.RootElement

        match root.TryGetProperty "domain" with
        | true, domain ->
            domain.GetRawText()
            |> options.ErrorCodec.Decode
            |> Result.mapError Db.toStoreError
            |> Result.map CommandFailure.Domain
        | _ ->
            match root.TryGetProperty "machine", root.TryGetProperty "replay" with
            | (true, machine), _ when machine.ValueKind = JsonValueKind.String ->
                Ok(CommandFailure.Machine(machine.GetString()))
            | _, (true, replay) -> ReplayMapping.ofJson replay |> Result.map CommandFailure.Replay
            | _ ->
                Error(
                    Db.decodeFailure (nameof CommandFailure) "a command error carried no domain, machine or replay tag"
                )

    /// A rejection or a dead letter, encoded before anything is sent.
    let fail
        (resolve: string -> Resolution<'EntityId, 'State, 'Event, 'Action>)
        (commandId: CommandId)
        token
        (failure: CommandFailure<'Err>)
        ct
        =
        backgroundTaskResult {
            let! encoded = encodeFailure failure
            return! Sql.run context (finalize commandId token (resolve encoded)) ct
        }

    /// Rebuilds a committed transition from its generated row. Seven decodes can fail
    /// independently, and none of them is expected: each means the row is not what this code was
    /// compiled for. Shared by the command-result read and the history page, so one row shape
    /// has one decoder.
    let toTransition
        (row: fsm.transition)
        : Result<CommittedTransition<'EntityId, 'State, 'Event, 'Action>, StoreError> =
        let entityId =
            row.entity_id
            |> options.EntityIdDecode
            |> Result.mapError (Db.decodeFailure "EntityId")

        let decode (codec: Codec<'T>) (json: string) =
            codec.Decode json |> Result.mapError Db.toStoreError

        let chartVersion =
            row.chart_version
            |> ChartVersion.tryCreate
            |> Result.mapError (Db.decodeFailure "ChartVersion")

        match
            entityId,
            decode options.EventCodec row.event,
            decode options.ActionCodec row.actions,
            decode options.StateCodec row.from_state,
            decode options.StateCodec row.to_state,
            Db.instanceStatusFromString row.status,
            chartVersion
        with
        | Ok entityId, Ok event, Ok actions, Ok fromState, Ok toState, Ok status, Ok chartVersion ->
            Ok
                { Draft =
                    { MachineId = MachineId.create row.machine_id
                      EntityId = entityId
                      Event = event
                      Actions = actions
                      FromState = fromState
                      ToState = toState
                      HandledBy = StateId.create row.handled_by
                      Exited = row.exited |> Array.map StateId.create |> List.ofArray
                      Entered = row.entered |> Array.map StateId.create |> List.ofArray
                      Status = status
                      EffectiveAt = Db.fromTimestamp row.effective_at }
                  Epoch = Db.epochOf row.epoch
                  CommandId = CommandId.ofInt64 row.command_id
                  ChartVersion = chartVersion
                  CommittedAt = Db.fromTimestamp row.committed_at }
        | Error error, _, _, _, _, _, _
        | _, Error error, _, _, _, _, _
        | _, _, Error error, _, _, _, _
        | _, _, _, Error error, _, _, _
        | _, _, _, _, Error error, _, _
        | _, _, _, _, _, Error error, _
        | _, _, _, _, _, _, Error error -> Error error

    /// The current belief, from the view's nullable columns. The view cannot carry NOT NULL, so
    /// each column is required here instead of assumed.
    let toSnapshot (row: fsm.current_belief) : Result<Snapshot<'State>, StoreError> =
        let column name value = Db.required "Snapshot" name value

        result {
            let! state = column "state" row.state
            let! status = column "status" row.status
            let! epoch = column "epoch" row.epoch
            let! state = options.StateCodec.Decode state |> Result.mapError Db.toStoreError
            let! status = Db.instanceStatusFromString status

            return
                { State = state
                  Epoch = Db.epochOf epoch
                  Status = status }
        }

    /// What became of one command, from the command, its transition if it committed, and its
    /// error if it failed.
    let toResult
        (command: fsm.command, transition: fsm.transition option, error: fsm.command_error option)
        : Result<CommandResult<'EntityId, 'State, 'Event, 'Action, 'Err>, StoreError> =
        let failure () =
            match error with
            | Some error -> readFailure error.error
            | None -> Error(Db.decodeFailure (nameof CommandResult) "a failed command has no recorded error")

        match command.status, transition with
        | ("ready" | "leased"), _ -> Ok CommandResult.Pending
        | "succeeded", Some transition -> toTransition transition |> Result.map CommandResult.Committed
        | "succeeded", None -> Error(Db.decodeFailure (nameof CommandResult) "a succeeded command has no transition")
        | "rejected", _ -> failure () |> Result.map CommandResult.Rejected
        | "dead_letter", _ -> failure () |> Result.map CommandResult.DeadLettered
        | other, _ -> Error(Db.decodeFailure (nameof CommandResult) $"unknown command status {other}")

    interface IStateReader<'EntityId, 'State, 'Event, 'Action> with

        member _.TryGetSnapshot(machineId, entityId, ct) =
            let machine = Some(MachineId.value machineId)
            let entity = Some(options.EntityIdEncode entityId)

            Sql.run
                context
                (postgres {
                    let! row =
                        Sql.select (fun query token ->
                            selectTask query {
                                for b in fsm.current_belief do
                                    where (b.machine_id = machine && b.entity_id = entity)
                                    select b
                                    tryHead
                                    cancel token
                            })

                    return! row |> Option.traverseResult toSnapshot
                })
                ct

        member _.History(machineId, entityId, paging, ct) =
            let machine = MachineId.value machineId
            let entity = options.EntityIdEncode entityId

            // An absent cursor is zero rather than a null, because transition_epoch_positive
            // forbids a stored epoch of zero. One query shape then serves every page.
            let after =
                Page.cursor paging
                |> Option.map (Epoch.value >> int64)
                |> Option.defaultValue 0L

            let limit = Page.limit paging

            Sql.run
                context
                (postgres {
                    let! rows =
                        Sql.select (fun query token ->
                            selectTask query {
                                for t in fsm.transition do
                                    where (t.machine_id = machine && t.entity_id = entity && t.epoch > after)
                                    orderBy t.epoch
                                    take limit
                                    select t
                                    toList
                                    cancel token
                            })

                    return! rows |> List.traverseResultM toTransition
                })
                ct

    interface ICommandProcessorStore<'EntityId, 'State, 'Event, 'Action, 'Err> with

        member _.Commit(commandId, token, expected, draft, ct) =
            backgroundTaskResult {
                let! encoded = encodeDraft draft
                return! Sql.run context (finalize commandId token (Commit(expected, encoded))) ct
            }

        member _.Reject(commandId, token, error, ct) = fail Reject commandId token error ct

        member _.DeadLetter(commandId, token, error, ct) =
            fail DeadLetter commandId token error ct

        member _.TryGetResult(commandId, ct) =
            let id = CommandId.value commandId

            Sql.run
                context
                (postgres {
                    let! row =
                        Sql.select (fun query token ->
                            selectTask query {
                                for c in fsm.command do
                                    leftJoin t in fsm.transition on (c.command_id = t.Value.command_id)
                                    leftJoin e in fsm.command_error on (c.command_id = e.Value.command_id)
                                    where (c.command_id = id)
                                    select (c, t, e)
                                    tryHead
                                    cancel token
                            })

                    return! row |> Option.traverseResult toResult
                })
                ct

    interface IReplayStore<'EntityId, 'State, 'Event, 'Action> with

        member _.Suffix(machineId, entityId, from, limit, ct) =
            let machine = MachineId.value machineId
            let entity = options.EntityIdEncode entityId
            let since = Db.timestamp from

            Sql.run
                context
                (postgres {
                    let! suffix =
                        Sql.select (fun query token ->
                            selectTask query {
                                for t in fsm.transition do
                                    where (t.machine_id = machine && t.entity_id = entity && t.effective_at >= since)
                                    orderBy t.effective_at
                                    thenBy t.epoch
                                    take limit
                                    select t
                                    toList
                                    cancel token
                            })

                    // Retention purges an entity's oldest commands with their transitions, so a
                    // log that no longer starts at epoch 1, and whose oldest survivor is already
                    // past the corrected instant, may have lost events after it.
                    let! oldest =
                        Sql.select (fun query token ->
                            selectTask query {
                                for t in fsm.transition do
                                    where (t.machine_id = machine && t.entity_id = entity)
                                    orderBy t.epoch
                                    select t
                                    tryHead
                                    cancel token
                            })

                    let incomplete =
                        match oldest with
                        | Some first -> first.epoch > 1L && first.effective_at > since
                        | None -> false

                    let! transitions = suffix |> List.traverseResultM toTransition

                    return
                        { Transitions = transitions
                          Incomplete = incomplete }
                })
                ct

        member _.CommitCorrection(commandId, token, expected, commit, ct) =
            backgroundTaskResult {
                // Everything is encoded before anything is sent, so a state that will not encode
                // fails the correction rather than half-writing it.
                let! encoded = encodeDraft commit.Transition
                let! beliefs = BeliefPayload.encode options.StateCodec commit.Beliefs
                let draft = commit.Transition

                return!
                    Sql.run
                        context
                        (postgres {
                            let! outcome, epoch =
                                Sql.one
                                    (nameof FinalizeOutcome)
                                    finalizeCorrection
                                    [ "command_id", Param.Bigint(CommandId.value commandId)
                                      "lease_token", Param.Bigint(LeaseToken.value token)
                                      "expected_epoch", Param.Bigint(int64 (Epoch.value expected))
                                      "valid_from", Param.Timestamp commit.ValidFrom
                                      "beliefs", Param.Jsonb beliefs
                                      "instance_status", Param.Text(Db.instanceStatusToString draft.Status)
                                      "event", Param.Jsonb encoded.EventJson
                                      "from_state", Param.Jsonb encoded.FromJson
                                      "to_state", Param.Jsonb encoded.ToJson
                                      "handled_by", Param.Text(StateId.value draft.HandledBy)
                                      "exited", Param.TextArray(states draft.Exited)
                                      "entered", Param.TextArray(states draft.Entered) ]
                                    (fun reader -> Row.string reader "outcome", Row.int64 reader "epoch")

                            return! ProcessorMapping.outcomeFromString outcome epoch expected
                        })
                        ct
            }

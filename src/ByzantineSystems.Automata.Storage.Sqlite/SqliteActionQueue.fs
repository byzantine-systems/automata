namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Internal
open ByzantineSystems.Automata.Storage.Sqlite.Schema
open FsToolkit.ErrorHandling
open Microsoft.Data.Sqlite

/// <summary>What the action queue needs to bridge one machine's actions to <c>fsm_action</c>.</summary>
type ActionQueueOptions<'EntityId, 'Action> =
    {
        Context: SqliteContext
        ActionCodec: Codec<'Action>
        EntityIdEncode: 'EntityId -> string
        /// <summary>Decoding returns a result, so a corrupt row is a typed failure rather than an exception.</summary>
        EntityIdDecode: string -> Result<'EntityId, string>
    }

/// <summary>
/// The SQLite end of action delivery.
///
/// The queue is a table, <c>fsm_action</c>, keyed by the command that emitted each action and
/// its position in that command's list, and shared by every machine in the file: a claim names
/// its machine, and <c>fsm_action_claim_idx</c> leads with it. There is no queue to create or
/// name, so a machine whose chart emits nothing needs nothing configured.
///
/// Every write is fenced by the row's key and the claim's token together. A claim draws a fresh
/// token, so a worker whose lease expired and was taken over, or whose delivery it rescheduled,
/// changes nothing. <c>DeliveryCount</c> is the row's <c>read_ct</c>, which the backoff reads as
/// the attempt number.
/// </summary>
type SqliteActionQueue<'EntityId, 'Action>(options: ActionQueueOptions<'EntityId, 'Action>) =

    let context = options.Context

    let nextToken = StatementKey.create "system" "next_token" |> Sql.prepareProgram
    let claimActions = StatementKey.create "action" "claim" |> Sql.prepareProgram
    let completeAction = StatementKey.create "action" "complete" |> Sql.prepareProgram

    let rescheduleAction =
        StatementKey.create "action" "reschedule" |> Sql.prepareProgram

    let abandonAction = StatementKey.create "action" "abandon" |> Sql.prepareProgram

    let actionRow (reader: SqliteDataReader) : main.fsm_action =
        { command_id = Row.int64 reader "command_id"
          ordinal = Row.int64 reader "ordinal"
          machine_id = Row.string reader "machine_id"
          entity_id = Row.string reader "entity_id"
          epoch = Row.int64 reader "epoch"
          action = Row.string reader "action"
          visible_at = Row.int64 reader "visible_at"
          lease_token = Row.int64 reader "lease_token"
          read_ct = Row.int64 reader "read_ct"
          enqueued_at = Row.int64 reader "enqueued_at" }

    let toLeased (row: main.fsm_action) : Result<LeasedAction<'EntityId, 'Action>, StoreError> =
        result {
            let! entityId =
                row.entity_id
                |> options.EntityIdDecode
                |> Result.mapError (Db.decodeFailure "EntityId")

            let! action = options.ActionCodec.Decode row.action |> Result.mapError Db.toStoreError

            let! machineId =
                row.machine_id
                |> MachineId.tryCreate
                |> Result.mapError (Db.decodeFailure "MachineId")

            return
                { Work =
                    { MachineId = machineId
                      EntityId = entityId
                      CommandId = CommandId.ofInt64 row.command_id
                      Epoch = Db.epochOf row.epoch
                      Ordinal = int row.ordinal
                      Action = action }
                  Token = LeaseToken.ofInt64 row.lease_token
                  DeliveryCount = int row.read_ct
                  ExpiresAt = Instant.toDateTimeOffset row.visible_at }
        }

    /// The row a lease names and the token that fences it, as every fenced statement binds them.
    let fence (action: LeasedAction<'EntityId, 'Action>) : (string * Param) list =
        [ "@command_id", Param.Integer(CommandId.value action.Work.CommandId)
          "@ordinal", Param.Integer(int64 action.Work.Ordinal)
          "@lease_token", Param.Integer(LeaseToken.value action.Token) ]

    /// The token and the claimed rows, in the claim's own transaction. Decoding waits until it
    /// has committed, so the write transaction holds no codec work.
    let claim (machineId: MachineId) (batch: int) (lease: TimeSpan) (now: int64) =
        store {
            let! token = StoreProgram.queryOne nextToken [] (fun reader -> Ok(Row.int64 reader "value"))

            return!
                StoreProgram.queryRows
                    claimActions
                    [ "@machine_id", Param.Text(MachineId.value machineId)
                      "@batch", Param.Integer(int64 batch)
                      "@now", Param.Integer now
                      "@deadline", Param.Integer(Instant.add now lease)
                      "@lease_token", Param.Integer token ]
                    (actionRow >> Ok)
        }

    interface IActionQueue<'EntityId, 'Action> with

        member _.Claim(machineId, batch, lease, ct) =
            // Not idempotent, like the command claim: a claim whose outcome was lost may have
            // leased the rows, and repeating it leases more while the first batch stays invisible.
            // The dispatcher polls again shortly, which recovers faster.
            backgroundTaskResult {
                let! rows =
                    Sql.writeProgramOnceWith
                        context
                        (claim machineId (ClaimBatch.value batch) (LeaseDuration.value lease))
                        ct

                return! rows |> List.traverseResultM toLeased
            }

        member _.Complete(action, ct) =
            Sql.writeProgram
                context
                (StoreProgram.executeCount completeAction (fence action)
                 |> StoreProgram.map CommandMapping.leaseOutcome)
                ct

        member _.Reschedule(action, backoff, ct) =
            Sql.writeProgramWith
                context
                (fun now ->
                    StoreProgram.executeCount
                        rescheduleAction
                        (fence action
                         @ [ "@now", Param.Integer now
                             "@base_us", Param.Integer(CommandMapping.micros (Backoff.baseDelay backoff))
                             "@cap_us", Param.Integer(CommandMapping.micros (Backoff.ceiling backoff)) ])
                    |> StoreProgram.map CommandMapping.leaseOutcome)
                ct

        member _.Abandon(action, reason, ct) =
            Sql.writeProgramWith
                context
                (fun now ->
                    store {
                        let! recorded =
                            StoreProgram.executeCount
                                abandonAction
                                (fence action
                                 @ [ "@reason", Param.Text(Reason.value reason); "@now", Param.Integer now ])

                        match CommandMapping.leaseOutcome recorded with
                        | LeaseUpdateOutcome.LeaseLost -> return LeaseUpdateOutcome.LeaseLost
                        | Updated ->
                            do! StoreProgram.executeDiscard completeAction (fence action)
                            return Updated
                    })
                ct

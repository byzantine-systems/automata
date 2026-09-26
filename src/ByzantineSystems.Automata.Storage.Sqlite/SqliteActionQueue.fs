namespace ByzantineSystems.Automata.Storage.Sqlite

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
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

    let write work ct =
        Db.protect
            context.Resilience
            (fun token -> Db.writeTransaction context.ConnectionString context.Clock work token)
            ct

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

            return
                { Work =
                    { MachineId = MachineId.create row.machine_id
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
    let fence (action: LeasedAction<'EntityId, 'Action>) : (string * obj) list =
        [ "@command_id", box (CommandId.value action.Work.CommandId)
          "@ordinal", box action.Work.Ordinal
          "@lease_token", box (LeaseToken.value action.Token) ]

    interface IActionQueue<'EntityId, 'Action> with

        member _.Claim(machineId, batch, lease, ct) =
            if batch < 1 then
                invalidArg (nameof batch) "A claim batch size must be positive."

            if lease <= TimeSpan.Zero then
                invalidArg (nameof lease) "A lease must be a positive duration."

            // Not idempotent, like the command claim: a claim whose outcome was lost may have
            // leased the rows, and repeating it leases more while the first batch stays invisible.
            // The dispatcher polls again shortly, which recovers faster.
            backgroundTaskResult {
                let! rows =
                    Db.protectOnce
                        (fun token ->
                            Db.writeTransaction
                                context.ConnectionString
                                context.Clock
                                (fun conn transaction now cancel ->
                                    backgroundTask {
                                        let! leaseToken =
                                            Statement.tryOne
                                                conn
                                                transaction
                                                (SqlResources.get "system" "next_token")
                                                []
                                                (fun reader -> Row.int64 reader "value")
                                                cancel

                                        match leaseToken with
                                        | None ->
                                            return
                                                Error(
                                                    Db.decodeFailure "LeaseToken" "the lease token counter is missing"
                                                )
                                        | Some leaseToken ->
                                            let! rows =
                                                Statement.rows
                                                    conn
                                                    transaction
                                                    (SqlResources.get "action" "claim")
                                                    [ "@machine_id", box (MachineId.value machineId)
                                                      "@batch", box batch
                                                      "@now", box now
                                                      "@deadline", box (Instant.add now lease)
                                                      "@lease_token", box leaseToken ]
                                                    actionRow
                                                    cancel

                                            return Ok rows
                                    })
                                token)
                        ct

                return! rows |> List.traverseResultM toLeased
            }

        member _.Complete(action, ct) =
            write
                (fun conn transaction _ token ->
                    Statement.execute conn transaction (SqlResources.get "action" "complete") (fence action) token
                    |> Task.map (CommandMapping.leaseOutcome >> Ok))
                ct

        member _.Reschedule(action, backoff, ct) =
            write
                (fun conn transaction now token ->
                    Statement.execute
                        conn
                        transaction
                        (SqlResources.get "action" "reschedule")
                        (fence action
                         @ [ "@now", box now
                             "@base_us", box (CommandMapping.micros (Backoff.baseDelay backoff))
                             "@cap_us", box (CommandMapping.micros (Backoff.ceiling backoff)) ])
                        token
                    |> Task.map (CommandMapping.leaseOutcome >> Ok))
                ct

        member _.Abandon(action, reason, ct) =
            if String.IsNullOrWhiteSpace reason then
                invalidArg (nameof reason) "An abandoned delivery must say why."

            write
                (fun conn transaction now token ->
                    backgroundTask {
                        let! recorded =
                            Statement.execute
                                conn
                                transaction
                                (SqlResources.get "action" "abandon")
                                (fence action @ [ "@reason", box reason; "@now", box now ])
                                token

                        match CommandMapping.leaseOutcome recorded with
                        | LeaseLost -> return Ok LeaseLost
                        | Updated ->
                            let! _ =
                                Statement.execute
                                    conn
                                    transaction
                                    (SqlResources.get "action" "complete")
                                    (fence action)
                                    token

                            return Ok Updated
                    })
                ct

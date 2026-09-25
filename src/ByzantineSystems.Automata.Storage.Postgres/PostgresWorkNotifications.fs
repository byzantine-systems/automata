namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Threading
open System.Threading.Tasks
open System.Transactions
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Npgsql

/// <summary>
/// Where a machine's notification listener connects.
///
/// <c>LISTEN</c> belongs to a session, so it does not survive a transaction-mode pooler such as
/// PgBouncer in transaction mode or a managed "-pooler" endpoint: the session that ran it is
/// handed to somebody else after the statement, and the notifications go with it. A deployment
/// whose data source goes through one says where the server can be reached directly, or turns
/// listening off and relies on polling, which is always there underneath.
/// </summary>
[<RequireQualifiedAccess>]
type ListenerConnection =
    /// <summary>A connection from the store's own data source, held for as long as it listens.</summary>
    | SameDataSource

    /// <summary>
    /// A dedicated, unpooled connection to this connection string, for reaching the server around
    /// a transaction-mode pooler.
    /// </summary>
    | Direct of connectionString: string

    /// <summary>No listening. Work submitted elsewhere is found by polling alone.</summary>
    | Off

/// <summary>The pieces of one listening session, each small enough to read on its own.</summary>
[<RequireQualifiedAccess>]
module internal Listener =

    /// Long enough to cost nothing, short enough that a dead connection is noticed within a
    /// minute rather than never.
    let keepAlive = TimeSpan.FromSeconds 30.

    /// A dedicated connection that neither pools nor enlists, for going around a pooler.
    let openDirect (connectionString: string) (ct: CancellationToken) : Task<NpgsqlConnection> =
        backgroundTask {
            let builder =
                NpgsqlConnectionStringBuilder(connectionString, Pooling = false, Enlist = false)

            let conn = new NpgsqlConnection(builder.ConnectionString)
            do! conn.OpenAsync ct
            return conn
        }

    /// A connection from the data source, opened with any ambient transaction suppressed. A
    /// listening session inside a long transaction stops the server clearing its notification
    /// queue for everybody, and a full queue fails every commit that notifies.
    let openFrom (dataSource: NpgsqlDataSource) (ct: CancellationToken) : Task<NpgsqlConnection> =
        backgroundTask {
            // Awaited inside the scope, so the suppression holds for the whole open, including
            // the enlistment check that runs after the connection is acquired.
            use _ =
                new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled)

            return! dataSource.OpenConnectionAsync(ct).AsTask()
        }

    let execute (sql: string) (conn: NpgsqlConnection) (ct: CancellationToken) : Task<unit> =
        backgroundTask {
            use cmd = new NpgsqlCommand(sql, conn)
            let! _ = cmd.ExecuteNonQueryAsync ct
            return ()
        }

    /// Which callback a notification is for. Both channels are shared by every machine in the
    /// database, and a payload is a machine id or a queue name and never data, so the payload is
    /// how this machine hears only its own.
    let route
        (machine: string)
        (queue: string)
        (onCommands: unit -> unit)
        (onActions: unit -> unit)
        (notification: NpgsqlNotificationEventArgs)
        =
        match notification.Channel with
        | "fsm_command" when notification.Payload = machine -> onCommands ()
        | "fsm_action" when notification.Payload = queue -> onActions ()
        | _ -> ()

    /// One quiet interval: a notification arrives, or a round trip proves the connection is
    /// still there. Waiting with no timeout never returns on a connection that died without
    /// saying so, and a listener that hangs silently is worse than one that fails.
    let waitOrPing (conn: NpgsqlConnection) (ct: CancellationToken) : Task<bool> =
        backgroundTask {
            match! conn.WaitAsync(keepAlive, ct) with
            | true -> ()
            | false -> do! execute "SELECT 1" conn ct

            return true
        }

    /// Subscribes, says once that work may have been missed, then waits until cancelled.
    /// PostgreSQL keeps nothing for a session that was not connected, so the moment listening
    /// starts is the one moment this knows it may have missed an announcement.
    let run
        (conn: NpgsqlConnection)
        (handler: NpgsqlNotificationEventArgs -> unit)
        (missed: unit -> unit)
        (ct: CancellationToken)
        : Task<Result<unit, StoreError>> =
        backgroundTask {
            conn.Notification.Add handler
            do! execute "LISTEN fsm_command; LISTEN fsm_action" conn ct
            missed ()
            do! Recurring.repeat (fun () -> waitOrPing conn ct) ct
            return Ok()
        }

/// <summary>
/// Hearing <c>fsm.notify_pending</c> from another process.
///
/// Notifications are hints and nothing more: what is actually waiting is found by claiming, as
/// after a poll. The callbacks are raised on the thread reading the connection, in the middle of
/// a wait, so they must not block; the runtime's are a non-blocking write to a bounded channel.
///
/// It runs through <c>Db.protectOnce</c>, not the retrying pipeline. A listener that lost its
/// connection has nothing to repeat; the runtime waits and listens again.
/// </summary>
type PostgresWorkNotifications(context: PostgresContext, queue: string, connection: ListenerConnection) =

    let openConnection (ct: CancellationToken) =
        match connection with
        | ListenerConnection.Direct connectionString -> Listener.openDirect connectionString ct
        | ListenerConnection.SameDataSource
        | ListenerConnection.Off -> Listener.openFrom context.DataSource ct

    let session (machine: string) onCommands onActions (ct: CancellationToken) =
        backgroundTask {
            use! conn = openConnection ct
            let handler = Listener.route machine queue onCommands onActions

            let missed () =
                onCommands ()
                onActions ()

            return! Listener.run conn handler missed ct
        }

    interface IWorkNotifications with

        member _.Listen(machineId, onCommands, onActions, ct) =
            match connection with
            | ListenerConnection.Off -> Task.FromResult(Ok())
            | ListenerConnection.SameDataSource
            | ListenerConnection.Direct _ ->
                Db.protectOnce (session (MachineId.value machineId) onCommands onActions) ct

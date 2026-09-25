namespace ByzantineSystems.Automata.Storage.Postgres

open ByzantineSystems.Automata.Resilience
open Npgsql
open Polly

/// <summary>
/// The connection pool and the resilience applied to everything sent through it.
///
/// The two travel together because neither is useful alone: a pipeline whose breaker covers
/// only some of a database's traffic cannot judge that database's health, and a data source
/// with no policy retries nothing. Every store takes one of these rather than a bare data
/// source, so one database has one pool and one circuit.
/// </summary>
type PostgresContext =
    { DataSource: NpgsqlDataSource
      Resilience: ResiliencePipeline }

/// <summary>Builds the pooled data source the stores read and write through.</summary>
[<RequireQualifiedAccess>]
module DataSource =

    /// <summary>
    /// One data source per application, registered once and shared. It owns the connection pool,
    /// so building a second one for the same database doubles the pool without doubling the
    /// server's willingness to accept connections.
    /// </summary>
    /// <remarks>
    /// Automatic preparation is enabled deliberately. Every statement this assembly sends comes
    /// from a fixed set of embedded files, so the candidate set is small, bounded and known at
    /// build time, which is exactly the shape automatic preparation is good at. The claim runs on
    /// every worker poll and is the statement that benefits most.
    /// </remarks>
    let create (connectionString: string) : NpgsqlDataSource =
        let builder = NpgsqlDataSourceBuilder(connectionString)
        builder.ConnectionStringBuilder.MaxAutoPrepare <- 32
        builder.ConnectionStringBuilder.AutoPrepareMinUsages <- 2
        builder.Build()

    /// <summary>
    /// The resilience every call through a data source runs under, built once and shared so the
    /// circuit breaker sees the whole database's traffic rather than one method's.
    ///
    /// The driver's failures are classified here because this is the only assembly that knows
    /// what an <c>NpgsqlException</c> means. Pass <c>ignore</c> when no event sink is wanted.
    /// </summary>
    let resilience (policy: TransientPolicy) (onEvent: PipelineEventSink) : ResiliencePipeline =
        TransientPolicy.toPipeline policy "automata-postgres" Db.isTransient onEvent

/// <summary>Operations on <see cref="T:ByzantineSystems.Automata.Storage.Postgres.PostgresContext" />.</summary>
[<RequireQualifiedAccess>]
module PostgresContext =

    /// <summary>Pairs an existing data source with a policy of the caller's choosing.</summary>
    let create (dataSource: NpgsqlDataSource) (policy: TransientPolicy) (onEvent: PipelineEventSink) : PostgresContext =
        { DataSource = dataSource
          Resilience = DataSource.resilience policy onEvent }

    /// <summary>
    /// A context from a connection string alone, on the default policy and with no event sink.
    /// The data source it builds is owned by the caller, who disposes it.
    /// </summary>
    let ofConnectionString (connectionString: string) : PostgresContext =
        create (DataSource.create connectionString) TransientPolicy.defaults ignore

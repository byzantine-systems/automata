namespace ByzantineSystems.Automata.Storage.Postgres

open Npgsql

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

namespace ByzantineSystems.Automata.Storage.Postgres

/// <summary>The PostgreSQL storage profile. Snapshot is the default; bitemporal history is opt-in.</summary>
[<RequireQualifiedAccess>]
type BeliefModel =
    /// <summary>Current snapshots and the transition log.</summary>
    | Snapshot
    /// <summary>Current snapshots plus valid-time and system-time beliefs.</summary>
    | Bitemporal

/// <summary>Configuration spellings for the two PostgreSQL profiles.</summary>
[<RequireQualifiedAccess>]
module BeliefModel =
    /// <summary>The stable database and configuration spelling.</summary>
    let value =
        function
        | BeliefModel.Snapshot -> "snapshot"
        | BeliefModel.Bitemporal -> "bitemporal"

    /// <summary>Parses a profile without throwing.</summary>
    let tryCreate =
        function
        | "snapshot" -> Ok BeliefModel.Snapshot
        | "bitemporal" -> Ok BeliefModel.Bitemporal
        | name -> Error $"Unknown PostgreSQL belief model '{name}'. Expected snapshot or bitemporal."

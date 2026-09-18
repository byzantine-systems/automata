module ByzantineSystems.Automata.Migrate.Program

/// <summary>
/// Thin CLI that applies the ByzantineSystems.Automata PostgreSQL migrations.
/// Wired to real migrations (DbUp, main/repeatable split) in phase 5.
/// </summary>
[<EntryPoint>]
let main _ =
    printfn "automata-migrate: migrations arrive with phase 5 (ByzantineSystems.Automata.Storage.Postgres)."
    0

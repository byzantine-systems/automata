module ByzantineSystems.Automata.Migrate.Program

open System
open ByzantineSystems.Automata.Storage.Postgres

let private readConnectionString () : string option =
    let read name =
        Environment.GetEnvironmentVariable name
        |> Option.ofObj
        |> Option.filter (String.IsNullOrWhiteSpace >> not)

    match read "BS_AUTOMATA_CONN" with
    | Some value -> Some value
    | None -> read "ConnectionStrings__BS_AUTOMATA_CONN"

[<EntryPoint>]
let main _ =
    match readConnectionString () with
    | None ->
        eprintfn "No connection string: set BS_AUTOMATA_CONN or ConnectionStrings__BS_AUTOMATA_CONN."
        1
    | Some connectionString ->
        try
            Migrator.migrate connectionString
            printfn "Migrations applied."
            0
        with ex ->
            eprintfn "Migration failed: %s" ex.Message
            1

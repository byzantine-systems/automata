module ByzantineSystems.Automata.Storage.Postgres.Tests.ListenerTests

open System
open System.Threading
open Npgsql
open ByzantineSystems.Automata.Storage.Postgres
open Expecto

let tests =
    testList
        "Postgres listener"
        [ testCase "a failed direct open surfaces the failure and disposes the connection" (fun () ->
              let connection = new NpgsqlConnection()
              let mutable abandoned = 0
              let failure = InvalidOperationException("the server was unreachable")

              let openDirect =
                  Listener.openDirectWith (fun () -> connection) (fun _ _ -> raise failure) (fun _ ->
                      abandoned <- abandoned + 1)

              let run () =
                  (openDirect CancellationToken.None).GetAwaiter().GetResult() |> ignore

              Expect.throws run "the open failure reaches the caller"

              Expect.equal abandoned 1 "the abandoned connection was handed to cleanup exactly once") ]

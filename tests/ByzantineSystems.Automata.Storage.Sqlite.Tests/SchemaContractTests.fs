module ByzantineSystems.Automata.Storage.Sqlite.Tests.SchemaContractTests

open ByzantineSystems.Automata.Storage.Sqlite
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

/// A test against the shared database, emptied first.
let private fixture name (body: string -> unit) =
    testCase name
    <| fun _ ->
        reset ()
        body (shared ())

let private refuses (expected: int) (what: string) (action: unit -> unit) =
    Expect.equal (extendedError action) expected what

/// The invariants the stores rely on but never exercise, because they never attempt the write.
/// They are the boundary against any other writer: a second process on an older build, a manual
/// fix, a future bug. Delete one of these and no store test notices.
let private invariantTests =
    testList
        "invariants"
        [ fixture "an entity has at most one unblocked open command"
          <| fun db ->
              insertCommand db []

              refuses Code.constraintUnique "a second head" (fun () ->
                  insertCommand db [ "seq", box 2L; "idempotency_key", box "key-2" ])

              // Waiting its turn is how a second command is admitted.
              insertCommand db [ "seq", box 2L; "idempotency_key", box "key-2"; "blocked", box 1L ]

          fixture "a command may only pin a registered chart version"
          <| fun db ->
              refuses Code.constraintForeignKey "version 9 was never declared" (fun () ->
                  insertCommand db [ "chart_version", box 9L ])

          fixture "a resubmitted key is one command"
          <| fun db ->
              insertCommand db [ "status", box "succeeded"; "lease_token", box 1L ]

              refuses Code.constraintUnique "same entity, same key" (fun () ->
                  insertCommand db [ "seq", box 2L; "blocked", box 1L ]) ]

let private triggerTests =
    testList
        "guards"
        [ fixture "a terminal command is never updated"
          <| fun db ->
              insertCommand db [ "status", box "succeeded"; "lease_token", box 7L ]

              refuses Code.constraintTrigger "reopening a finished command" (fun () ->
                  exec db "UPDATE fsm_command SET status = 'ready', lease_token = 0;" [])

          fixture "a live command's lifecycle columns may move"
          <| fun db ->
              insertCommand db []
              exec db "UPDATE fsm_command SET status = 'leased', lease_token = 3, read_ct = 1, visible_at = 10;" []
              exec db "UPDATE fsm_command SET status = 'succeeded';" []
              Expect.equal (scalar db "SELECT status FROM fsm_command;") "succeeded" "claim, then finish"

          fixture "what was submitted is never rewritten"
          <| fun db ->
              insertCommand db []

              // Even to the same value: naming the column is the offence.
              refuses Code.constraintTrigger "the event" (fun () -> exec db "UPDATE fsm_command SET event = event;" [])

              refuses Code.constraintTrigger "the order" (fun () -> exec db "UPDATE fsm_command SET seq = 2;" [])

          fixture "the transition log is append-only"
          <| fun db ->
              insertCommand db [ "status", box "succeeded"; "lease_token", box 1L ]
              let id = commandId db "key-1"

              exec
                  db
                  "INSERT INTO fsm_transition (machine_id, entity_id, epoch, command_id, chart_version, event, actions,
                                               from_state, to_state, handled_by, exited, entered, status,
                                               effective_at, committed_at)
                   VALUES (@machine, 'entity-1', 1, @id, 1, '{}', '[]', '{}', '{}', 'root', '[]', '[]', 'running', 0, 0);"
                  [ "@machine", box machine; "@id", box id ]

              refuses Code.constraintTrigger "editing history" (fun () ->
                  exec db "UPDATE fsm_transition SET to_state = '{\"edited\":true}';" [])

          fixture "retention may still delete a finished command"
          <| fun db ->
              insertCommand db [ "status", box "dead_letter"; "lease_token", box 2L ]
              exec db "DELETE FROM fsm_command;" []
              Expect.equal (scalar db "SELECT count(*) FROM fsm_command;") "0" "deletes are not guarded" ]

/// The access paths the stores will depend on, pinned as the planner chooses them today. A
/// statement that still returns the right rows through a scan fails nothing else, so these are
/// what notice when an index and its query drift apart.
let private planTests =
    let noSort (steps: string list) =
        Expect.isFalse (steps |> List.exists (fun step -> step.Contains "TEMP B-TREE")) "no sort step"

    testList
        "query plans"
        [ fixture "the shipped claim is served by the claim index, in order"
          <| fun db ->
              let steps = plan db (SqlResources.get "command" "claim")

              Expect.exists steps (fun step -> step.Contains "USING INDEX fsm_command_claim_idx") "the claim index"
              noSort steps

          fixture "the shipped promotion reads the first open command, not the history"
          <| fun db ->
              let steps = plan db (SqlResources.get "command" "promote_head")

              Expect.exists steps (fun step -> step.Contains "fsm_command_entity_open_idx") "the open index"
              noSort steps

          fixture "the shipped action claim seeks the machine's due actions"
          <| fun db ->
              let steps = plan db (SqlResources.get "action" "claim")

              // A covering index today: the candidates are rowids, which the index carries.
              Expect.exists steps (fun step -> step.Contains "INDEX fsm_action_claim_idx") "the claim index"
              noSort steps

          fixture "the shipped submission finds the entity's last seq and open sibling by index"
          <| fun db ->
              let steps = plan db (SqlResources.get "command" "submit")

              Expect.isFalse
                  (steps |> List.exists (fun step -> step.StartsWith "SCAN fsm_command"))
                  "no scan of the inbox"

          fixture "a history page seeks the log"
          <| fun db ->
              let steps =
                  plan
                      db
                      "SELECT * FROM fsm_transition
                       WHERE machine_id = @machine AND entity_id = @entity AND epoch > @after
                       ORDER BY epoch LIMIT @limit"

              Expect.exists steps (fun step -> step.StartsWith "SEARCH fsm_transition") "a seek, not a scan"
              noSort steps

          fixture "deleting a command finds its snapshot by index"
          <| fun db ->
              // The lookup SQLite runs for the foreign key when retention deletes a command.
              let steps = plan db "SELECT 1 FROM fsm_entity_snapshot WHERE command_id = @command"
              Expect.exists steps (fun step -> step.Contains "fsm_entity_snapshot_command_idx") "the child-side index" ]

[<Tests>]
let tests =
    testList "Schema contract" [ invariantTests; triggerTests; planTests ]
    |> testSequenced

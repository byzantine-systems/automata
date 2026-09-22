module ByzantineSystems.Automata.Storage.Postgres.Tests.SchemaContractTests

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Npgsql
open TestContext

/// The columns PostgresCommandInbox reads by name. Without code generation, this list is the
/// contract: if the schema drifts away from it, these tests fail here rather than at a runtime
/// column lookup in production.
let private commandColumns =
    [ "command_id"
      "machine_id"
      "entity_id"
      "seq"
      "idempotency_key"
      "chart_version"
      "event"
      "status"
      "blocked"
      "visible_at"
      "lease_token"
      "read_ct"
      "attempts"
      "received_at"
      "tenant"
      "principal"
      "source"
      "correlation_id"
      "causation_id" ]

/// Builds the plan probe from the routine's own body, read back out of the catalog. Copying the
/// statement into the test instead would let the two drift, which is exactly the failure this
/// test exists to catch.
let private explainClaim () : string =
    use conn = (dataSource ()).OpenConnection()

    let body =
        use read =
            new NpgsqlCommand(
                "SELECT prosrc FROM pg_proc WHERE oid = 'fsm.claim_commands(text,integer,interval)'::regprocedure",
                conn
            )

        read.ExecuteScalar() :?> string

    use prepare =
        new NpgsqlCommand($"PREPARE claim_probe (text, integer, interval) AS {body}", conn)

    prepare.ExecuteNonQuery() |> ignore

    use explain =
        new NpgsqlCommand(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE claim_probe('plan-probe', 1, interval '30 seconds')",
            conn
        )

    explain.ExecuteScalar() |> string

let tests =
    testList
        "Postgres schema contract"
        [ testTask "no column in the fsm schema is nullable" {
              do! reset ()

              Expect.isEmpty
                  (nullableColumns ())
                  "absence in this schema is always a value, so that a CHECK cannot pass by being unknown"
          }

          testTask "the claim returns exactly the columns the reader expects" {
              do! reset ()

              let columns =
                  columnsOf
                      (SqlResources.get "command" "claim")
                      [ "machine_id", box "none"
                        "batch", box 1
                        "lease", box (TimeSpan.FromSeconds 30.0) ]

              Expect.equal columns commandColumns "the claim result is the inbox reader's contract"
          }

          testTask "the by-id read returns exactly the columns the reader expects" {
              do! reset ()

              let columns =
                  columnsOf (SqlResources.get "command" "by_id") [ "command_id", box 0L ]

              Expect.equal columns commandColumns "the single-command read is the same contract"
          }

          testTask "a blocked command can never be leased" {
              do! reset ()
              let inbox = newInbox ()
              let! _ = inbox.Submit(submission (entityId "e1") "k1" Finish, noCancellation)
              let! _ = inbox.Submit(submission (entityId "e1") "k2" Finish, noCancellation)

              // The non-clamped check: derived state that drifts must fail loudly at the write.
              match exec "UPDATE fsm.command SET status = 'leased', lease_token = 1 WHERE seq = 2" with
              | Ok() -> failtest "a blocked command should not be allowed into the leased state"
              | Error error ->
                  Expect.stringContains
                      error.Message
                      "command_blocked_is_ready"
                      "the constraint should name itself in the failure"
          }

          testTask "an entity cannot have two claimable commands" {
              do! reset ()
              let inbox = newInbox ()
              let! _ = inbox.Submit(submission (entityId "e1") "k1" Finish, noCancellation)
              let! _ = inbox.Submit(submission (entityId "e1") "k2" Finish, noCancellation)

              match exec "UPDATE fsm.command SET blocked = false WHERE seq = 2" with
              | Ok() -> failtest "two unblocked commands for one entity should be impossible"
              | Error error ->
                  Expect.stringContains
                      error.Message
                      "command_entity_head_idx"
                      "the head invariant is enforced by a unique index, not by convention"
          }

          testTask "a ready command cannot carry a lease token" {
              do! reset ()
              let inbox = newInbox ()
              let! _ = inbox.Submit(submission (entityId "e1") "k1" Finish, noCancellation)

              match exec "UPDATE fsm.command SET lease_token = 5 WHERE seq = 1" with
              | Ok() -> failtest "a ready command holding a token should be impossible"
              | Error error ->
                  Expect.stringContains error.Message "command_lease_token_matches_status" "status and token must agree"
          }

          testTask "the claim uses its index and evaluates the candidate set once" {
              do! reset ()

              // Enough rows that a sequential scan would be the cheaper plan if the index were
              // unusable, so this fails if the predicate ever drifts from command_claim_idx.
              exec
                  """INSERT INTO fsm.command (machine_id, entity_id, seq, idempotency_key, chart_version, event, blocked)
                     SELECT 'plan-probe', 'e' || g, 1, 'k' || g, 1, '{}'::jsonb, false FROM generate_series(1, 20000) g"""
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "seeding failed: %s" error.Message

              exec "ANALYZE fsm.command"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "analyze failed: %s" error.Message

              let plan = explainClaim ()

              Expect.stringContains plan "command_claim_idx" "the claim must be served by its partial index"

              Expect.isFalse (plan.Contains "Seq Scan") "a sequential scan means the predicate drifted from the index"

              Expect.isFalse
                  (plan.Contains "\"Node Type\": \"Sort\"")
                  "the index order must satisfy ORDER BY without a sort"

              // The regression the materialised CTE prevents: an inlined candidate set is
              // rescanned per outer row, and the Limit node then reports more than one loop.
              let limitLoops =
                  plan.Split "\"Node Type\": \"Limit\""
                  |> Array.skip 1
                  |> Array.map (fun section ->
                      let marker = "\"Actual Loops\": "
                      let start = section.IndexOf marker + marker.Length
                      let rest = section.Substring start
                      rest.Substring(0, rest.IndexOfAny [| ','; '\n'; '}' |]).Trim())

              Expect.isNonEmpty limitLoops "the plan should contain a Limit node"

              for loops in limitLoops do
                  Expect.equal loops "1" "the candidate set must be evaluated exactly once"
          } ]

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

          testTask "the chart version read returns exactly the column the reader expects" {
              do! reset ()

              let columns =
                  columnsOf (SqlResources.get "chart" "by_version") [ "machine_id", box "none"; "version", box 1 ]

              Expect.equal columns [ "fingerprint" ] "the registry reads this column by name"
          }

          testTask "a chart version below one is refused" {
              do! reset ()

              match
                  exec
                      "INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint) VALUES ('m', 0, repeat('a', 64))"
              with
              | Ok() -> failtest "version 0 should be impossible"
              | Error error ->
                  Expect.stringContains
                      error.Message
                      "machine_chart_version_positive"
                      "versions are declared from one upwards"
          }

          testTask "a fingerprint that is not a digest is refused" {
              do! reset ()

              // Accepting a truncated or upper-cased hash would store something that mismatches
              // every chart forever, and an operator reading that mismatch could not tell it from
              // a chart somebody really did edit.
              for bad in [ "abc"; String.replicate 64 "A"; String.replicate 63 "a"; "" ] do
                  match
                      exec
                          $"INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint) VALUES ('m-{bad.Length}', 1, '{bad}')"
                  with
                  | Ok() -> failtestf "'%s' should not be a valid fingerprint" bad
                  | Error error ->
                      Expect.stringContains
                          error.Message
                          "machine_chart_version_fingerprint_shape"
                          "the shape check is what names the actual fault"
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
              // unusable, so this fails if the predicate ever drifts from command_claim_idx. The
              // probe machine declares a chart version of its own because a command may only pin
              // one that exists.
              exec
                  """INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint)
                     VALUES ('plan-probe', 1, repeat('a', 64));

                     INSERT INTO fsm.command (machine_id, entity_id, seq, idempotency_key, chart_version, event, blocked)
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

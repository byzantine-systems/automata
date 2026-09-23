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

          testTask "the belief reads return exactly the columns they promise" {
              do! reset ()

              // The live read carries what a snapshot needs; the as-of read is the time-travel
              // shape and deliberately does not, because a superseded belief's epoch is not the
              // entity's current one.
              Expect.equal
                  (columnsOf (SqlResources.get "belief" "live") [ "machine_id", box "none"; "entity_id", box "none" ])
                  [ "machine_id"
                    "entity_id"
                    "state"
                    "status"
                    "epoch"
                    "command_id"
                    "chart_version"
                    "valid_during"
                    "system_time" ]
                  "the live read is what IStateReader.TryGetSnapshot maps"

              Expect.equal
                  (columnsOf
                      (SqlResources.get "belief" "as_of")
                      [ "machine_id", box "none"
                        "entity_id", box "none"
                        "valid_at", box DateTime.UtcNow
                        "known_at", box DateTime.UtcNow ])
                  [ "machine_id"; "entity_id"; "state"; "valid_during"; "system_time" ]
                  "the as-of read is the time-travel shape"
          }

          testTask "the live belief read uses its partial index" {
              do! reset ()

              // Ten superseded valid-time versions per entity plus one live one, which is what
              // this table looks like in use. The shape matters: with a single version per
              // entity the temporal key is the same size as the partial index and the planner
              // reasonably picks either. Once history accumulates, the partial index stays
              // proportional to the live frontier while the key grows with every version.
              exec
                  """INSERT INTO fsm.instance_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, valid_during)
                     SELECT 'plan-probe', 'e' || e, '{}'::jsonb, 'running', 1, 1, 1,
                            tstzrange('2026-01-01'::timestamptz + (v || ' days')::interval,
                                      '2026-01-01'::timestamptz + ((v + 1) || ' days')::interval, '[)')
                     FROM generate_series(1, 2000) e, generate_series(0, 9) v;

                     INSERT INTO fsm.instance_state (machine_id, entity_id, state, status, epoch, command_id, chart_version, valid_during)
                     SELECT 'plan-probe', 'e' || e, '{}'::jsonb, 'running', 1, 1, 1, tstzrange('2026-03-01', 'infinity', '[)')
                     FROM generate_series(1, 2000) e"""
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "seeding failed: %s" error.Message

              exec "ANALYZE fsm.instance_state"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "analyze failed: %s" error.Message

              let plan =
                  use conn = (dataSource ()).OpenConnection()

                  use cmd =
                      new NpgsqlCommand(
                          "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                          + (SqlResources.get "belief" "live")
                              .Replace("@machine_id", "'plan-probe'")
                              .Replace("@entity_id", "'e1234'"),
                          conn
                      )

                  cmd.ExecuteScalar() |> string

              Expect.stringContains plan "instance_state_live_idx" "the current-state read must use the partial index"

              Expect.isFalse
                  (plan.Contains "Seq Scan")
                  "a sequential scan means the predicate drifted from the index predicate"
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

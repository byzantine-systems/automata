module ByzantineSystems.Automata.Storage.Postgres.Tests.TemporalTests

open System
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open TestContext

/// Writes a belief directly, bypassing fsm.close_and_open, so that the trigger can be tested
/// without the split routine's behaviour mixed in.
let private insertBelief (entity: string) (state: string) (validFrom: string) =
    exec
        $"INSERT INTO fsm.instance_state (machine_id, entity_id, state, valid_during)
          VALUES ('pg-tests', '{entity}', '{state}'::jsonb, tstzrange('{validFrom}', 'infinity', '[)'))"
    |> function
        | Ok() -> ()
        | Error error -> failtestf "insert failed: %s" error.Message

let private closeAndOpen (entity: string) (at: string) (state: string) : Result<string, exn> =
    try
        Ok(
            scalar<string>
                $"SELECT outcome FROM fsm.close_and_open('pg-tests', '{entity}', '{at}'::timestamptz, '{state}'::jsonb)"
        )
    with error ->
        Error error

let tests =
    testList
        "Postgres temporal versioning"
        [

          testTask "an insert opens a belief and archives nothing" {
              do! reset ()
              insertBelief "e1" "{\"v\":1}" "2026-01-01"

              Expect.equal
                  (scalar<bool>
                      "SELECT upper(system_time) = 'infinity' AND lower_inc(system_time) FROM fsm.instance_state")
                  true
                  "a new belief is open-ended and half-open"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history")
                  0L
                  "nothing has been superseded yet"
          }

          // The clock_timestamp() failure mode. With a volatile clock the archived upper bound
          // and the live lower bound come from two separate calls, so they differ by microseconds
          // and leave a gap that an as-of query inside it answers with nothing at all.
          testTask "the archived belief ends exactly where the new one begins" {
              do! reset ()
              insertBelief "e1" "{\"v\":1}" "2026-01-01"

              exec "UPDATE fsm.instance_state SET state = '{\"v\":2}'"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "update failed: %s" error.Message

              Expect.equal
                  (scalar<bool>
                      "SELECT h.system_time -|- s.system_time AND upper(h.system_time) = lower(s.system_time)
                       FROM fsm.instance_state_history h, fsm.instance_state s")
                  true
                  "the two windows must be adjacent, with no gap and no overlap"
          }

          // The now() failure mode. Transaction time is frozen, so the archived row would be
          // closed at its own lower bound and the belief window would be empty and invisible.
          testTask "two statements in one transaction produce two distinct beliefs" {
              do! reset ()

              exec
                  "BEGIN;
                   INSERT INTO fsm.instance_state (machine_id, entity_id, state, valid_during)
                   VALUES ('pg-tests', 'e1', '{\"v\":1}'::jsonb, tstzrange('2026-01-01', 'infinity', '[)'));
                   UPDATE fsm.instance_state SET state = '{\"v\":2}' WHERE entity_id = 'e1';
                   COMMIT;"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "transaction failed: %s" error.Message

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.instance_state_history")
                  1L
                  "the superseded belief is archived even within one transaction"

              Expect.equal
                  (scalar<bool>
                      "SELECT NOT isempty(system_time) AND upper(system_time) > lower(system_time)
                       FROM fsm.instance_state_history")
                  true
                  "belief time advances between statements, so the archived window is not empty"
          }

          // One statement is one change of belief. clock_timestamp() would stamp a separate
          // instant per row here, which is the same defect as above seen from the other side.
          testTask "one statement updating many rows stamps one instant" {
              do! reset ()

              for i in 1..5 do
                  insertBelief $"e{i}" "{\"v\":1}" "2026-01-01"

              exec "UPDATE fsm.instance_state SET state = '{\"v\":2}'"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "update failed: %s" error.Message

              Expect.equal
                  (scalar<int64> "SELECT count(DISTINCT upper(system_time)) FROM fsm.instance_state_history")
                  1L
                  "five archived rows, one belief instant"
          }

          testTask "a delete archives the belief rather than losing it" {
              do! reset ()
              insertBelief "e1" "{\"v\":1}" "2026-01-01"

              exec "DELETE FROM fsm.instance_state WHERE entity_id = 'e1'"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "delete failed: %s" error.Message

              Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.instance_state") 0L "the live row is gone"

              Expect.equal
                  (scalar<int64>
                      "SELECT count(*) FROM fsm.instance_state_history WHERE upper(system_time) <> 'infinity'")
                  1L
                  "and it is closed in history"
          }

          // An empty range normalises to 'empty', whose lower_inc() is false, so the half-open
          // check rejects it. The history twin has no temporal key, so on that table the copied
          // CHECK is the only thing between an empty range and a row nothing can read back.
          testTask "an empty valid_during is refused on both tables" {
              do! reset ()

              for table in [ "fsm.instance_state"; "fsm.instance_state_history" ] do
                  match
                      exec
                          $"INSERT INTO {table} (machine_id, entity_id, state, valid_during)
                            VALUES ('pg-tests', 'e1', '{{}}'::jsonb, tstzrange('2026-01-01', '2026-01-01', '[)'))"
                  with
                  | Ok() -> failtestf "an empty valid-time range should be impossible in %s" table
                  | Error error -> Expect.stringContains error.Message "instance_state_valid_half_open" $"in {table}"
          }

          // Two different defences, because the two tables are written differently. On the live
          // table the trigger owns system_time and overwrites whatever a caller supplies, so a
          // bad value never reaches the constraint. The history twin is written by the trigger
          // itself, so there the CHECK is all there is.
          testTask "the trigger overrides a supplied system_time on the live table" {
              do! reset ()

              exec
                  "INSERT INTO fsm.instance_state (machine_id, entity_id, state, valid_during, system_time)
                   VALUES ('pg-tests', 'e1', '{}'::jsonb, tstzrange('2026-01-01', 'infinity', '[)'),
                           tstzrange('2026-01-01', '2026-01-01', '[)'))"
              |> function
                  | Ok() -> ()
                  | Error error -> failtestf "insert failed: %s" error.Message

              Expect.equal
                  (scalar<bool>
                      "SELECT upper(system_time) = 'infinity' AND lower(system_time) > '2026-01-01'
                       FROM fsm.instance_state")
                  true
                  "belief time is the trigger's to decide, not the caller's"
          }

          testTask "an empty system_time is refused in history" {
              do! reset ()

              match
                  exec
                      "INSERT INTO fsm.instance_state_history (machine_id, entity_id, state, valid_during, system_time)
                       VALUES ('pg-tests', 'e1', '{}'::jsonb, tstzrange('2026-01-01', 'infinity', '[)'),
                               tstzrange('2026-01-01', '2026-01-01', '[)'))"
              with
              | Ok() -> failtest "an empty belief window should be impossible"
              | Error error ->
                  Expect.stringContains
                      error.Message
                      "instance_state_system_half_open"
                      "no trigger guards this table, so the CHECK is the only defence"
          }

          testTask "an inclusive upper bound is refused" {
              do! reset ()

              match
                  exec
                      "INSERT INTO fsm.instance_state (machine_id, entity_id, state, valid_during)
                       VALUES ('pg-tests', 'e1', '{}'::jsonb, tstzrange('2026-01-01', '2026-02-01', '[]'))"
              with
              | Ok() -> failtest "only half-open ranges should be storable"
              | Error error ->
                  Expect.stringContains
                      error.Message
                      "instance_state_valid_half_open"
                      "two spellings of one interval would make every as-of test ambiguous"
          }

          testTask "two overlapping beliefs for one entity are refused" {
              do! reset ()
              insertBelief "e1" "{\"v\":1}" "2026-01-01"

              match
                  exec
                      "INSERT INTO fsm.instance_state (machine_id, entity_id, state, valid_during)
                       VALUES ('pg-tests', 'e1', '{}'::jsonb, tstzrange('2026-06-01', 'infinity', '[)'))"
              with
              | Ok() -> failtest "one entity cannot hold two beliefs about the same instant"
              | Error error ->
                  Expect.stringContains error.Message "instance_state_pkey" "the temporal key refuses the overlap"
          }

          testTask "close_and_open opens the first belief" {
              do! reset ()

              Expect.equal (closeAndOpen "e1" "2026-01-01" "{\"v\":1}") (Ok "opened") "nothing to supersede"

              Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.instance_state") 1L "one live belief"
          }

          testTask "close_and_open splits at a later instant" {
              do! reset ()
              closeAndOpen "e1" "2026-01-01" "{\"v\":1}" |> ignore

              Expect.equal
                  (closeAndOpen "e1" "2026-02-01" "{\"v\":2}")
                  (Ok "split")
                  "the live belief is closed, not replaced"

              Expect.equal
                  (scalar<string>
                      "SELECT string_agg(state::text, ' | ' ORDER BY lower(valid_during)) FROM fsm.instance_state")
                  "{\"v\": 1} | {\"v\": 2}"
                  "both valid-time versions survive"

              Expect.equal
                  (scalar<bool>
                      "SELECT count(*) = 1 AND bool_and(upper(valid_during) = 'infinity') FROM fsm.instance_state
                       WHERE upper(valid_during) = 'infinity'")
                  true
                  "exactly one of them is live"
          }

          // Closing [t0, infinity) at t0 would write an empty range, so the routine replaces the
          // belief instead. The delete still runs through the trigger, so the superseded belief
          // is archived rather than discarded.
          testTask "close_and_open replaces when the instant equals the live lower bound" {
              do! reset ()
              closeAndOpen "e1" "2026-01-01" "{\"v\":1}" |> ignore

              Expect.equal (closeAndOpen "e1" "2026-01-01" "{\"v\":2}") (Ok "replaced") "same instant, so no split"

              Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.instance_state") 1L "one belief, not two"

              Expect.equal
                  (scalar<string> "SELECT state::text FROM fsm.instance_state_history")
                  "{\"v\": 1}"
                  "the superseded belief is archived, never overwritten"
          }

          testTask "close_and_open refuses an instant before the live belief" {
              do! reset ()
              closeAndOpen "e1" "2026-02-01" "{\"v\":1}" |> ignore

              match closeAndOpen "e1" "2026-01-01" "{\"v\":2}" with
              | Ok outcome -> failtestf "a back-dated correction should not silently produce %s" outcome
              | Error error ->
                  Expect.stringContains
                      error.Message
                      "precedes the live belief"
                      "reconciling it is correction replay, which this routine does not do"
          }

          // The operator scenario from the design doc: the same moment in the world, two
          // different opinions about it, and the difference is the explanation.
          testTask "as_of separates what was true from what was believed" {
              do! reset ()
              closeAndOpen "e1" "2026-01-01" "{\"v\":1}" |> ignore

              let believedFirst = scalar<DateTime> "SELECT statement_timestamp()"

              closeAndOpen "e1" "2026-01-01" "{\"v\":2}" |> ignore

              // The shipped file, run as shipped. Only the parameters differ between the two
              // readings, and one of those parameters is the whole question.
              let validAt = DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc)

              let readAsOf (knownAt: DateTime) =
                  rows
                      (SqlResources.get "belief" "as_of")
                      [ "machine_id", box "pg-tests"
                        "entity_id", box "e1"
                        "valid_at", box validAt
                        "known_at", box knownAt ]
                      (fun reader -> Row.string reader "state")

              Expect.equal
                  (readAsOf believedFirst)
                  [ "{\"v\": 1}" ]
                  "what we believed about that instant before the correction"

              Expect.equal
                  (readAsOf (DateTime.UtcNow.AddMinutes 1.0))
                  [ "{\"v\": 2}" ]
                  "and what we believe about the same instant now"
          } ]

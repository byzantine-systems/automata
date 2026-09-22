module ByzantineSystems.Automata.Storage.Postgres.Tests.ChartRegistryTests

open System
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open Npgsql
open TestContext

/// Distinct, well-formed fingerprints. These stand in for real charts: what the registry does
/// with a fingerprint does not depend on which chart produced it, and Core's own suite is where
/// the hashing is tested.
let private fingerprintA = ChartFingerprint.create (String.replicate 64 "b")
let private fingerprintB = ChartFingerprint.create (String.replicate 64 "c")

let private identity version fingerprint =
    { MachineId = machine
      Version = ChartVersion.create version
      Fingerprint = fingerprint }

let private register (registry: IChartRegistry) version fingerprint : Task<ChartRegistration> =
    task {
        let! outcome = registry.Register(identity version fingerprint, noCancellation)
        return outcome |> expectOk $"register version {version}"
    }

/// Waits until a registration is actually blocked on a lock, and fails if none ever is.
///
/// This is what makes the race test below a test. Firing N registrations in parallel and hoping
/// they overlap does not work: an earlier version of that test passed against a deliberately
/// broken routine, because the first call finished before the others reached the server. Proving
/// the block happened is the difference between exercising the race and hoping for it.
let private waitForBlockedRegistration () : Task =
    task {
        let deadline = DateTime.UtcNow.AddSeconds 10.0
        let mutable blocked = false

        while not blocked && DateTime.UtcNow < deadline do
            // pg_backend_pid() excludes this very query, whose own text would otherwise match.
            let waiting =
                scalar<int64>
                    "SELECT count(*) FROM pg_stat_activity
                     WHERE wait_event_type = 'Lock'
                       AND pid <> pg_backend_pid()
                       AND query LIKE '%register_chart_version%'"

            if waiting > 0L then blocked <- true else do! Task.Delay 25

        if not blocked then
            failtest "no registration ever blocked, so the race this test exists for did not happen"
    }

let tests =
    testList
        "Postgres IChartRegistry"
        [

          testTask "a version is claimed once and then only confirmed" {
              do! reset ()
              let registry = newRegistry ()

              let! first = register registry 2 fingerprintA
              let! second = register registry 2 fingerprintA

              Expect.equal first ChartRegistration.Registered "the first registration claims the version"
              Expect.equal second ChartRegistration.Matched "the second confirms it"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.machine_chart_version WHERE version = 2")
                  1L
                  "confirming must not write a second row"
          }

          testTask "a different chart under a claimed version is reported, not accepted" {
              do! reset ()
              let registry = newRegistry ()

              let! _ = register registry 2 fingerprintA
              let! outcome = register registry 2 fingerprintB

              Expect.equal
                  outcome
                  (ChartRegistration.Mismatched fingerprintA)
                  "the mismatch carries what is stored, which is what an operator needs"

              Expect.equal
                  (scalar<string> "SELECT fingerprint FROM fsm.machine_chart_version WHERE version = 2")
                  (ChartFingerprint.value fingerprintA)
                  "a registration never overwrites the stored fingerprint"
          }

          testTask "versions and machines are independent" {
              do! reset ()
              let registry = newRegistry ()

              let! two = register registry 2 fingerprintA
              let! three = register registry 3 fingerprintB

              Expect.equal two ChartRegistration.Registered "version 2"
              Expect.equal three ChartRegistration.Registered "a different version of the same machine"

              let! elsewhere =
                  registry.Register(
                      { MachineId = machineId "other-machine"
                        Version = ChartVersion.create 2
                        Fingerprint = fingerprintB },
                      noCancellation
                  )

              Expect.equal
                  (elsewhere |> expectOk "register for another machine")
                  ChartRegistration.Registered
                  "another machine's version 2 is a different fact"
          }

          testTask "a version nothing registered is absent rather than empty" {
              do! reset ()
              let registry = newRegistry ()

              let! missing = registry.TryGet(machine, ChartVersion.create 9, noCancellation)
              Expect.equal (missing |> expectOk "read an unregistered version") None "nothing is registered"

              let! _ = register registry 9 fingerprintA
              let! found = registry.TryGet(machine, ChartVersion.create 9, noCancellation)

              Expect.equal
                  (found |> expectOk "read a registered version")
                  (Some fingerprintA)
                  "reading must not claim anything of its own"
          }

          // The reason fsm.register_chart_version re-reads in a second statement instead of
          // deciding from one clever CTE. Every process starts at once after a deployment, so
          // losing this race is ordinary rather than exotic, and a loser reporting 'mismatched'
          // for a chart identical to the winner's would fail startup fleet-wide.
          //
          // Written against a single-statement version of the routine, this fails with
          // 'mismatched': ON CONFLICT tests uniqueness against the latest row version, while a
          // sibling SELECT in the same statement reads the statement's snapshot, so the loser
          // finds its insert skipped and still sees no row to compare against.
          testTask "a process that loses the race is told its chart matched" {
              do! reset ()
              let registry = newRegistry ()

              // The winner, holding an uncommitted row for the version the loser wants.
              use! holder = (dataSource ()).OpenConnectionAsync(noCancellation).AsTask()
              let holder: NpgsqlConnection = holder
              use transaction = holder.BeginTransaction()

              use insert =
                  new NpgsqlCommand(
                      "INSERT INTO fsm.machine_chart_version (machine_id, version, fingerprint)
                       VALUES (@machine_id, 4, @fingerprint)",
                      holder,
                      transaction
                  )

              insert.Parameters.AddWithValue("machine_id", MachineId.value machine) |> ignore

              insert.Parameters.AddWithValue("fingerprint", ChartFingerprint.value fingerprintA)
              |> ignore

              let! _ = insert.ExecuteNonQueryAsync noCancellation

              // The loser blocks on the winner's uncommitted row until the commit below.
              let loser = register registry 4 fingerprintA
              do! waitForBlockedRegistration ()
              do! transaction.CommitAsync noCancellation

              let! outcome = loser

              Expect.equal outcome ChartRegistration.Matched "losing the race to an identical chart is not a mismatch"

              Expect.equal
                  (scalar<int64> "SELECT count(*) FROM fsm.machine_chart_version WHERE version = 4")
                  1L
                  "one row, however many racers"
          }

          testTask "a command cannot pin a version no chart declared" {
              do! reset ()
              let inbox = newInbox ()

              let unregistered =
                  { submission (entityId "e1") "k1" (Start 1) with
                      ChartVersion = ChartVersion.create 7 }

              let! outcome = inbox.Submit(unregistered, noCancellation)

              match outcome with
              | Error(StoreError.NotFound entity) ->
                  Expect.stringContains entity "7" "the error should name the version that is missing"
              | other -> failtestf "expected NotFound, got %A" other

              Expect.equal (scalar<int64> "SELECT count(*) FROM fsm.command") 0L "nothing should have been written"

              let registry = newRegistry ()
              let! _ = register registry 7 fingerprintA
              let! accepted = inbox.Submit(unregistered, noCancellation)

              match accepted |> expectOk "submit once the version is registered" with
              | Accepted _ -> ()
              | other -> failtestf "expected Accepted, got %A" other
          }

          // There is no routine for forgetting a registration, and this is why one would not
          // help: a version any command has referenced cannot be removed anyway. The local
          // remedy for a mismatch is to bump the version or run make db-reset.
          testTask "a referenced version cannot be deleted" {
              do! reset ()
              let inbox = newInbox ()
              let! _ = inbox.Submit(submission (entityId "e1") "k1" (Start 1), noCancellation)

              match exec "DELETE FROM fsm.machine_chart_version WHERE version = 1" with
              | Ok() -> failtest "deleting a referenced chart version should have been refused"
              | Error error ->
                  Expect.stringContains
                      (string error)
                      "command_chart_version_fkey"
                      "the foreign key should be what refuses it"
          } ]

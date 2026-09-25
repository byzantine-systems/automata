module ByzantineSystems.Automata.Storage.Sqlite.Tests.ChartRegistryTests

open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open ByzantineSystems.Automata.Storage.Sqlite.Tests.TestContext
open Expecto

/// Distinct, well-formed fingerprints. What the registry does with one does not depend on which
/// chart produced it; Core's suite is where the hashing is tested.
let private fingerprintA = ChartFingerprint.create (String.replicate 64 "b")
let private fingerprintB = ChartFingerprint.create (String.replicate 64 "c")

let private identity version fingerprint =
    { MachineId = testMachine
      Version = ChartVersion.create version
      Fingerprint = fingerprint }

let private register (registry: IChartRegistry) version fingerprint : Task<ChartRegistration> =
    task {
        let! outcome = registry.Register(identity version fingerprint, noCancellation)
        return outcome |> expectOk $"register version {version}"
    }

let tests =
    testList
        "IChartRegistry"
        [ testTask "a version is claimed once and then only confirmed" {
              let db = fixture "registry"
              let registry = registryOf db

              let! first = register registry 2 fingerprintA
              let! second = register registry 2 fingerprintA

              Expect.equal first ChartRegistration.Registered "the first registration claims the version"
              Expect.equal second ChartRegistration.Matched "the second confirms it"

              Expect.equal
                  (scalar db.Db "SELECT count(*) FROM fsm_machine_chart_version WHERE version = 2;")
                  "1"
                  "confirming writes no second row"
          }

          testTask "a different chart under a claimed version is reported, not accepted" {
              let db = fixture "registry"
              let registry = registryOf db

              let! _ = register registry 2 fingerprintA
              let! outcome = register registry 2 fingerprintB

              Expect.equal outcome (ChartRegistration.Mismatched fingerprintA) "the mismatch carries what is stored"

              Expect.equal
                  (scalar db.Db "SELECT fingerprint FROM fsm_machine_chart_version WHERE version = 2;")
                  (ChartFingerprint.value fingerprintA)
                  "a registration never overwrites the stored fingerprint"
          }

          testTask "versions and machines are independent" {
              let db = fixture "registry"
              let registry = registryOf db

              let! two = register registry 2 fingerprintA
              let! three = register registry 3 fingerprintB

              let! elsewhere =
                  registry.Register(
                      { MachineId = machineId "other-machine"
                        Version = ChartVersion.create 2
                        Fingerprint = fingerprintB },
                      noCancellation
                  )

              Expect.equal two ChartRegistration.Registered "version 2"
              Expect.equal three ChartRegistration.Registered "a different version of the same machine"

              Expect.equal
                  (elsewhere |> expectOk "another machine")
                  ChartRegistration.Registered
                  "another machine's version 2 is a different fact"
          }

          testTask "a version nothing registered is absent" {
              let db = fixture "registry"
              let registry = registryOf db

              let! missing = registry.TryGet(testMachine, ChartVersion.create 9, noCancellation)
              Expect.equal (missing |> expectOk "unregistered") None "nothing is registered"

              let! _ = register registry 9 fingerprintA
              let! found = registry.TryGet(testMachine, ChartVersion.create 9, noCancellation)
              Expect.equal (found |> expectOk "registered") (Some fingerprintA) "the stored fingerprint"
          }

          // Every process starts at once after a deploy. With one writer per file, the racers
          // queue rather than interleave, and all but one are told their chart matched.
          testTask "concurrent registrations of one chart agree" {
              let db = fixture "registry"
              let registry = registryOf db

              let! outcomes = Task.WhenAll [ for _ in 1..16 -> register registry 4 fingerprintA ]

              Expect.equal
                  (outcomes |> Array.countBy id |> Map.ofArray)
                  (Map.ofList [ ChartRegistration.Registered, 1; ChartRegistration.Matched, 15 ])
                  "one registers and every other racer matches"
          }

          testTask "a referenced version cannot be deleted" {
              let db = fixture "registry"
              let! _ = submit (inboxOf db) (entityId "e1") "k1" (Start 1)

              Expect.equal
                  (extendedError (fun () -> exec db.Db "DELETE FROM fsm_machine_chart_version WHERE version = 1;" []))
                  Code.constraintForeignKey
                  "the foreign key refuses it"
          } ]

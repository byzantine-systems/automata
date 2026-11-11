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
        [ ContractProperties.registryProperty "registrations follow an immutable finite map" 40 (fun () ->
              fixture "registry-property" |> registryOf |> Task.FromResult)

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

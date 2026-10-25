module ByzantineSystems.Automata.Storage.ContractProperties

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open FsCheck

type private RegistryOperation =
    | Register of machine: byte * version: byte * fingerprint: byte
    | Read of machine: byte * version: byte

let private identity machineSeed versionSeed fingerprintSeed =
    let alphabet = "abcdef0123456789"
    let fingerprint = alphabet[int fingerprintSeed % alphabet.Length] |> string |> String.replicate 64

    { MachineId = machineId $"contract-machine-%d{int machineSeed % 4}"
      Version = ChartVersion.create (2 + int versionSeed % 4)
      Fingerprint = ChartFingerprint.create fingerprint }

let private runRegistryTrace (fresh: unit -> Task<IChartRegistry>) generated =
    task {
        let operations =
            match generated with
            | [] -> [ Read(0uy, 0uy) ]
            | values -> values

        let! registry = fresh ()

        let rec run model remaining =
            task {
                match remaining with
                | [] -> return true
                | operation :: rest ->
                    match operation with
                    | Register(machineSeed, versionSeed, fingerprintSeed) ->
                        let chart = identity machineSeed versionSeed fingerprintSeed
                        let key = machineSeed % 4uy, versionSeed % 4uy

                        let expected, next =
                            match Map.tryFind key model with
                            | None -> ChartRegistration.Registered, Map.add key chart.Fingerprint model
                            | Some stored when stored = chart.Fingerprint -> ChartRegistration.Matched, model
                            | Some stored -> ChartRegistration.Mismatched stored, model

                        let! actual = registry.Register(chart, CancellationToken.None)
                        let! found = registry.TryGet(chart.MachineId, chart.Version, CancellationToken.None)

                        match actual, found with
                        | Ok registration, Ok fingerprint when registration = expected && fingerprint = Map.tryFind key next ->
                            return! run next rest
                        | _ -> return false
                    | Read(machineSeed, versionSeed) ->
                        let chart = identity machineSeed versionSeed 0uy
                        let key = machineSeed % 4uy, versionSeed % 4uy
                        let! actual = registry.TryGet(chart.MachineId, chart.Version, CancellationToken.None)

                        match actual with
                        | Ok fingerprint when fingerprint = Map.tryFind key model -> return! run model rest
                        | _ -> return false
            }

        return! run Map.empty operations
    }

let registryProperty name maxTests fresh =
    testCase name (fun () ->
        let config =
            Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(30)

        Check.One(
            config,
            fun (operations: RegistryOperation list) ->
                runRegistryTrace fresh operations
                |> fun running -> running.GetAwaiter().GetResult()
        ))

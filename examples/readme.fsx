// The runnable version of the light-switch chart from README.md, adapted from:
// https://statecharts.dev/on-off-statechart.html
//
// Run from the repository root:
//   dotnet fsi examples/readme.fsx

// Load the local Core project in its .fsproj compilation order so this script always
// exercises the checked-out source rather than a separately published NuGet version.
#load "../src/ByzantineSystems.Automata.Core/Identifiers.fs"
#load "../src/ByzantineSystems.Automata.Core/Transition.fs"
#load "../src/ByzantineSystems.Automata.Core/Errors.fs"
#load "../src/ByzantineSystems.Automata.Core/Boundary.fs"
#load "../src/ByzantineSystems.Automata.Core/Codec.fs"
#load "../src/ByzantineSystems.Automata.Core/Rules.fs"
#load "../src/ByzantineSystems.Automata.Core/Resolution.fs"
#load "../src/ByzantineSystems.Automata.Core/Chart.fs"
#load "../src/ByzantineSystems.Automata.Core/ChartBuilder.fs"

open ByzantineSystems.Automata.Core

type SwitchState =
    | Off
    | On

type SwitchEvent = Flick

type SwitchAction =
    | TurnLightOn
    | TurnLightOff

let switchChart =
    statechart<SwitchState, SwitchEvent, SwitchAction, string> {
        root "switch"

        classify (function
            | Off -> stateId "off"
            | On -> stateId "on")

        state "off" { on (fun _ event -> event = Flick) (fun _ _ -> [], On) }

        state "on" {
            onEntry (fun _ _ -> [ TurnLightOn ])
            onExit (fun _ _ -> [ TurnLightOff ])
            on (fun _ event -> event = Flick) (fun _ _ -> [], Off)
        }
    }
    |> function
        | Ok chart -> chart
        | Error errors -> invalidOp $"Invalid switch chart: %A{errors}"

let flick state =
    match Chart.resolve switchChart state Flick with
    | Ok resolution ->
        printfn "%A --Flick--> %A; actions = %A" state resolution.Next resolution.Actions
        resolution.Next
    | Error error -> invalidOp $"Could not resolve Flick from %A{state}: %A{error}"

let afterFirstFlick = flick Off
let afterSecondFlick = flick afterFirstFlick

printfn "Final state: %A" afterSecondFlick

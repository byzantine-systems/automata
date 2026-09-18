module ByzantineSystems.Automata.Samples.Supervision.Program

/// <summary>
/// Crash + restart demo: kills a supervised child and shows the restart in
/// automata.supervision_event (phase 6).
/// </summary>
[<EntryPoint>]
let main _ =
    printfn "Supervision sample: populated in phase 6."
    0

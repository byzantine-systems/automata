module ByzantineSystems.Automata.Core.Tests.Property

open Expecto
open FsCheck

/// FsCheck 3's Check.One prints falsifications instead of throwing, so a property always reports
/// true to FsCheck while real counterexamples are captured and asserted here. FsCheck owns
/// generation; Expecto owns failure. Every property suite in this project runs through this one
/// harness, so a counterexample always fails the Expecto test that owns it.
let property (name: string) (prop: 'a -> bool) : Test =
    testCase name (fun () ->
        let failures = ResizeArray<'a>()

        Check.One(
            Config.Quick,
            fun (sample: 'a) ->
                if not (prop sample) then
                    failures.Add sample

                true
        )

        if failures.Count > 0 then
            failtestf "%s - %d counterexamples, first: %A" name failures.Count (Seq.head failures))

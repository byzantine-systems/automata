module ByzantineSystems.Automata.Storage.Tests.Property

open Expecto
open FsCheck

/// Runs FsCheck under the owning Expecto test, preserving shrinking and replay information.
let property (name: string) (law: 'a -> bool) : Test =
    testCase name (fun () -> Check.One(Config.QuickThrowOnFailure, law))

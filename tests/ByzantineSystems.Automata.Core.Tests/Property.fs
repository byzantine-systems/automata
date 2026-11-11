module ByzantineSystems.Automata.Core.Tests.Property

open Expecto
open FsCheck

/// Maps every Int32 value, including Int32.MinValue, into [0, count).
let bounded count value = int (uint32 value % uint32 count)

/// Runs FsCheck with its unit-test configuration so falsifications are shrunk and then thrown into
/// the owning Expecto test with replay information.
let property (name: string) (prop: 'a -> bool) : Test =
    testCase name (fun () -> Check.One(Config.QuickThrowOnFailure, prop))

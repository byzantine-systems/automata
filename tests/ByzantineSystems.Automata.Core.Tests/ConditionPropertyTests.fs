module ByzantineSystems.Automata.Core.Tests.ConditionPropertyTests

open ByzantineSystems.Automata.Core
open Expecto

type private Event = bool * bool

let private eventFirst = Condition.eventIs "event.first" fst
let private eventSecond = Condition.eventIs "event.second" snd
let private stateTrue = Condition.stateIs "state.true" id

let private bothSame =
    Condition.both "both.same" (fun state (first, _) -> state = first)

let private customEither =
    Condition.custom "custom.either" (fun state (first, second) -> state || first || second)

let rec private generatedCondition seeds : Condition<bool, Event> =
    match seeds with
    | [] -> Condition.always
    | head :: tail ->
        match Property.bounded 10 head with
        | 0 -> Condition.always
        | 1 -> Condition.never
        | 2 -> eventFirst
        | 3 -> eventSecond
        | 4 -> stateTrue
        | 5 -> bothSame
        | 6 -> customEither
        | 7 -> tail |> generatedCondition |> Condition.negate
        | mode ->
            let split = tail.Length / 2
            let left, right = List.splitAt split tail
            let terms = [ generatedCondition left; generatedCondition right ]

            if mode = 8 then
                Condition.allOf terms
            else
                Condition.anyOf terms

let rec private evaluateSyntax syntax state (first, second) =
    match syntax with
    | ConditionSyntax.Always -> true
    | ConditionSyntax.Never -> false
    | ConditionSyntax.AllOf terms -> terms |> List.forall (fun term -> evaluateSyntax term state (first, second))
    | ConditionSyntax.AnyOf terms -> terms |> List.exists (fun term -> evaluateSyntax term state (first, second))
    | ConditionSyntax.Not term -> not (evaluateSyntax term state (first, second))
    | ConditionSyntax.EventIs "event.first" -> first
    | ConditionSyntax.EventIs "event.second" -> second
    | ConditionSyntax.StateIs "state.true" -> state
    | ConditionSyntax.Both "both.same" -> state = first
    | ConditionSyntax.Custom "custom.either" -> state || first || second
    | unknown -> failtestf "the toy syntax interpreter does not know %A" unknown

let rec private rebuild syntax : Condition<bool, Event> =
    match syntax with
    | ConditionSyntax.Always -> Condition.always
    | ConditionSyntax.Never -> Condition.never
    | ConditionSyntax.AllOf terms -> terms |> List.map rebuild |> Condition.allOf
    | ConditionSyntax.AnyOf terms -> terms |> List.map rebuild |> Condition.anyOf
    | ConditionSyntax.Not term -> term |> rebuild |> Condition.negate
    | ConditionSyntax.EventIs "event.first" -> eventFirst
    | ConditionSyntax.EventIs "event.second" -> eventSecond
    | ConditionSyntax.StateIs "state.true" -> stateTrue
    | ConditionSyntax.Both "both.same" -> bothSame
    | ConditionSyntax.Custom "custom.either" -> customEither
    | unknown -> failtestf "the toy syntax rebuilder does not know %A" unknown

let private finiteDomain =
    [ for state in [ false; true ] do
          for first in [ false; true ] do
              for second in [ false; true ] do
                  state, (first, second) ]

let private truthTable condition =
    finiteDomain
    |> List.map (fun (state, event) -> Condition.matches condition state event)

let private explorationTest =
    test "finite-signature exploration finds no normalized-syntax collision with different observations" {
        let atoms = [ eventFirst; eventSecond; stateTrue; bothSame ]

        let terms =
            [ yield Condition.always
              yield Condition.never
              yield! atoms
              yield! atoms |> List.map Condition.negate

              for left in atoms do
                  for right in atoms do
                      yield Condition.allOf [ left; right ]
                      yield Condition.anyOf [ left; right ] ]

        let collisions =
            terms
            |> List.groupBy Condition.syntax
            |> List.choose (fun (syntax, equivalents) ->
                match equivalents |> List.map truthTable |> List.distinct with
                | []
                | [ _ ] -> None
                | observations -> Some(syntax, observations))

        Expect.isEmpty collisions "one normalized syntax must have one observation on the finite signature"
    }

let tests =
    testList
        "condition laws"
        [ Property.property
              "matches interprets the normalized syntax on the finite toy domain"
              (fun (seeds: int list, state: bool, first: bool, second: bool) ->
                  let condition = generatedCondition seeds
                  let event = first, second
                  Condition.matches condition state event = evaluateSyntax (Condition.syntax condition) state event)

          Property.property
              "allOf and anyOf are boolean homomorphisms"
              (fun (leftSeeds: int list, rightSeeds: int list, state: bool, first: bool, second: bool) ->
                  let left = generatedCondition leftSeeds
                  let right = generatedCondition rightSeeds
                  let event = first, second
                  let leftResult = Condition.matches left state event
                  let rightResult = Condition.matches right state event

                  Condition.matches (Condition.allOf [ left; right ]) state event = (leftResult && rightResult)
                  && Condition.matches (Condition.anyOf [ left; right ]) state event = (leftResult || rightResult))

          Property.property
              "double negation and De Morgan preserve observations"
              (fun (leftSeeds: int list, rightSeeds: int list, state: bool, first: bool, second: bool) ->
                  let left = generatedCondition leftSeeds
                  let right = generatedCondition rightSeeds
                  let event = first, second
                  let matches condition = Condition.matches condition state event

                  matches (left |> Condition.negate |> Condition.negate) = matches left
                  && matches (Condition.allOf [ left; right ] |> Condition.negate) = matches (
                      Condition.anyOf [ Condition.negate left; Condition.negate right ]
                  )
                  && matches (Condition.anyOf [ left; right ] |> Condition.negate) = matches (
                      Condition.allOf [ Condition.negate left; Condition.negate right ]
                  ))

          Property.property "normalization is idempotent" (fun (seeds: int list) ->
              let condition = generatedCondition seeds
              let syntax = Condition.syntax condition
              Condition.syntax (rebuild syntax) = syntax)

          test "smart constructors canonicalize monoid spellings" {
              let conjunction =
                  Condition.allOf [ eventFirst; Condition.allOf [ stateTrue; eventSecond ] ]

              let reordered = Condition.allOf [ eventSecond; eventFirst; stateTrue ]

              let disjunction =
                  Condition.anyOf [ eventFirst; Condition.anyOf [ stateTrue; eventSecond ] ]

              let disjunctionReordered = Condition.anyOf [ eventSecond; eventFirst; stateTrue ]

              Expect.equal (Condition.syntax conjunction) (Condition.syntax reordered) "allOf is flattened and sorted"

              Expect.equal
                  (Condition.syntax disjunction)
                  (Condition.syntax disjunctionReordered)
                  "anyOf is flattened and sorted"

              Expect.equal (Condition.syntax (Condition.allOf [])) ConditionSyntax.Always "empty conjunction"
              Expect.equal (Condition.syntax (Condition.anyOf [])) ConditionSyntax.Never "empty disjunction"

              Expect.equal
                  (Condition.syntax (Condition.allOf [ Condition.always; eventFirst ]))
                  (Condition.syntax eventFirst)
                  "Always is the conjunction identity"

              Expect.equal
                  (Condition.syntax (Condition.anyOf [ Condition.never; eventFirst ]))
                  (Condition.syntax eventFirst)
                  "Never is the disjunction identity"
          }

          test "named predicates require a stable nonblank name" {
              Expect.throwsT<System.ArgumentException>
                  (fun () -> Condition.custom " " (fun (_: bool) (_: Event) -> true) |> ignore)
                  "blank names cannot identify opaque behavior"
          }

          explorationTest ]

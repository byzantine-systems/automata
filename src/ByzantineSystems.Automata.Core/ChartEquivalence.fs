namespace ByzantineSystems.Automata.Core

/// <summary>Observable chart traces and explicit equivalence checks.</summary>
[<RequireQualifiedAccess>]
module ChartEquivalence =

    /// <summary>One successful resolution observed without exposing state representation.</summary>
    type Step<'Action> =
        { Leaf: StateId
          Terminal: bool
          Actions: 'Action list }

    /// <summary>The successful prefix and first failure of a trace.</summary>
    type TraceFailure<'Action, 'Err> =
        { Steps: Step<'Action> list
          Error: TransitionError<'Err> }

    /// <summary>Runs events in order and stops at the first resolution failure.</summary>
    let trace
        (chart: Chart<'State, 'Event, 'Action, 'Err>)
        (initialState: 'State)
        (events: 'Event list)
        : Result<Step<'Action> list, TraceFailure<'Action, 'Err>> =
        let rec loop steps state remaining =
            match remaining with
            | [] -> Ok(List.rev steps)
            | event :: rest ->
                match Chart.resolve chart state event with
                | Ok resolution ->
                    let next = resolution.Next

                    let step =
                        { Leaf = Chart.classifyOf chart next
                          Terminal = Chart.isTerminal chart next
                          Actions = resolution.Actions }

                    loop (step :: steps) next rest
                | Result.Error error ->
                    Result.Error
                        { Steps = List.rev steps
                          Error = error }

        loop [] initialState events

    let rec private listEqual equal left right =
        match left, right with
        | [], [] -> true
        | leftHead :: leftTail, rightHead :: rightTail -> equal leftHead rightHead && listEqual equal leftTail rightTail
        | _ -> false

    let private stepsEqual actionEqual left right =
        let stepEqual leftStep rightStep =
            leftStep.Leaf = rightStep.Leaf
            && leftStep.Terminal = rightStep.Terminal
            && listEqual actionEqual leftStep.Actions rightStep.Actions

        listEqual stepEqual left right

    /// <summary>Compares traces over caller-supplied samples; finite samples never prove equivalence.</summary>
    let equivalentOn
        (actionEqual: 'Action -> 'Action -> bool)
        (errorEqual: TransitionError<'Err> -> TransitionError<'Err> -> bool)
        (eventSequences: 'Event list list)
        ((leftChart, leftState): Chart<'State, 'Event, 'Action, 'Err> * 'State)
        ((rightChart, rightState): Chart<'State, 'Event, 'Action, 'Err> * 'State)
        : bool =
        let traceEqual events =
            match trace leftChart leftState events, trace rightChart rightState events with
            | Ok left, Ok right -> stepsEqual actionEqual left right
            | Result.Error left, Result.Error right ->
                stepsEqual actionEqual left.Steps right.Steps
                && errorEqual left.Error right.Error
            | _ -> false

        eventSequences |> List.forall traceEqual

    /// <summary>Tests exact canonical declaration equality, not callback behavior.</summary>
    let declaredEquivalent
        (left: Chart<'State, 'Event, 'Action, 'Err>)
        (right: Chart<'State, 'Event, 'Action, 'Err>)
        : bool =
        Chart.declaredSyntax left = Chart.declaredSyntax right

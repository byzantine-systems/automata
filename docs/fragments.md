# Chart fragments

A chart is built from `state` and `compound` blocks, and each block is an ordinary value, a `NodeDraft`. Nothing ties it to the chart it is first written for, so a piece of behaviour can be written once and used wherever it is needed.

## A fragment is a value

```fsharp
let approval =
    compound "approval" {
        initial "pending"
        state "pending" { on (fun _ e -> e = Approve) (fun _ _ -> [], Approved) }
        state "rejected" { terminal }
    }

let chart =
    statechart<Stage, Review, Notice, string> {
        root "document"
        classify classifyStage
        state "drafting" { goto (fun _ e -> e = Submit) (stateId "approval") (fun _ _ -> [], Pending) }
        approval
    }
```

A value that differs between uses is a function of what differs:

```fsharp
let approval (reviewer: string) (rejected: Stage) (approved: Stage) =
    compound "approval" { ... }
```

## Several at once

A fragment library can export a list. `yield!` splices every node of it, at the top of a chart or inside a compound:

```fsharp
statechart<_, _, _, _> {
    root "document"
    classify classifyStage
    yield! reviewSteps
}
```

## The same fragment twice

Node ids must be unique within a chart, so two copies of one fragment collide. For a fragment that is a function, the simplest answer is to take its name as an argument as well. When the fragment is someone else's, or its internal names matter to it, `Fragment.prefix` renames it:

```fsharp
yield!
    [ Fragment.prefix "legal" (approval "legal" LegalRejected FinancePending)
      Fragment.prefix "finance" (approval "finance" FinanceRejected Published) ]
```

`Fragment.prefix "legal"` renames every node the fragment declares from `id` to `legal.id`, and rewrites what refers to them:

- each node's initial child;
- every `goto` whose target the fragment declares, both in what the chart reads and in what the rule returns when it fires.

A `goto` to a state the fragment does **not** declare is left alone. That is how a fragment sends an entity back to its host: `goto ... (stateId "drafting")` inside `approval` still reaches the host's `drafting` after the prefix.

## The one rule the prefix cannot keep for you

The classifier is the host's own code, so a prefix cannot rewrite it. It must return the prefixed ids:

```fsharp
classify (function
    | LegalPending -> stateId "legal.pending"
    | FinancePending -> stateId "finance.pending"
    ...)
```

A classifier that forgets is caught rather than trusted:

- the machine refuses to build when its initial state classifies to an undeclared node (`InitialStateUnknown`);
- resolution answers `UnknownState` with the stale id for any other state.

And a `goto` to a node the chart does not declare is refused when the chart is built, as `ChartError.UnknownGotoTarget`, whether it came from a prefix or from a typo.

Renaming nodes changes the chart's structure, so it changes `Chart.fingerprint`. Prefixing a fragment in a chart that has already run is a new chart version.

namespace ByzantineSystems.Automata.Runtime

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// Turns what the chart decided into what the store can commit.
///
/// Pure and total: given a resolution it always produces a draft, and it reads no clock, no
/// configuration and no database. Everything the database owns is absent by construction, which
/// is the point of the draft being a separate type from the committed transition.
/// </summary>
[<RequireQualifiedAccess>]
module Draft =

    /// <summary>
    /// Assembles the draft for one resolved command.
    ///
    /// <paramref name="effectiveAt" /> is business time and is the caller's, never the clock's.
    /// The processor passes the command's recorded arrival instant, so a replay of the log
    /// reproduces the same drafts it produced the first time.
    /// </summary>
    let ofResolution
        (machineId: MachineId)
        (entityId: 'EntityId)
        (chart: Chart<'State, 'Event, 'Action, 'Err>)
        (fromState: 'State)
        (event: 'Event)
        (effectiveAt: DateTimeOffset)
        (resolution: Resolution<'State, 'Action>)
        : TransitionDraft<'EntityId, 'State, 'Event, 'Action> =
        { MachineId = machineId
          EntityId = entityId
          Event = event
          Actions = resolution.Actions
          FromState = fromState
          ToState = resolution.Next
          HandledBy = resolution.HandledBy
          Exited = resolution.Exited
          Entered = resolution.Entered
          // A terminal target ends the instance. Asking the chart rather than tracking it means
          // the lifecycle cannot drift from the structure that defines it.
          Status =
            if Chart.isTerminal chart resolution.Next then
                InstanceStatus.Terminated
            else
                InstanceStatus.Running
          EffectiveAt = effectiveAt }

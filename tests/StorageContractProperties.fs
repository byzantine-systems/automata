module ByzantineSystems.Automata.Storage.ContractProperties

open System
open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage
open Expecto
open FsCheck

// ---------------------------------------------------------------------------------------------
// The chart registry.
// ---------------------------------------------------------------------------------------------

type private RegistryOperation =
    | Register of machine: byte * version: byte * fingerprint: byte
    | Read of machine: byte * version: byte

let private identity machineSeed versionSeed fingerprintSeed =
    let alphabet = "abcdef0123456789"

    let fingerprint =
        alphabet[int fingerprintSeed % alphabet.Length] |> string |> String.replicate 64

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
                        | Ok registration, Ok fingerprint when
                            registration = expected && fingerprint = Map.tryFind key next
                            ->
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
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(30)

        Check.One(
            config,
            fun (operations: RegistryOperation list) ->
                runRegistryTrace fresh operations
                |> fun running -> running.GetAwaiter().GetResult()
        ))

// ---------------------------------------------------------------------------------------------
// The inbox state machine.
//
// A generated trace drives the durable inbox and the fenced writes that finish a claim, and a
// model that knows only the documented contract predicts every answer: which heads a claim may
// return, what each fenced write reports, and what every read-back holds. Both providers run
// the same traces; each supplies a fresh fixture per case and its own control of time.
// ---------------------------------------------------------------------------------------------

/// The domain every generated inbox trace speaks.
type ContractEntity = class end

type ContractEvent =
    | Ping of int
    | Pong

type ContractState = Idle
type ContractAction = Fired
type ContractError = Refused of string

type ContractEntityId = EntityId<ContractEntity>

type ContractInbox = ICommandInbox<ContractEntityId, ContractEvent>

type ContractProcessor =
    ICommandProcessorStore<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError>

/// The stores one provider supplies per generated case, plus that provider's control of time.
type ContractStores =
    {
        Inbox: ContractInbox
        Processor: ContractProcessor
        /// The effective now for claiming: a frozen clock the trace moves, or the real one.
        Now: unit -> DateTimeOffset
        /// Moves the effective now forward by the span.
        Advance: TimeSpan -> Task
    }

/// Everything a provider fills in to run the shared inbox property.
type ContractInboxEnv =
    {
        Machine: MachineId
        Version: ChartVersion
        /// The lease every claim in a trace takes.
        ClaimLease: LeaseDuration
        /// The envelope every reschedule in a trace uses.
        Backoff: Backoff
        /// Lease deadlines are stamped by the database, so claimability near them is not
        /// predictable; inside this deadband the model declines to assert.
        Margin: TimeSpan
        /// Abstract ticks to provider time.
        Ticks: int -> TimeSpan
        /// A fixture with nothing in it, once per generated case, shrinking reruns included.
        Fresh: unit -> Task<ContractStores>
    }

/// One step of a generated trace, as raw choices.
type InboxChoice =
    | Submit of entity: byte * key: byte * event: byte * delay: byte
    | Claim of batch: byte
    | Finish of kind: byte * claim: byte
    | Stale of kind: byte * claim: byte
    | Advance of ticks: byte
    | Lookup of entity: byte * key: byte

let private entityName (raw: byte) = $"e{int raw % 3}"

let private keyName (raw: byte) = $"k{int raw % 6}"

let private eventOf (raw: byte) : ContractEvent =
    if int raw % 4 = 3 then Pong else Ping(int raw % 4)

let private delayTable = [| 0; 0; 40; 90; 150 |]
let private delayOf (raw: byte) = delayTable[int raw % 5]

let private batchTable = [| 1; 2; 4; 50 |]
let private batchOf (raw: byte) = batchTable[int raw % 4]

let private advanceTable = [| 5; 10; 20; 40; 60; 90; 150; 300 |]
let private advanceOf (raw: byte) = advanceTable[int raw % 8]

/// What a fenced write does: 0 rejects, 1 dead-letters, 2 reschedules, 3 extends the lease.
let private finishKindOf (raw: byte) = int raw % 4

let private staleKindName =
    function
    | 0 -> "stale-reject"
    | 1 -> "stale-deadletter"
    | 2 -> "stale-retry"
    | _ -> "stale-renew"

// The model: one entry per submitted command, updated as the trace runs.

type private ModelStatus =
    | Waiting of visibleAt: DateTimeOffset * uncertainUntil: DateTimeOffset option
    | Leased of deadline: DateTimeOffset
    | Finished of CommandStatus

type private Tracked =
    { Index: int
      Entity: string
      EntityId: ContractEntityId
      Key: string
      Event: ContractEvent
      Sequence: int64
      HadDelay: bool
      mutable Status: ModelStatus
      mutable Attempts: int
      mutable Deliveries: int
      mutable LastToken: int64
      mutable RowToken: int64
      mutable LiveClaim: int option
      mutable Id: CommandId
      ReceivedBefore: DateTimeOffset
      ReceivedAfter: DateTimeOffset }

type private HeadClass =
    | HeadClaimable
    | HeadHeld
    | HeadHidden
    | HeadAmbiguous

/// Arrival instants are stamped inside the database, so read-backs compare against a tolerance
/// that covers the difference between the two clocks.
let private receivedTolerance = TimeSpan.FromMilliseconds 100.0

/// Runs one trace against the stores, checking every answer against the model. Answers with
/// the verdict and the behavior classes the executed steps covered.
let private runInboxTrace
    (env: ContractInboxEnv)
    (stores: ContractStores)
    (choices: InboxChoice list)
    : Task<bool * string list> =
    task {
        let classes = ResizeArray<string>()
        let commands = ResizeArray<Tracked>()
        // Every claim the trace observed, in observation order, as (command index, token).
        let claims = ResizeArray<int * int64>()
        let ct = CancellationToken.None

        let finished (tracked: Tracked) =
            match tracked.Status with
            | Finished _ -> true
            | _ -> false

        let commandStatusOf (tracked: Tracked) =
            match tracked.Status with
            | Waiting _ -> CommandStatus.Ready
            | Leased _ -> CommandStatus.Leased
            | Finished status -> status

        let blocked (tracked: Tracked) =
            commands
            |> Seq.take tracked.Index
            |> Seq.exists (fun earlier -> not (finished earlier) && earlier.Entity = tracked.Entity)

        let classify (now: DateTimeOffset) (tracked: Tracked) =
            match tracked.Status with
            | Finished _ -> HeadHidden // unreachable: a head is never finished
            | Waiting(visibleAt, uncertainUntil) ->
                // A caller-supplied instant is exact; a jittered retry is only bounded.
                match uncertainUntil with
                | None -> if now >= visibleAt then HeadClaimable else HeadHidden
                | Some readyAt -> if now >= readyAt then HeadClaimable else HeadAmbiguous
            | Leased deadline ->
                if now >= deadline + env.Margin then HeadClaimable
                elif now >= deadline - env.Margin then HeadAmbiguous
                else HeadHeld

        /// The earliest non-terminal command of each entity, in submission order.
        let heads () =
            let seen = ResizeArray<string>()

            [ for tracked in commands do
                  if not (finished tracked) && not (seen.Contains tracked.Entity) then
                      seen.Add tracked.Entity
                      tracked ]

        let trackedOf (leased: LeasedCommand<ContractEntityId, ContractEvent>) =
            commands
            |> Seq.tryFind (fun tracked ->
                tracked.EntityId = leased.Work.EntityId
                && tracked.Key = leased.Work.IdempotencyKey)

        let rec loop (remaining: InboxChoice list) : Task<bool> =
            task {
                match remaining with
                | [] -> return true
                | choice :: rest ->
                    match choice with
                    | Submit(entity, key, event, delay) ->
                        let name = entityName entity
                        let idempotencyKey = keyName key
                        let event = eventOf event
                        let delayMs = delayOf delay

                        let submission: CommandSubmission<ContractEntityId, ContractEvent> =
                            { MachineId = env.Machine
                              EntityId = entityId name
                              IdempotencyKey = idempotencyKey
                              ChartVersion = env.Version
                              Kind = CommandKind.Event
                              Event = event
                              VisibleAt =
                                if delayMs = 0 then
                                    None
                                else
                                    Some(stores.Now() + env.Ticks delayMs)
                              ReceivedAt = None
                              Audit = AuditContext.empty }

                        match
                            commands
                            |> Seq.tryFind (fun tracked -> tracked.Entity = name && tracked.Key = idempotencyKey)
                        with
                        | Some tracked ->
                            let! outcome = stores.Inbox.Submit(submission, ct)

                            match outcome with
                            | Ok(AlreadySubmitted found) when found = tracked.Id ->
                                classes.Add "resubmit"
                                return! loop rest
                            | _ -> return false
                        | None ->
                            let before = stores.Now()
                            let! outcome = stores.Inbox.Submit(submission, ct)

                            match outcome with
                            | Ok(Accepted id) ->
                                let sequence =
                                    int64 (commands |> Seq.filter (fun tracked -> tracked.Entity = name) |> Seq.length)
                                    + 1L

                                commands.Add(
                                    { Index = commands.Count
                                      Entity = name
                                      EntityId = entityId name
                                      Key = idempotencyKey
                                      Event = event
                                      Sequence = sequence
                                      HadDelay = delayMs > 0
                                      Status = Waiting(defaultArg submission.VisibleAt before, None)
                                      Attempts = 0
                                      Deliveries = 0
                                      LastToken = 0L
                                      RowToken = 0L
                                      LiveClaim = None
                                      Id = id
                                      ReceivedBefore = before
                                      ReceivedAfter = stores.Now() }
                                )

                                return! loop rest
                            | _ -> return false

                    | Claim(batch) ->
                        let now = stores.Now()
                        let classified = heads () |> List.map (fun tracked -> tracked, classify now tracked)

                        // Near a database-stamped deadline the answer is not predictable, so
                        // this claim is skipped rather than asserted either way.
                        if classified |> List.exists (fun (_, kind) -> kind = HeadAmbiguous) then
                            return! loop rest
                        else
                            let expected = classified |> List.filter (snd >> (=) HeadClaimable) |> List.map fst
                            let batchLimit = batchOf batch

                            let! claimed =
                                stores.Inbox.Claim(env.Machine, ClaimBatch.create batchLimit, env.ClaimLease, ct)

                            match claimed with
                            | Error _ -> return false
                            | Ok observed ->
                                let entities = observed |> List.map (fun leased -> leased.Work.EntityId)

                                let shapeOk =
                                    observed
                                    |> List.forall (fun leased ->
                                        match trackedOf leased with
                                        | Some tracked ->
                                            tracked.Id = leased.Work.CommandId
                                            && tracked.Event = leased.Work.Event
                                            && tracked.Sequence = leased.Work.Sequence
                                            && leased.Work.ChartVersion = env.Version
                                            && leased.Work.MachineId = env.Machine
                                            && leased.Work.Kind = CommandKind.Event
                                        | None -> false)

                                let countsOk =
                                    observed
                                    |> List.forall (fun leased ->
                                        match trackedOf leased with
                                        | Some tracked -> leased.DeliveryCount = tracked.Deliveries + 1
                                        | None -> false)

                                let tokensOk =
                                    observed
                                    |> List.forall (fun leased ->
                                        match trackedOf leased with
                                        | Some tracked -> LeaseToken.value leased.Token > tracked.LastToken
                                        | None -> false)

                                let structureOk =
                                    (List.distinct entities).Length = entities.Length
                                    && observed.Length = min batchLimit expected.Length
                                    && shapeOk
                                    && countsOk
                                    && tokensOk
                                    && List.forall (fun leased -> leased.ExpiresAt > now) observed

                                if not structureOk then
                                    return false
                                else
                                    if observed.IsEmpty then
                                        classes.Add "empty-claim"

                                    if expected.Length > batchLimit then
                                        classes.Add "batch-limited"

                                    if
                                        commands
                                        |> Seq.filter (fun tracked -> not (finished tracked))
                                        |> Seq.countBy (fun tracked -> tracked.Entity)
                                        |> Seq.exists (fun (_, count) -> count > 1)
                                    then
                                        classes.Add "blocked-sibling"

                                    if
                                        classified
                                        |> List.exists (fun (tracked, kind) -> tracked.HadDelay && kind = HeadHidden)
                                    then
                                        classes.Add "delay-not-yet"

                                    let pairs = observed |> List.map (fun leased -> leased, trackedOf leased)

                                    if pairs |> List.exists (snd >> Option.isNone) then
                                        return false
                                    else
                                        for leased, found in pairs do
                                            let tracked = found.Value

                                            if tracked.Deliveries + 1 >= 2 then
                                                classes.Add "expiry-reclaimed"

                                            if tracked.HadDelay then
                                                classes.Add "delay-elapsed"

                                            claims.Add(tracked.Index, LeaseToken.value leased.Token)

                                            tracked.Status <- Leased leased.ExpiresAt
                                            tracked.Deliveries <- tracked.Deliveries + 1
                                            tracked.LastToken <- LeaseToken.value leased.Token
                                            tracked.RowToken <- LeaseToken.value leased.Token
                                            tracked.LiveClaim <- Some(claims.Count - 1)

                                        return! loop rest

                    | Finish(kind, claim) ->
                        if claims.Count = 0 then
                            return! loop rest
                        else
                            let claimIndex = int claim % claims.Count
                            let commandIndex, token = claims[claimIndex]
                            let tracked = commands[commandIndex]

                            let live =
                                match tracked.Status, tracked.LiveClaim with
                                | Leased deadline, Some held when
                                    held = claimIndex && stores.Now() < deadline - env.Margin
                                    ->
                                    true
                                | _ -> false

                            if not live then
                                return! loop rest
                            else
                                let tokenId = LeaseToken.ofInt64 token
                                let failure = CommandFailure.Domain(Refused "the trace refused it")

                                match finishKindOf kind with
                                | 0 ->
                                    let! outcome = stores.Processor.Reject(tracked.Id, tokenId, failure, ct)

                                    match outcome with
                                    | Ok(Finalized committed) when committed = Epoch.initial ->
                                        tracked.Status <- Finished CommandStatus.Rejected
                                        tracked.LiveClaim <- None
                                        classes.Add "reject-releases"
                                        return! loop rest
                                    | _ -> return false
                                | 1 ->
                                    let! outcome = stores.Processor.DeadLetter(tracked.Id, tokenId, failure, ct)

                                    match outcome with
                                    | Ok(Finalized committed) when committed = Epoch.initial ->
                                        tracked.Status <- Finished CommandStatus.DeadLettered
                                        tracked.LiveClaim <- None
                                        classes.Add "deadletter-releases"
                                        return! loop rest
                                    | _ -> return false
                                | 2 ->
                                    let before = stores.Now()
                                    let! outcome = stores.Inbox.Reschedule(tracked.Id, tokenId, env.Backoff, ct)

                                    match outcome with
                                    | Ok Updated ->
                                        let after = stores.Now()
                                        let! record = stores.Inbox.TryGet(tracked.Id, ct)

                                        let readBackOk =
                                            match record with
                                            | Ok(Some read) ->
                                                read.Status = CommandStatus.Ready
                                                && read.Attempts = tracked.Attempts + 1
                                                && not read.Blocked
                                                && read.VisibleAt >= before
                                                && read.VisibleAt <= after + Backoff.ceiling env.Backoff
                                            | _ -> false

                                        if readBackOk then
                                            tracked.Attempts <- tracked.Attempts + 1
                                            tracked.Status <- Waiting(before, Some(after + Backoff.ceiling env.Backoff))
                                            tracked.LiveClaim <- None
                                            tracked.RowToken <- 0L
                                            classes.Add "retry-deferred"
                                            return! loop rest
                                        else
                                            return false
                                    | _ -> return false
                                | _ ->
                                    let! outcome = stores.Inbox.ExtendLease(tracked.Id, tokenId, env.ClaimLease, ct)

                                    match outcome with
                                    | Ok Updated ->
                                        tracked.Status <- Leased(stores.Now() + LeaseDuration.value env.ClaimLease)
                                        classes.Add "renew-held"
                                        return! loop rest
                                    | _ -> return false

                    | Stale(kind, claim) ->
                        if claims.Count = 0 then
                            return! loop rest
                        else
                            let claimIndex = int claim % claims.Count
                            let commandIndex, token = claims[claimIndex]
                            let tracked = commands[commandIndex]

                            if finished tracked then
                                return! loop rest
                            else if
                                // Reschedule resets the row's lease token to the nobody-holds
                                // sentinel, so a "stale" token of zero collides with it when the
                                // claim's token was the first ever issued. To the store that token
                                // is not stale at all, so this step is skipped rather than
                                // asserted either way.
                                token - 1L = tracked.RowToken
                            then
                                return! loop rest
                            else
                                let stale = LeaseToken.ofInt64 (token - 1L)
                                let failure = CommandFailure.Domain(Refused "a stale worker")

                                let kindIndex = finishKindOf kind

                                let! fenced =
                                    task {
                                        match kindIndex with
                                        | 0 ->
                                            let! outcome = stores.Processor.Reject(tracked.Id, stale, failure, ct)
                                            return outcome = Ok FinalizeOutcome.LeaseLost
                                        | 1 ->
                                            let! outcome = stores.Processor.DeadLetter(tracked.Id, stale, failure, ct)
                                            return outcome = Ok FinalizeOutcome.LeaseLost
                                        | 2 ->
                                            let! outcome = stores.Inbox.Reschedule(tracked.Id, stale, env.Backoff, ct)
                                            return outcome = Ok LeaseUpdateOutcome.LeaseLost
                                        | _ ->
                                            let! outcome =
                                                stores.Inbox.ExtendLease(tracked.Id, stale, env.ClaimLease, ct)

                                            return outcome = Ok LeaseUpdateOutcome.LeaseLost
                                    }

                                if not fenced then
                                    return false
                                else
                                    let! record = stores.Inbox.TryGet(tracked.Id, ct)

                                    let untouched =
                                        match record with
                                        | Ok(Some read) ->
                                            read.Attempts = tracked.Attempts && read.Status = commandStatusOf tracked
                                        | _ -> false

                                    if untouched then
                                        classes.Add(staleKindName kindIndex)
                                        return! loop rest
                                    else
                                        return false

                    | Advance(ticks) ->
                        do! stores.Advance(env.Ticks(advanceOf ticks))
                        return! loop rest

                    | Lookup(entity, key) ->
                        let name = entityName entity
                        let idempotencyKey = keyName key

                        let! found = stores.Inbox.TryFind(env.Machine, entityId name, idempotencyKey, ct)

                        match
                            commands
                            |> Seq.tryFind (fun tracked -> tracked.Entity = name && tracked.Key = idempotencyKey)
                        with
                        | Some tracked ->
                            match found with
                            | Ok(Some read) ->
                                let hitOk =
                                    read.CommandId = tracked.Id
                                    && read.Event = tracked.Event
                                    && read.ChartVersion = env.Version
                                    && read.Sequence = tracked.Sequence
                                    && read.Status = commandStatusOf tracked
                                    && read.Attempts = tracked.Attempts
                                    && read.Blocked = blocked tracked
                                    && read.ReceivedAt >= tracked.ReceivedBefore - receivedTolerance
                                    && read.ReceivedAt <= tracked.ReceivedAfter + receivedTolerance

                                if hitOk then
                                    classes.Add "lookup-hit"
                                    return! loop rest
                                else
                                    return false
                            | _ -> return false
                        | None ->
                            match found with
                            | Ok None ->
                                classes.Add "lookup-miss"
                                return! loop rest
                            | _ -> return false
            }

        let! verdict = loop (List.truncate 14 choices)
        return verdict, List.ofSeq classes
    }

// ---------------------------------------------------------------------------------------------
// An in-memory reference store, so a thousand simulated traces can prove the generated shapes
// reach every behavior class before any database is asked to run them.
// ---------------------------------------------------------------------------------------------

type private FakeStatus =
    | Ready
    | Leased
    | Rejected
    | Dead

type private FakeRow =
    { Id: CommandId
      Entity: string
      EntityId: ContractEntityId
      Key: string
      Event: ContractEvent
      Sequence: int64
      mutable Status: FakeStatus
      mutable VisibleAt: DateTimeOffset
      mutable Deadline: DateTimeOffset
      mutable Token: int64
      mutable Deliveries: int
      mutable Attempts: int
      ReceivedAt: DateTimeOffset }

/// The contract as the reference sees it: exactly the behavior the runner asserts, nothing more.
type FakeInbox(env: ContractInboxEnv) =
    let mutable now = DateTimeOffset(2026, 10, 1, 8, 0, 0, TimeSpan.Zero)
    let mutable nextId = 0L
    let mutable nextToken = 0L
    let rows = ResizeArray<FakeRow>()
    // Grouping wants a comparison-friendly name per entity, assigned in first-seen order.
    let entityNames = ResizeArray<ContractEntityId>()

    let nameOf (entity: ContractEntityId) =
        let index = entityNames.IndexOf entity

        if index >= 0 then
            string index
        else
            entityNames.Add entity
            string (entityNames.Count - 1)

    let nonTerminal (row: FakeRow) =
        row.Status = Ready || row.Status = Leased

    let blocked (row: FakeRow) =
        rows
        |> Seq.exists (fun other -> other.Entity = row.Entity && nonTerminal other && other.Sequence < row.Sequence)

    let recordOf (row: FakeRow) : CommandRecord<ContractEntityId, ContractEvent> =
        { CommandId = row.Id
          MachineId = env.Machine
          EntityId = row.EntityId
          Sequence = row.Sequence
          IdempotencyKey = row.Key
          ChartVersion = env.Version
          Kind = CommandKind.Event
          Event = row.Event
          Status =
            (match row.Status with
             | Ready -> CommandStatus.Ready
             | Leased -> CommandStatus.Leased
             | Rejected -> CommandStatus.Rejected
             | Dead -> CommandStatus.DeadLettered)
          Blocked = blocked row
          VisibleAt =
            (match row.Status with
             | Leased -> row.Deadline
             | _ -> row.VisibleAt)
          Attempts = row.Attempts
          ReceivedAt = row.ReceivedAt
          Audit = AuditContext.empty }

    let live (row: FakeRow) (token: LeaseToken<CommandWork>) =
        row.Status = Leased && row.Token = LeaseToken.value token && row.Deadline > now

    /// The stores the shared runner drives.
    member this.Stores: ContractStores =
        { Inbox = this :> ContractInbox
          Processor = this :> ContractProcessor
          Now = fun () -> now
          Advance =
            fun span ->
                now <- now + span
                Task.CompletedTask }

    interface ContractInbox with

        member _.Submit(submission: CommandSubmission<ContractEntityId, ContractEvent>, _ct: CancellationToken) =
            match
                rows
                |> Seq.tryFind (fun row -> row.EntityId = submission.EntityId && row.Key = submission.IdempotencyKey)
            with
            | Some row -> Task.FromResult(Ok(AlreadySubmitted row.Id))
            | None ->
                nextId <- nextId + 1L

                let row =
                    { Id = CommandId.ofInt64 nextId
                      Entity = nameOf submission.EntityId
                      EntityId = submission.EntityId
                      Key = submission.IdempotencyKey
                      Event = submission.Event
                      Sequence =
                        int64 (
                            rows
                            |> Seq.filter (fun other -> other.EntityId = submission.EntityId)
                            |> Seq.length
                        )
                        + 1L
                      Status = Ready
                      VisibleAt = defaultArg submission.VisibleAt now
                      Deadline = now
                      Token = 0L
                      Deliveries = 0
                      Attempts = 0
                      ReceivedAt = now }

                rows.Add row
                Task.FromResult(Ok(Accepted row.Id))

        member _.Claim(_machine: MachineId, batch: ClaimBatch, lease: LeaseDuration, _ct: CancellationToken) =
            let claimed =
                rows
                |> Seq.filter nonTerminal
                |> Seq.groupBy (fun row -> row.Entity)
                |> Seq.map (fun (_, group) -> Seq.head group)
                |> Seq.filter (fun head ->
                    match head.Status with
                    | Ready -> head.VisibleAt <= now
                    | Leased -> head.Deadline <= now
                    | _ -> false)
                |> Seq.sortBy (fun row -> row.Entity, row.Sequence)
                |> Seq.truncate (ClaimBatch.value batch)
                |> List.ofSeq

            for row in claimed do
                nextToken <- nextToken + 1L
                row.Token <- nextToken
                row.Status <- Leased
                row.Deadline <- now + LeaseDuration.value lease
                row.Deliveries <- row.Deliveries + 1

            let leased =
                claimed
                |> List.map (fun row ->
                    ({ Work = recordOf row
                       Token = LeaseToken.ofInt64 row.Token
                       DeliveryCount = row.Deliveries
                       ExpiresAt = row.Deadline }
                    : LeasedCommand<ContractEntityId, ContractEvent>))

            Task.FromResult(Ok leased)

        member _.Reschedule
            (commandId: CommandId, token: LeaseToken<CommandWork>, _backoff: Backoff, _ct: CancellationToken)
            =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when live row token ->
                row.Status <- Ready
                row.Attempts <- row.Attempts + 1
                row.VisibleAt <- now
                Task.FromResult(Ok Updated)
            | _ -> Task.FromResult(Ok LeaseUpdateOutcome.LeaseLost)

        member _.ExtendLease
            (commandId: CommandId, token: LeaseToken<CommandWork>, lease: LeaseDuration, _ct: CancellationToken)
            =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when live row token ->
                row.Deadline <- now + LeaseDuration.value lease
                Task.FromResult(Ok Updated)
            | _ -> Task.FromResult(Ok LeaseUpdateOutcome.LeaseLost)

        member _.TryGet(commandId: CommandId, _ct: CancellationToken) =
            rows
            |> Seq.tryFind (fun row -> row.Id = commandId)
            |> Option.map recordOf
            |> fun found -> Task.FromResult(Ok found)

        member _.TryFind(_machine: MachineId, entity: ContractEntityId, key: string, _ct: CancellationToken) =
            rows
            |> Seq.tryFind (fun row -> row.EntityId = entity && row.Key = key)
            |> Option.map recordOf
            |> fun found -> Task.FromResult(Ok found)

    interface ContractProcessor with

        member _.Commit(_commandId, _token, _expected, _draft, _ct) =
            failwith "the reference store does not serve commits"

        member _.Reject(commandId: CommandId, token: LeaseToken<CommandWork>, _failure, _ct) =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when live row token ->
                row.Status <- Rejected
                Task.FromResult(Ok(Finalized Epoch.initial))
            | _ -> Task.FromResult(Ok FinalizeOutcome.LeaseLost)

        member _.DeadLetter(commandId: CommandId, token: LeaseToken<CommandWork>, _failure, _ct) =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when live row token ->
                row.Status <- Dead
                Task.FromResult(Ok(Finalized Epoch.initial))
            | _ -> Task.FromResult(Ok FinalizeOutcome.LeaseLost)

        member _.TryGetResult(_commandId, _ct) =
            failwith "the reference store does not serve results"

/// Raw choices for a simulated trace, three to sixteen steps long.
let randomInboxChoices (seed: int) : InboxChoice list =
    let rng = Random seed

    let next () : InboxChoice =
        match rng.Next 6 with
        | 0 -> Submit(byte (rng.Next()), byte (rng.Next()), byte (rng.Next()), byte (rng.Next()))
        | 1 -> Claim(byte (rng.Next()))
        | 2 -> Finish(byte (rng.Next()), byte (rng.Next()))
        | 3 -> Stale(byte (rng.Next()), byte (rng.Next()))
        | 4 -> Advance(byte (rng.Next()))
        | _ -> Lookup(byte (rng.Next()), byte (rng.Next()))

    List.init (3 + rng.Next 14) (fun _ -> next ())

/// The behavior classes a trace covers, decided by running it against the reference store.
let inboxClasses (env: ContractInboxEnv) (choices: InboxChoice list) : string list =
    let fake = FakeInbox env
    let run = runInboxTrace env fake.Stores (List.truncate 14 choices)
    snd (run.GetAwaiter().GetResult())

/// The compiled trace, for reading a shrunk counterexample.
let renderTrace (choices: InboxChoice list) : string =
    let finishKind =
        function
        | 0 -> "reject"
        | 1 -> "deadletter"
        | 2 -> "reschedule"
        | _ -> "extend"

    let lines =
        choices
        |> List.truncate 14
        |> List.map (function
            | Submit(entity, key, event, delay) ->
                let eventText =
                    if int event % 4 = 3 then
                        "Pong"
                    else
                        $"Ping {int event % 4}"

                $"submit {entityName entity} {keyName key} {eventText} delay={delayOf delay}ms"
            | Claim batch -> $"claim batch {batchOf batch}"
            | Finish(kind, claim) -> $"finish {finishKind (finishKindOf kind)} ref {int claim}"
            | Stale(kind, claim) -> $"stale {finishKind (finishKindOf kind)} ref {int claim}"
            | Advance ticks -> $"advance {advanceOf ticks}ms"
            | Lookup(entity, key) -> $"lookup {entityName entity} {keyName key}")

    let counts =
        choices
        |> List.truncate 14
        |> List.countBy (function
            | Submit _ -> "submit"
            | Claim _ -> "claim"
            | Finish _ -> "finish"
            | Stale _ -> "stale"
            | Advance _ -> "advance"
            | Lookup _ -> "lookup")
        |> List.map (fun (kind, count) -> $"{kind}={count}")
        |> String.concat " "

    String.concat "\n" lines + "\ncounts: " + counts

/// The shared inbox property. The law is a task, so every generated case, and every rerun a
/// shrunk counterexample produces, takes its fixture and runs against it for real.
let inboxProperty name maxTests endSize (env: ContractInboxEnv) =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(endSize)

        Check.One(
            config,
            fun (choices: InboxChoice list) ->
                task {
                    let trace = List.truncate 14 choices
                    let! stores = env.Fresh()
                    let! verdict, classes = runInboxTrace env stores trace

                    if not verdict then
                        eprintfn "inbox trace falsified:\n%s" (renderTrace trace)

                    return verdict
                }
        ))

/// Asserts that a thousand simulated traces cover every behavior class the property can assert.
let inboxReachability name (env: ContractInboxEnv) (expected: string list) =
    testCase name (fun () ->
        let classes =
            seq { 0..999 }
            |> Seq.collect (fun seed -> inboxClasses env (randomInboxChoices seed))
            |> Set.ofSeq

        Expect.equal classes (set expected) "every behavior class is reachable from a thousand simulated traces")

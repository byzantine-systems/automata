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

type ContractState =
    | Idle
    | Active of int

type ContractAction = Fired of int
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
    | Commit of claim: byte * expected: byte * state: byte * token: byte
    | Repeat of claim: byte
    | Stale of kind: byte * claim: byte
    | Advance of ticks: byte
    | Lookup of entity: byte * key: byte
    | Result of entity: byte * key: byte

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

let private failureOf (raw: byte) : CommandFailure<ContractError> =
    match int raw / 4 % 4 with
    | 0 -> CommandFailure.Domain(Refused "the trace refused it")
    | 1 -> CommandFailure.Machine "the trace could not process it"
    | 2 -> CommandFailure.Replay(ReplayError.Diverged(Epoch.ofUInt64 3UL, "the trace diverged"))
    | _ -> CommandFailure.Replay ReplayError.HistoryPurged

let private contractSubmission env entity key event =
    { MachineId = env.Machine
      EntityId = entity
      IdempotencyKey = key
      ChartVersion = env.Version
      Kind = CommandKind.Event
      Event = event
      VisibleAt = None
      ReceivedAt = None
      Audit = AuditContext.empty }

let private contractDraft env entity event raw fromState =
    let state = Active(1 + int raw % 7)

    { MachineId = env.Machine
      EntityId = entity
      Event = event
      Actions = [ Fired(1 + int raw % 5) ]
      FromState = fromState
      ToState = state
      HandledBy = stateId "contract"
      Exited = [ stateId "from" ]
      Entered = [ stateId "to" ]
      Status = Running
      EffectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, int raw % 60, TimeSpan.Zero) }

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
    | Finished of ModelTerminal

and private ModelTerminal =
    | CommittedResult of
        expected: Epoch *
        committed: Epoch *
        draft: TransitionDraft<ContractEntityId, ContractState, ContractEvent, ContractAction> *
        before: DateTimeOffset *
        after: DateTimeOffset
    | RejectedResult of CommandFailure<ContractError>
    | DeadLetterResult of CommandFailure<ContractError>

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
        let mutable entityEpochs: Map<string, Epoch> = Map.empty
        let mutable entityStates: Map<string, ContractState> = Map.empty
        let ct = CancellationToken.None

        let finished (tracked: Tracked) =
            match tracked.Status with
            | Finished _ -> true
            | _ -> false

        let commandStatusOf (tracked: Tracked) =
            match tracked.Status with
            | Waiting _ -> CommandStatus.Ready
            | Leased _ -> CommandStatus.Leased
            | Finished(CommittedResult _) -> CommandStatus.Succeeded
            | Finished(RejectedResult _) -> CommandStatus.Rejected
            | Finished(DeadLetterResult _) -> CommandStatus.DeadLettered

        let currentEpoch entity =
            Map.tryFind entity entityEpochs |> Option.defaultValue Epoch.initial

        let currentState entity =
            Map.tryFind entity entityStates |> Option.defaultValue Idle

        let expectedEpoch raw actual =
            match int raw % 3 with
            | 0 -> actual
            | 1 -> Epoch.next actual
            | _ -> Epoch.initial

        let draftOf (tracked: Tracked) raw =
            contractDraft env tracked.EntityId tracked.Event raw (currentState tracked.Entity)

        let resultMatches (tracked: Tracked) observed =
            match tracked.Status, observed with
            | Waiting _, Some CommandResult.Pending
            | Leased _, Some CommandResult.Pending -> true
            | Finished(RejectedResult failure), Some(CommandResult.Rejected actual) -> actual = failure
            | Finished(DeadLetterResult failure), Some(CommandResult.DeadLettered actual) -> actual = failure
            | Finished(CommittedResult(_, epoch, draft, before, after)), Some(CommandResult.Committed actual) ->
                actual.CommandId = tracked.Id
                && actual.ChartVersion = env.Version
                && actual.Epoch = epoch
                && actual.Draft = draft
                && actual.CommittedAt >= before - receivedTolerance
                && actual.CommittedAt <= after + receivedTolerance
            | _ -> false

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
                            | Ok(AlreadySubmitted found) when found = tracked.Id && event = tracked.Event ->
                                classes.Add "resubmit"
                                return! loop rest
                            | Ok(SubmissionOutcome.Refused(KeyReused found)) when
                                found = tracked.Id && event <> tracked.Event
                                ->
                                classes.Add "key-reused"
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
                                let failure = failureOf kind

                                match finishKindOf kind with
                                | 0 ->
                                    let! outcome = stores.Processor.Reject(tracked.Id, tokenId, failure, ct)

                                    match outcome with
                                    | Ok(Finalized committed) when committed = Epoch.initial ->
                                        tracked.Status <- Finished(RejectedResult failure)
                                        tracked.LiveClaim <- None
                                        classes.Add "reject-releases"
                                        return! loop rest
                                    | _ -> return false
                                | 1 ->
                                    let! outcome = stores.Processor.DeadLetter(tracked.Id, tokenId, failure, ct)

                                    match outcome with
                                    | Ok(Finalized committed) when committed = Epoch.initial ->
                                        tracked.Status <- Finished(DeadLetterResult failure)
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

                    | Commit(claim, expected, state, tokenKind) ->
                        if claims.Count = 0 then
                            return! loop rest
                        else
                            let claimIndex = int claim % claims.Count
                            let commandIndex, claimedToken = claims[claimIndex]
                            let tracked = commands[commandIndex]

                            let token =
                                match int tokenKind % 3 with
                                | 0 -> claimedToken
                                | 1 -> 0L
                                | _ -> max 0L (claimedToken - 1L)

                            match tracked.Status with
                            | Finished(RejectedResult _)
                            | Finished(DeadLetterResult _) when token = tracked.RowToken ->
                                // A cross-resolution retry: the command finished under a
                                // different resolution, and the answer reports the stored
                                // status so the caller can tell. The epoch is the caller's
                                // own expectation echoed back, because a failure wrote no
                                // transition to name one.
                                let actual = currentEpoch tracked.Entity
                                let expected = expectedEpoch expected actual

                                let! outcome =
                                    stores.Processor.Commit(
                                        tracked.Id,
                                        LeaseToken.ofInt64 token,
                                        expected,
                                        draftOf tracked state,
                                        ct
                                    )

                                if outcome = Ok(AlreadyFinalized(expected, commandStatusOf tracked)) then
                                    classes.Add "commit-already"
                                    return! loop rest
                                else
                                    return false
                            | status ->
                                let actual = currentEpoch tracked.Entity
                                let expected = expectedEpoch expected actual
                                let draft = draftOf tracked state
                                let before = stores.Now()

                                let! outcome =
                                    stores.Processor.Commit(tracked.Id, LeaseToken.ofInt64 token, expected, draft, ct)

                                let after = stores.Now()

                                match status with
                                | Finished(CommittedResult(_, committed, _, _, _)) when token = tracked.RowToken ->
                                    if outcome = Ok(AlreadyFinalized(committed, CommandStatus.Succeeded)) then
                                        classes.Add "commit-already"
                                        return! loop rest
                                    else
                                        return false
                                | Finished _ ->
                                    if outcome = Ok FinalizeOutcome.LeaseLost then
                                        classes.Add "terminal-stale-commit"
                                        return! loop rest
                                    else
                                        return false
                                | Waiting _ ->
                                    if outcome = Ok FinalizeOutcome.LeaseLost then
                                        if token = 0L then
                                            classes.Add "ready-zero-token-fenced"
                                        else
                                            classes.Add "commit-stale-fenced"

                                        return! loop rest
                                    else
                                        return false
                                | Leased _ when token <> tracked.RowToken ->
                                    if outcome = Ok FinalizeOutcome.LeaseLost then
                                        classes.Add "commit-stale-fenced"
                                        return! loop rest
                                    else
                                        return false
                                | Leased _ when expected <> actual ->
                                    if outcome = Ok(Conflict(expected, actual)) then
                                        classes.Add "commit-conflict"
                                        return! loop rest
                                    else
                                        return false
                                | Leased deadline ->
                                    let committed = Epoch.next expected

                                    if outcome = Ok(Finalized committed) then
                                        if before >= deadline then
                                            classes.Add "expired-unreclaimed"

                                        entityEpochs <- Map.add tracked.Entity committed entityEpochs
                                        entityStates <- Map.add tracked.Entity draft.ToState entityStates

                                        tracked.Status <-
                                            Finished(CommittedResult(expected, committed, draft, before, after))

                                        tracked.LiveClaim <- None
                                        classes.Add "commit-finalized"
                                        return! loop rest
                                    else
                                        return false

                    | Repeat(claim) ->
                        if claims.Count = 0 then
                            return! loop rest
                        else
                            let claimIndex = int claim % claims.Count
                            let commandIndex, token = claims[claimIndex]
                            let tracked = commands[commandIndex]

                            if token <> tracked.RowToken then
                                return! loop rest
                            else
                                let token = LeaseToken.ofInt64 token

                                match tracked.Status with
                                | Finished(CommittedResult(expected, committed, draft, _, _)) ->
                                    let! outcome = stores.Processor.Commit(tracked.Id, token, expected, draft, ct)

                                    if outcome = Ok(AlreadyFinalized(committed, CommandStatus.Succeeded)) then
                                        classes.Add "commit-already"
                                        return! loop rest
                                    else
                                        return false
                                | Finished(RejectedResult failure) ->
                                    let! outcome = stores.Processor.Reject(tracked.Id, token, failure, ct)

                                    if outcome = Ok(AlreadyFinalized(Epoch.initial, CommandStatus.Rejected)) then
                                        classes.Add "reject-already"
                                        return! loop rest
                                    else
                                        return false
                                | Finished(DeadLetterResult failure) ->
                                    let! outcome = stores.Processor.DeadLetter(tracked.Id, token, failure, ct)

                                    if outcome = Ok(AlreadyFinalized(Epoch.initial, CommandStatus.DeadLettered)) then
                                        classes.Add "deadletter-already"
                                        return! loop rest
                                    else
                                        return false
                                | _ -> return! loop rest

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

                    | Result(entity, key) ->
                        let name = entityName entity
                        let idempotencyKey = keyName key

                        match
                            commands
                            |> Seq.tryFind (fun tracked -> tracked.Entity = name && tracked.Key = idempotencyKey)
                        with
                        | Some tracked ->
                            let! observed = stores.Processor.TryGetResult(tracked.Id, ct)

                            match observed with
                            | Ok result when resultMatches tracked result ->
                                match tracked.Status with
                                | Waiting _
                                | Leased _ -> classes.Add "result-pending"
                                | Finished(CommittedResult _) -> classes.Add "result-committed"
                                | Finished(RejectedResult _) -> classes.Add "result-rejected"
                                | Finished(DeadLetterResult _) -> classes.Add "result-deadlettered"

                                return! loop rest
                            | _ -> return false
                        | None ->
                            let! observed = stores.Processor.TryGetResult(CommandId.ofInt64 404L, ct)

                            match observed with
                            | Ok None ->
                                classes.Add "result-missing"
                                return! loop rest
                            | _ -> return false
            }

        let! verdict = loop (List.truncate 14 choices)
        return verdict, List.ofSeq classes
    }

// ---------------------------------------------------------------------------------------------
// An in-memory reference store, so simulated traces can prove the generated shapes reach every
// behavior class before any database is asked to run them.
// ---------------------------------------------------------------------------------------------

type private FakeStatus =
    | Ready
    | Leased
    | Succeeded
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
      mutable Result:
          CommandResult<ContractEntityId, ContractState, ContractEvent, ContractAction, ContractError> option
      ReceivedAt: DateTimeOffset }

/// The fake's own status labels, as the contract spells them. The finalize answers carry the
/// stored status, so the mapping lives once beside the row it reads from.
let private commandStatusOf (status: FakeStatus) =
    match status with
    | Ready -> CommandStatus.Ready
    | Leased -> CommandStatus.Leased
    | Succeeded -> CommandStatus.Succeeded
    | Rejected -> CommandStatus.Rejected
    | Dead -> CommandStatus.DeadLettered

/// The contract as the reference sees it: exactly the behavior the runner asserts, nothing more.
type FakeInbox(env: ContractInboxEnv) =
    let mutable now = DateTimeOffset(2026, 10, 1, 8, 0, 0, TimeSpan.Zero)
    let mutable nextId = 0L
    let mutable nextToken = 0L
    let mutable entityEpochs: Map<string, Epoch> = Map.empty
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
          Status = commandStatusOf row.Status
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
            | Some row when row.Event = submission.Event -> Task.FromResult(Ok(AlreadySubmitted row.Id))
            | Some row -> Task.FromResult(Ok(SubmissionOutcome.Refused(KeyReused row.Id)))
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
                      Result = None
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
                row.Token <- 0L
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

        member _.Commit(commandId, token, expected, draft, _ct) =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when row.Status = Succeeded || row.Status = Rejected || row.Status = Dead ->
                if row.Token <> LeaseToken.value token then
                    Task.FromResult(Ok FinalizeOutcome.LeaseLost)
                else
                    // The epoch is the one a commit wrote, or the caller's own expectation
                    // echoed back when a failure left no transition; the status is always what
                    // was stored, so a retry with a different resolution can tell.
                    let stored =
                        match row.Result with
                        | Some(CommandResult.Committed committed) -> committed.Epoch, CommandStatus.Succeeded
                        | _ -> expected, commandStatusOf row.Status

                    Task.FromResult(Ok(AlreadyFinalized stored))
            | Some row when row.Status = Leased && row.Token = LeaseToken.value token ->
                let actual =
                    Map.tryFind row.Entity entityEpochs |> Option.defaultValue Epoch.initial

                if expected <> actual then
                    Task.FromResult(Ok(Conflict(expected, actual)))
                else
                    let committed = Epoch.next expected

                    row.Result <-
                        Some(
                            CommandResult.Committed
                                { Draft = draft
                                  Epoch = committed
                                  CommandId = row.Id
                                  ChartVersion = env.Version
                                  CommittedAt = now }
                        )

                    row.Status <- Succeeded
                    entityEpochs <- Map.add row.Entity committed entityEpochs
                    Task.FromResult(Ok(Finalized committed))
            | _ -> Task.FromResult(Ok FinalizeOutcome.LeaseLost)

        member _.Reject(commandId: CommandId, token: LeaseToken<CommandWork>, failure, _ct) =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when row.Status = Rejected && row.Token = LeaseToken.value token ->
                Task.FromResult(Ok(AlreadyFinalized(Epoch.initial, CommandStatus.Rejected)))
            | Some row when row.Status = Leased && row.Token = LeaseToken.value token ->
                row.Status <- Rejected
                row.Result <- Some(CommandResult.Rejected failure)
                Task.FromResult(Ok(Finalized Epoch.initial))
            | _ -> Task.FromResult(Ok FinalizeOutcome.LeaseLost)

        member _.DeadLetter(commandId: CommandId, token: LeaseToken<CommandWork>, failure, _ct) =
            match rows |> Seq.tryFind (fun row -> row.Id = commandId) with
            | Some row when row.Status = Dead && row.Token = LeaseToken.value token ->
                Task.FromResult(Ok(AlreadyFinalized(Epoch.initial, CommandStatus.DeadLettered)))
            | Some row when row.Status = Leased && row.Token = LeaseToken.value token ->
                row.Status <- Dead
                row.Result <- Some(CommandResult.DeadLettered failure)
                Task.FromResult(Ok(Finalized Epoch.initial))
            | _ -> Task.FromResult(Ok FinalizeOutcome.LeaseLost)

        member _.TryGetResult(commandId, _ct) =
            rows
            |> Seq.tryFind (fun row -> row.Id = commandId)
            |> Option.map (fun row -> defaultArg row.Result CommandResult.Pending)
            |> fun found -> Task.FromResult(Ok found)

/// Raw choices for a simulated trace, three to sixteen steps long.
let randomInboxChoices (seed: int) : InboxChoice list =
    let rng = Random seed

    let next () : InboxChoice =
        match rng.Next 9 with
        | 0 -> Submit(byte (rng.Next()), byte (rng.Next()), byte (rng.Next()), byte (rng.Next()))
        | 1 -> Claim(byte (rng.Next()))
        | 2 -> Finish(byte (rng.Next()), byte (rng.Next()))
        | 3 -> Commit(byte (rng.Next()), byte (rng.Next()), byte (rng.Next()), byte (rng.Next()))
        | 4 -> Repeat(byte (rng.Next()))
        | 5 -> Stale(byte (rng.Next()), byte (rng.Next()))
        | 6 -> Advance(byte (rng.Next()))
        | 7 -> Lookup(byte (rng.Next()), byte (rng.Next()))
        | _ -> Result(byte (rng.Next()), byte (rng.Next()))

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
            | Commit(claim, expected, state, token) ->
                $"commit ref {int claim} expected={int expected % 3} state={1 + int state % 7} token={int token % 3}"
            | Repeat claim -> $"repeat ref {int claim}"
            | Stale(kind, claim) -> $"stale {finishKind (finishKindOf kind)} ref {int claim}"
            | Advance ticks -> $"advance {advanceOf ticks}ms"
            | Lookup(entity, key) -> $"lookup {entityName entity} {keyName key}"
            | Result(entity, key) -> $"result {entityName entity} {keyName key}")

    let counts =
        choices
        |> List.truncate 14
        |> List.countBy (function
            | Submit _ -> "submit"
            | Claim _ -> "claim"
            | Finish _ -> "finish"
            | Commit _ -> "commit"
            | Repeat _ -> "repeat"
            | Stale _ -> "stale"
            | Advance _ -> "advance"
            | Lookup _ -> "lookup"
            | Result _ -> "result")
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

/// A key names one immutable request. Retries may change delivery/audit metadata, but cannot
/// replace any request field, allocate a sequence, or change the stored command on refusal.
let requestIdentityProperty name maxTests (env: ContractInboxEnv) =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests)
        let ct = CancellationToken.None

        Check.One(
            config,
            fun (payload: byte, seconds: byte) ->
                task {
                    let! stores = env.Fresh()
                    let entity: ContractEntityId = entityId "request-identity"
                    let original = contractSubmission env entity "identity" (Ping(int payload))

                    let at =
                        DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(float seconds)

                    let correction = CommandKind.Correction(at, CorrectionPolicy.defaults)

                    let changed =
                        [ { original with
                              Event = Ping(int payload + 1) }
                          { original with
                              ChartVersion = ChartVersion.create (ChartVersion.value env.Version + 1) }
                          { original with Kind = correction } ]

                    let! first = stores.Inbox.Submit(original, ct)
                    let! before = stores.Inbox.TryFind(env.Machine, entity, original.IdempotencyKey, ct)

                    match first with
                    | Ok(Accepted commandId) ->
                        let! retry =
                            stores.Inbox.Submit(
                                { original with
                                    VisibleAt = Some(stores.Now().AddDays 1.)
                                    ReceivedAt = Some at
                                    Audit =
                                        { AuditContext.empty with
                                            CorrelationId = Some "retry" } },
                                ct
                            )

                        Expect.equal retry (Ok(AlreadySubmitted commandId)) "retry metadata does not redefine identity"

                        for request in changed do
                            let! outcome = stores.Inbox.Submit(request, ct)

                            Expect.equal
                                outcome
                                (Ok(SubmissionOutcome.Refused(KeyReused commandId)))
                                "a changed request is refused"

                        let! after = stores.Inbox.TryFind(env.Machine, entity, original.IdempotencyKey, ct)
                        Expect.equal after before "refused submissions do not alter the original"

                        let correctionRequest =
                            { original with
                                IdempotencyKey = "correction"
                                Kind = correction }

                        let! acceptedCorrection = stores.Inbox.Submit(correctionRequest, ct)

                        match acceptedCorrection with
                        | Ok(Accepted correctionId) ->
                            let! same = stores.Inbox.Submit(correctionRequest, ct)

                            Expect.equal
                                same
                                (Ok(AlreadySubmitted correctionId))
                                "a correction also retries identically"

                            for kind in
                                [ CommandKind.Event
                                  CommandKind.Correction(at.AddSeconds 1., CorrectionPolicy.defaults)
                                  CommandKind.Correction(
                                      at,
                                      { CorrectionPolicy.defaults with
                                          ReplayLimit = CorrectionPolicy.defaults.ReplayLimit + 1 }
                                  )
                                  CommandKind.Correction(
                                      at,
                                      { CorrectionPolicy.defaults with
                                          OnDivergence = Divergence.Truncate }
                                  ) ] do
                                let! outcome = stores.Inbox.Submit({ correctionRequest with Kind = kind }, ct)

                                Expect.equal
                                    outcome
                                    (Ok(SubmissionOutcome.Refused(KeyReused correctionId)))
                                    "correction fields define identity"

                            let! next = stores.Inbox.TryFind(env.Machine, entity, "correction", ct)

                            Expect.equal
                                (next |> Result.map (Option.map _.Sequence))
                                (Ok(Some 2L))
                                "refusals allocate no sequence"
                        | other -> failtestf "expected a new correction, got %A" other
                    | other -> failtestf "expected a new command, got %A" other
                }
        ))

/// Sequence order implies nondecreasing arrival time, including a predecessor ahead of the
/// database clock. An explicitly earlier arrival is refused without consuming a sequence.
let arrivalOrderProperty name maxTests (env: ContractInboxEnv) =
    testCase name (fun () ->
        Check.One(
            Config.QuickThrowOnFailure.WithMaxTest(maxTests),
            fun (seconds: byte) ->
                task {
                    let! stores = env.Fresh()
                    let entity: ContractEntityId = entityId "arrival-order"
                    let at = stores.Now().AddDays(1.).AddSeconds(float seconds)
                    // Persisted instants have microsecond precision on both providers.
                    let at = DateTimeOffset(at.Ticks / 10L * 10L, at.Offset)

                    let original =
                        { contractSubmission env entity "first" Pong with
                            ReceivedAt = Some at }

                    let ct = CancellationToken.None
                    let! first = stores.Inbox.Submit(original, ct)
                    Expect.isOk first "the first arrival is unconstrained"

                    let! earlier =
                        stores.Inbox.Submit(
                            { original with
                                IdempotencyKey = "earlier"
                                ReceivedAt = Some(at.AddSeconds -1.) },
                            ct
                        )

                    Expect.equal
                        earlier
                        (Ok(SubmissionOutcome.Refused(ArrivedBeforePredecessor at)))
                        "an earlier caller arrival is refused"

                    let! stamped =
                        stores.Inbox.Submit(
                            { original with
                                IdempotencyKey = "stamped"
                                ReceivedAt = None },
                            ct
                        )

                    Expect.isOk stamped "database time is clamped rather than refused"
                    let! found = stores.Inbox.TryFind(env.Machine, entity, "stamped", ct)

                    match found with
                    | Ok(Some row) ->
                        Expect.equal row.ReceivedAt at "a future predecessor clamps the database clock"
                        Expect.equal row.Sequence 2L "refusals consume no sequence"
                    | other -> failtestf "expected the stamped arrival, got %A" other

                    let! equal =
                        stores.Inbox.Submit(
                            { original with
                                IdempotencyKey = "equal" },
                            ct
                        )

                    Expect.isOk equal "equal caller arrivals are permitted"

                    let! correction =
                        stores.Inbox.Submit(
                            { original with
                                IdempotencyKey = "correction"
                                Kind = CommandKind.Correction(at.AddDays -1., CorrectionPolicy.defaults) },
                            ct
                        )

                    Expect.isOk correction "correction arrival time is also clamped"
                    let! foundCorrection = stores.Inbox.TryFind(env.Machine, entity, "correction", ct)

                    Expect.equal
                        (foundCorrection |> Result.map (Option.map _.ReceivedAt))
                        (Ok(Some at))
                        "correction effective time does not reorder arrival"
                }
        ))

/// Provider hooks only arrange corruption and observe its durable quarantine; the batch is
/// supplied and claimed through public store interfaces.
type PoisonBatchFixture =
    { Ids: CommandId list
      Corrupt: CommandId -> Task<unit>
      Claim: unit -> Task<Result<CommandId list, StoreError>>
      IsQuarantined: CommandId -> Task<bool> }

let poisonBatchProperty name maxTests (fresh: int -> int -> Task<PoisonBatchFixture>) =
    testCase name (fun () ->
        Check.One(
            Config.QuickThrowOnFailure.WithMaxTest(maxTests),
            fun (size: byte, position: byte, payload: byte) ->
                task {
                    let count = 1 + int size % 7
                    let! fixture = fresh count (int payload)
                    let poison = fixture.Ids[int position % count]
                    do! fixture.Corrupt poison
                    let! claimed = fixture.Claim()
                    let expected = fixture.Ids |> List.filter ((<>) poison) |> Set.ofList

                    Expect.equal
                        (claimed |> Result.map Set.ofList)
                        (Ok expected)
                        "healthy rows survive every poison position"

                    let! quarantined = fixture.IsQuarantined poison
                    Expect.isTrue quarantined "the poison has a durable dead letter"
                    let! next = fixture.Claim()
                    Expect.equal next (Ok []) "the poison no longer wedges claims"
                }
        ))

/// The finalize equations through public observations. Every generated case runs every semantic
/// branch against fresh stores; the generated payload varies codecs and transition data without
/// leaving critical outcomes to a low-probability command sequence.
let finalizeProperty name maxTests (env: ContractInboxEnv) =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(20)
        let ct = CancellationToken.None

        let claimKey (stores: ContractStores) key =
            task {
                match! stores.Inbox.Claim(env.Machine, ClaimBatch.create 2, env.ClaimLease, ct) with
                | Ok claimed -> return claimed |> List.tryFind (fun held -> held.Work.IdempotencyKey = key)
                | Error _ -> return None
            }

        let setup payload withSibling =
            task {
                let! stores = env.Fresh()
                let entity: ContractEntityId = entityId "finalize"
                let event = eventOf payload
                let target = contractSubmission env entity "target" event

                match! stores.Inbox.Submit(target, ct) with
                | Error _ -> return None
                | Ok _ ->
                    let! siblingSubmitted =
                        if withSibling then
                            task {
                                let siblingPayload = byte ((int payload + 1) % 256)

                                let! outcome =
                                    stores.Inbox.Submit(
                                        contractSubmission env entity "sibling" (eventOf siblingPayload),
                                        ct
                                    )

                                return Some outcome
                            }
                        else
                            Task.FromResult None

                    match siblingSubmitted with
                    | Some(Error _) -> return None
                    | _ ->
                        let! held = claimKey stores "target"
                        return held |> Option.map (fun target -> stores, target)
            }

        let committedResultMatches held draft epoch (before: DateTimeOffset) (after: DateTimeOffset) =
            function
            | Ok(Some(CommandResult.Committed committed)) ->
                committed.CommandId = held.Work.CommandId
                && committed.ChartVersion = env.Version
                && committed.Epoch = epoch
                && committed.Draft = draft
                && committed.CommittedAt >= before - receivedTolerance
                && committed.CommittedAt <= after + receivedTolerance
            | _ -> false

        let commitLaw payload =
            task {
                match! setup payload true with
                | None -> return false
                | Some(stores, first) ->
                    let firstDraft = contractDraft env first.Work.EntityId first.Work.Event payload Idle
                    let before = stores.Now()

                    let! firstOutcome =
                        stores.Processor.Commit(first.Work.CommandId, first.Token, Epoch.initial, firstDraft, ct)

                    let after = stores.Now()
                    let! firstResult = stores.Processor.TryGetResult(first.Work.CommandId, ct)
                    let! second = claimKey stores "sibling"

                    match second with
                    | None -> return false
                    | Some second ->
                        let firstEpoch = Epoch.next Epoch.initial
                        let secondPayload = byte ((int payload + 1) % 256)

                        let secondDraft =
                            contractDraft env second.Work.EntityId second.Work.Event secondPayload firstDraft.ToState

                        let! secondOutcome =
                            stores.Processor.Commit(second.Work.CommandId, second.Token, firstEpoch, secondDraft, ct)

                        let! repeated =
                            stores.Processor.Commit(first.Work.CommandId, first.Token, Epoch.initial, firstDraft, ct)

                        return
                            firstOutcome = Ok(Finalized firstEpoch)
                            && committedResultMatches first firstDraft firstEpoch before after firstResult
                            && secondOutcome = Ok(Finalized(Epoch.next firstEpoch))
                            && repeated = Ok(AlreadyFinalized(firstEpoch, CommandStatus.Succeeded))
            }

        let conflictLaw payload =
            task {
                match! setup payload false with
                | None -> return false
                | Some(stores, held) ->
                    let draft = contractDraft env held.Work.EntityId held.Work.Event payload Idle
                    let ahead = Epoch.next Epoch.initial
                    let! conflict = stores.Processor.Commit(held.Work.CommandId, held.Token, ahead, draft, ct)
                    let! pending = stores.Processor.TryGetResult(held.Work.CommandId, ct)
                    let! recovered = stores.Processor.Commit(held.Work.CommandId, held.Token, Epoch.initial, draft, ct)

                    return
                        conflict = Ok(Conflict(ahead, Epoch.initial))
                        && pending = Ok(Some CommandResult.Pending)
                        && recovered = Ok(Finalized ahead)
            }

        let failureLaw payload reject failure =
            task {
                match! setup payload true with
                | None -> return false
                | Some(stores, held) ->
                    let finalize () =
                        if reject then
                            stores.Processor.Reject(held.Work.CommandId, held.Token, failure, ct)
                        else
                            stores.Processor.DeadLetter(held.Work.CommandId, held.Token, failure, ct)

                    let! first = finalize ()
                    let! repeated = finalize ()
                    let! result = stores.Processor.TryGetResult(held.Work.CommandId, ct)
                    let! sibling = claimKey stores "sibling"

                    let expectedResult =
                        if reject then
                            Some(CommandResult.Rejected failure)
                        else
                            Some(CommandResult.DeadLettered failure)

                    let expectedStatus =
                        if reject then
                            CommandStatus.Rejected
                        else
                            CommandStatus.DeadLettered

                    return
                        first = Ok(Finalized Epoch.initial)
                        && repeated = Ok(AlreadyFinalized(Epoch.initial, expectedStatus))
                        && result = Ok expectedResult
                        && sibling.IsSome
            }

        let failureLaws payload =
            task {
                let failures =
                    [ true, CommandFailure.Domain(Refused $"domain-{payload}")
                      false, CommandFailure.Machine $"machine-{payload}"
                      true, CommandFailure.Replay(ReplayError.Diverged(Epoch.ofUInt64 3UL, $"diverged-{payload}"))
                      false, CommandFailure.Replay ReplayError.HistoryPurged ]

                let mutable valid = true

                for index, (reject, failure) in List.indexed failures do
                    let varied = byte ((int payload + index) % 256)
                    let! observed = failureLaw varied reject failure
                    valid <- valid && observed

                return valid
            }

        let staleLaw payload =
            task {
                match! setup payload false with
                | None -> return false
                | Some(stores, held) ->
                    let draft = contractDraft env held.Work.EntityId held.Work.Event payload Idle
                    let stale = LeaseToken.ofInt64 (max 0L (LeaseToken.value held.Token - 1L))
                    let! fenced = stores.Processor.Commit(held.Work.CommandId, stale, Epoch.initial, draft, ct)
                    let! pending = stores.Processor.TryGetResult(held.Work.CommandId, ct)
                    let! recovered = stores.Processor.Commit(held.Work.CommandId, held.Token, Epoch.initial, draft, ct)

                    return
                        fenced = Ok FinalizeOutcome.LeaseLost
                        && pending = Ok(Some CommandResult.Pending)
                        && recovered = Ok(Finalized(Epoch.next Epoch.initial))
            }

        let expiredLaw payload =
            task {
                match! setup payload false with
                | None -> return false
                | Some(stores, held) ->
                    do! stores.Advance(LeaseDuration.value env.ClaimLease + env.Margin + env.Ticks 20)

                    let draft = contractDraft env held.Work.EntityId held.Work.Event payload Idle
                    let! outcome = stores.Processor.Commit(held.Work.CommandId, held.Token, Epoch.initial, draft, ct)
                    return outcome = Ok(Finalized(Epoch.next Epoch.initial))
            }

        let reclaimedLaw payload =
            task {
                match! setup payload false with
                | None -> return false
                | Some(stores, stale) ->
                    do! stores.Advance(LeaseDuration.value env.ClaimLease + env.Margin + env.Ticks 20)

                    let! reclaimed = claimKey stores "target"

                    match reclaimed with
                    | None -> return false
                    | Some holder ->
                        let draft = contractDraft env holder.Work.EntityId holder.Work.Event payload Idle
                        let before = stores.Now()

                        let! fenced =
                            stores.Processor.Commit(stale.Work.CommandId, stale.Token, Epoch.initial, draft, ct)

                        let! committed =
                            stores.Processor.Commit(holder.Work.CommandId, holder.Token, Epoch.initial, draft, ct)

                        let after = stores.Now()

                        let! terminalFenced =
                            stores.Processor.Commit(stale.Work.CommandId, stale.Token, Epoch.initial, draft, ct)

                        let! result = stores.Processor.TryGetResult(holder.Work.CommandId, ct)
                        let epoch = Epoch.next Epoch.initial

                        return
                            LeaseToken.value holder.Token > LeaseToken.value stale.Token
                            && fenced = Ok FinalizeOutcome.LeaseLost
                            && committed = Ok(Finalized epoch)
                            && terminalFenced = Ok FinalizeOutcome.LeaseLost
                            && committedResultMatches holder draft epoch before after result
            }

        /// A retry that sends a different resolution than the one that landed is answered with
        /// the stored status, so the caller can tell its resolution was not the one recorded.
        let crossResolutionLaw payload =
            task {
                match! setup payload true with
                | None -> return false
                | Some(stores, held) ->
                    let failure = failureOf payload
                    let draft = contractDraft env held.Work.EntityId held.Work.Event payload Idle
                    let epoch = Epoch.next Epoch.initial

                    let! committed = stores.Processor.Commit(held.Work.CommandId, held.Token, Epoch.initial, draft, ct)

                    // The command committed; a late rejection and a late dead letter are both
                    // told the stored status rather than what they asked for.
                    let! lateRejection = stores.Processor.Reject(held.Work.CommandId, held.Token, failure, ct)

                    let! lateDeadLetter = stores.Processor.DeadLetter(held.Work.CommandId, held.Token, failure, ct)

                    match! claimKey stores "sibling" with
                    | None -> return false
                    | Some sibling ->
                        // A fresh command rejected; a late commit is told it never committed.
                        // Its epoch echo is the caller's expectation, which is the entity's
                        // epoch after the first command committed.
                        let siblingPayload = byte ((int payload + 1) % 256)

                        let! rejected = stores.Processor.Reject(sibling.Work.CommandId, sibling.Token, failure, ct)

                        let! lateCommit =
                            stores.Processor.Commit(
                                sibling.Work.CommandId,
                                sibling.Token,
                                epoch,
                                contractDraft env sibling.Work.EntityId sibling.Work.Event siblingPayload draft.ToState,
                                ct
                            )

                        return
                            committed = Ok(Finalized epoch)
                            && lateRejection = Ok(AlreadyFinalized(epoch, CommandStatus.Succeeded))
                            && lateDeadLetter = Ok(AlreadyFinalized(epoch, CommandStatus.Succeeded))
                            && rejected = Ok(Finalized Epoch.initial)
                            && lateCommit = Ok(AlreadyFinalized(epoch, CommandStatus.Rejected))
            }

        Check.One(
            config,
            fun (payload: byte) ->
                task {
                    let! committed = commitLaw payload
                    let! conflicted = conflictLaw payload
                    let! failed = failureLaws payload
                    let! stale = staleLaw payload
                    let! expired = expiredLaw payload
                    let! reclaimed = reclaimedLaw payload
                    let! crossed = crossResolutionLaw payload
                    return committed && conflicted && failed && stale && expired && reclaimed && crossed
                }
        ))

/// A ready row carries token zero as a nobody-holds sentinel. Rescheduling releases the real
/// lease, so no finalize operation may turn that sentinel back into authority.
let readyTokenProperty name maxTests (env: ContractInboxEnv) =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(20)

        Check.One(
            config,
            fun (ending: byte, payload: byte) ->
                task {
                    let! stores = env.Fresh()
                    let entity: ContractEntityId = entityId "ready-token"

                    let submission: CommandSubmission<ContractEntityId, ContractEvent> =
                        { MachineId = env.Machine
                          EntityId = entity
                          IdempotencyKey = "ready-token"
                          ChartVersion = env.Version
                          Kind = CommandKind.Event
                          Event = eventOf payload
                          VisibleAt = None
                          ReceivedAt = None
                          Audit = AuditContext.empty }

                    match! stores.Inbox.Submit(submission, CancellationToken.None) with
                    | Error _ -> return false
                    | Ok _ ->
                        match!
                            stores.Inbox.Claim(
                                env.Machine,
                                ClaimBatch.create 1,
                                env.ClaimLease,
                                CancellationToken.None
                            )
                        with
                        | Error _
                        | Ok [] -> return false
                        | Ok(held :: _) ->
                            match!
                                stores.Inbox.Reschedule(
                                    held.Work.CommandId,
                                    held.Token,
                                    env.Backoff,
                                    CancellationToken.None
                                )
                            with
                            | Error _
                            | Ok LeaseUpdateOutcome.LeaseLost -> return false
                            | Ok Updated ->
                                let nobody = LeaseToken.ofInt64 0L
                                let failure = failureOf payload

                                let! outcome =
                                    match int ending % 3 with
                                    | 0 ->
                                        let state = Active(1 + int payload % 7)

                                        let draft =
                                            { MachineId = env.Machine
                                              EntityId = entity
                                              Event = submission.Event
                                              Actions = [ Fired(1 + int payload % 5) ]
                                              FromState = Idle
                                              ToState = state
                                              HandledBy = stateId "contract"
                                              Exited = []
                                              Entered = []
                                              Status = Running
                                              EffectiveAt = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero) }

                                        stores.Processor.Commit(
                                            held.Work.CommandId,
                                            nobody,
                                            Epoch.initial,
                                            draft,
                                            CancellationToken.None
                                        )
                                    | 1 ->
                                        stores.Processor.Reject(
                                            held.Work.CommandId,
                                            nobody,
                                            failure,
                                            CancellationToken.None
                                        )
                                    | _ ->
                                        stores.Processor.DeadLetter(
                                            held.Work.CommandId,
                                            nobody,
                                            failure,
                                            CancellationToken.None
                                        )

                                let! result =
                                    stores.Processor.TryGetResult(held.Work.CommandId, CancellationToken.None)

                                return
                                    outcome = Ok FinalizeOutcome.LeaseLost
                                    && result = Ok(Some CommandResult.Pending)
                }
        ))

/// Asserts that broad random traces plus constructively reachable result paths cover every
/// behavior class the property can assert.
let inboxReachability name (env: ContractInboxEnv) (expected: string list) =
    testCase name (fun () ->
        let directed =
            [ [ Submit(0uy, 0uy, 0uy, 0uy); Result(0uy, 0uy) ]
              [ Submit(0uy, 0uy, 0uy, 0uy)
                Claim 0uy
                Commit(0uy, 0uy, 0uy, 0uy)
                Result(0uy, 0uy) ]
              [ Submit(0uy, 0uy, 0uy, 0uy); Claim 0uy; Finish(0uy, 0uy); Result(0uy, 0uy) ]
              [ Submit(0uy, 0uy, 0uy, 0uy); Claim 0uy; Finish(1uy, 0uy); Result(0uy, 0uy) ] ]

        let classes =
            seq {
                for seed in 0..999 do
                    yield randomInboxChoices seed

                yield! directed
            }
            |> Seq.collect (inboxClasses env)
            |> Set.ofSeq

        let expected = set expected

        Expect.equal
            (Set.difference expected classes, Set.difference classes expected)
            (Set.empty, Set.empty)
            "every behavior class is reachable from generated traces (missing, unexpected)")

// ---------------------------------------------------------------------------------------------
// The history equations.
//
// A finite per-entity log, built only through Submit, Claim and Commit, drives the durable
// snapshot and the paged history of both readers. A model that knows only the documented
// contract predicts every answer: the snapshot is the latest transition's state, epoch and
// status, a page is the log filtered by an exclusive epoch cursor and truncated by the limit,
// and a page pinned to a cursor does not move as the log grows behind it.
// ---------------------------------------------------------------------------------------------

type ContractReader = IStateReader<ContractEntityId, ContractState, ContractEvent, ContractAction>

/// The stores one provider supplies per generated history case, plus its view of now.
type ContractHistoryStores =
    {
        Inbox: ContractInbox
        Processor: ContractProcessor
        Reader: ContractReader
        /// The effective now for bounding commit times: the fixture's frozen clock, or the real one.
        Now: unit -> DateTimeOffset
    }

/// Everything a provider fills in to run the shared history property.
type ContractHistoryEnv =
    {
        Machine: MachineId
        Version: ChartVersion
        /// Commit instants are stamped by the database, so read-backs compare against a
        /// tolerance that covers the difference between the caller's clock and the store's.
        CommittedAtTolerance: TimeSpan
        /// A fixture with nothing in it, once per generated case, shrinking reruns included.
        Fresh: unit -> Task<ContractHistoryStores>
    }

type private ContractTransition = CommittedTransition<ContractEntityId, ContractState, ContractEvent, ContractAction>

type private AppendSeed =
    { Entity: byte
      Payload: byte
      Status: byte
      EffectiveAt: byte }

type private PageSeed =
    { Entity: byte
      Cursor: byte
      Limit: byte }

type private HistoryCase =
    { Appends: AppendSeed list
      GrowthSplit: byte
      Pages: PageSeed list }

let private historyStatus (raw: byte) : InstanceStatus =
    match int raw % 3 with
    | 0 -> Running
    | 1 -> Suspended
    | _ -> Terminated

let private historyEffectiveAt (index: int) (raw: byte) : DateTimeOffset =
    DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(float (index * 60 + int raw % 60))

let private historyDraft
    (env: ContractHistoryEnv)
    (entity: ContractEntityId)
    (event: ContractEvent)
    (payload: byte)
    (status: byte)
    (effectiveAt: DateTimeOffset)
    (fromState: ContractState)
    : TransitionDraft<ContractEntityId, ContractState, ContractEvent, ContractAction> =
    { MachineId = env.Machine
      EntityId = entity
      Event = event
      Actions = [ Fired(1 + int payload % 5) ]
      FromState = fromState
      ToState = Active(1 + int payload % 7)
      HandledBy = stateId "contract"
      Exited = [ stateId "from" ]
      Entered = [ stateId "to" ]
      Status = historyStatus status
      EffectiveAt = effectiveAt }

/// One compiled append: everything the model predicts the store wrote, except the commit instant
/// the database stamps, which is bounded rather than predicted.
type private CompiledRow =
    { Draft: TransitionDraft<ContractEntityId, ContractState, ContractEvent, ContractAction>
      Epoch: Epoch
      CommandId: CommandId
      Before: DateTimeOffset
      After: DateTimeOffset }

let private expectedSnapshot (log: CompiledRow list) : Snapshot<ContractState> option =
    match List.tryLast log with
    | None -> None
    | Some last ->
        Some
            { State = last.Draft.ToState
              Epoch = last.Epoch
              Status = last.Draft.Status }

let private expectedPage (page: Page) (log: CompiledRow list) : CompiledRow list =
    let after =
        match Page.cursor page with
        | None -> log
        | Some cursor -> log |> List.filter (fun row -> row.Epoch > cursor)

    after |> List.truncate (Page.limit page)

let private rowsMatch (env: ContractHistoryEnv) (rows: ContractTransition list) (expected: CompiledRow list) =
    rows.Length = expected.Length
    && List.forall2
        (fun (row: ContractTransition) (exp: CompiledRow) ->
            row.Draft = exp.Draft
            && row.Epoch = exp.Epoch
            && row.CommandId = exp.CommandId
            && row.ChartVersion = env.Version
            && row.CommittedAt >= exp.Before - env.CommittedAtTolerance
            && row.CommittedAt <= exp.After + env.CommittedAtTolerance)
        rows
        expected

/// Commits the generated appends against the store while compiling the model, and then checks
/// every observation the contract promises. Answers the verdict and never throws on a mismatch.
let private runHistory (env: ContractHistoryEnv) (stores: ContractHistoryStores) (case: HistoryCase) : Task<bool> =
    task {
        let ct = CancellationToken.None
        let appends = List.truncate 8 case.Appends
        let lease = LeaseDuration.create (TimeSpan.FromSeconds 30.0)
        let mutable logs: Map<string, CompiledRow list> = Map.empty
        let mutable states: Map<string, ContractState> = Map.empty
        let mutable epochs: Map<string, Epoch> = Map.empty

        let observePage
            (machine: MachineId)
            (entity: ContractEntityId)
            (page: Page)
            (expected: CompiledRow list)
            : Task<bool> =
            task {
                match! stores.Reader.History(machine, entity, page, ct) with
                | Ok rows -> return rowsMatch env rows expected
                | Error _ -> return false
            }

        let observeSnapshot
            (machine: MachineId)
            (entity: ContractEntityId)
            (expected: Snapshot<ContractState> option)
            : Task<bool> =
            task {
                match! stores.Reader.TryGetSnapshot(machine, entity, ct) with
                | Ok actual -> return actual = expected
                | Error _ -> return false
            }

        let commitAppend (index: int) (append: AppendSeed) : Task<bool> =
            task {
                let name = entityName append.Entity
                let entity: ContractEntityId = entityId name
                let event = eventOf append.Payload
                let key = $"h{index}"
                let fromState = Map.tryFind name states |> Option.defaultValue Idle
                let expected = Map.tryFind name epochs |> Option.defaultValue Epoch.initial

                let draft =
                    historyDraft
                        env
                        entity
                        event
                        append.Payload
                        append.Status
                        (historyEffectiveAt index append.EffectiveAt)
                        fromState

                let submission: CommandSubmission<ContractEntityId, ContractEvent> =
                    { MachineId = env.Machine
                      EntityId = entity
                      IdempotencyKey = key
                      ChartVersion = env.Version
                      Kind = CommandKind.Event
                      Event = event
                      VisibleAt = None
                      ReceivedAt = None
                      Audit = AuditContext.empty }

                match! stores.Inbox.Submit(submission, ct) with
                | Error _ -> return false
                | Ok _ ->
                    let before = stores.Now()

                    match! stores.Inbox.Claim(env.Machine, ClaimBatch.create 1, lease, ct) with
                    | Error _ -> return false
                    | Ok [] -> return false
                    | Ok(held :: _) when held.Work.IdempotencyKey = key ->
                        match! stores.Processor.Commit(held.Work.CommandId, held.Token, expected, draft, ct) with
                        | Ok(Finalized committed) when committed = Epoch.next expected ->
                            let after = stores.Now()

                            let row: CompiledRow =
                                { Draft = draft
                                  Epoch = committed
                                  CommandId = held.Work.CommandId
                                  Before = before
                                  After = after }

                            logs <- Map.add name ((Map.tryFind name logs |> Option.defaultValue []) @ [ row ]) logs
                            states <- Map.add name draft.ToState states
                            epochs <- Map.add name committed epochs
                            return true
                        | _ -> return false
                    | Ok _ -> return false
            }

        let rec appendLoop (nextIndex: int) (remaining: AppendSeed list) : Task<bool> =
            task {
                match remaining with
                | [] -> return true
                | append :: rest ->
                    let! ok = commitAppend nextIndex append

                    if ok then
                        return! appendLoop (nextIndex + 1) rest
                    else
                        return false
            }

        let checkEntity (name: string) (log: CompiledRow list) : Task<bool> =
            task {
                let entity: ContractEntityId = entityId name
                let! snapOk = observeSnapshot env.Machine entity (expectedSnapshot log)
                let epochValues = log |> List.map (_.Epoch >> Epoch.value)
                let epochsOk = epochValues = [ 1UL .. uint64 (List.length log) ]
                let! fullOk = observePage env.Machine entity (Page.create 10) log
                let! oneOk = observePage env.Machine entity (Page.create 1) (List.truncate 1 log)

                let! cursorOk =
                    match List.tryHead log with
                    | Some first ->
                        let page = Page.after first.Epoch 10
                        observePage env.Machine entity page (expectedPage page log)
                    | None -> Task.FromResult true

                return snapOk && epochsOk && fullOk && oneOk && cursorOk
            }

        let rec checkEntities (entities: (string * CompiledRow list) list) : Task<bool> =
            task {
                match entities with
                | [] -> return true
                | (name, log) :: rest ->
                    let! ok = checkEntity name log

                    if ok then return! checkEntities rest else return false
            }

        let beforePages = ResizeArray<string * Page * ContractTransition list>()

        let rec checkPrefixPages (entities: (string * CompiledRow list) list) : Task<bool> =
            task {
                match entities with
                | [] -> return true
                | (name, log) :: rest ->
                    let page = Page.create 3
                    let expected = expectedPage page log

                    match! stores.Reader.History(env.Machine, entityId name, page, ct) with
                    | Ok rows ->
                        if rowsMatch env rows expected then
                            beforePages.Add(name, page, rows)
                            return! checkPrefixPages rest
                        else
                            return false
                    | Error _ -> return false
            }

        let rec checkStability (pinned: (string * Page * ContractTransition list) list) : Task<bool> =
            task {
                match pinned with
                | [] -> return true
                | (name, page, beforeRows) :: rest ->
                    match! stores.Reader.History(env.Machine, entityId name, page, ct) with
                    | Ok afterRows ->
                        let stable =
                            if List.length beforeRows >= Page.limit page then
                                afterRows = beforeRows
                            else
                                List.truncate (List.length beforeRows) afterRows = beforeRows

                        if stable then return! checkStability rest else return false
                    | Error _ -> return false
            }

        let rec checkPageSeeds (seeds: PageSeed list) : Task<bool> =
            task {
                match seeds with
                | [] -> return true
                | seed :: rest ->
                    let name = entityName seed.Entity
                    let log = Map.tryFind name logs |> Option.defaultValue []
                    let limit = 1 + int seed.Limit % 6

                    let page =
                        match int seed.Cursor % 3 with
                        | 0 -> Page.create limit
                        | 1 ->
                            match log with
                            | [] -> Page.create limit
                            | _ -> Page.after log[int seed.Cursor % List.length log].Epoch limit
                        | _ ->
                            match List.tryLast log with
                            | Some row -> Page.after row.Epoch limit
                            | None -> Page.create limit

                    let expected = expectedPage page log

                    match! stores.Reader.History(env.Machine, entityId name, page, ct) with
                    | Ok rows when rowsMatch env rows expected -> return! checkPageSeeds rest
                    | _ -> return false
            }

        // A never-committed entity, and a machine that never wrote anything, answer nothing.
        let! missingSnap = observeSnapshot env.Machine (entityId "missing") None
        let! missingPage = observePage env.Machine (entityId "missing") (Page.create 10) []
        let! otherSnap = observeSnapshot (machineId "other-machine") (entityId "e0") None
        let! otherPage = observePage (machineId "other-machine") (entityId "e0") (Page.create 10) []

        let emptyOk = missingSnap && missingPage && otherSnap && otherPage

        // Commit the prefix, page it, then commit the suffix and prove the page is stable.
        let split = int case.GrowthSplit % (List.length appends + 1)
        let prefixAppends = List.take split appends
        let suffixAppends = List.skip split appends

        let! prefixOk = appendLoop 0 prefixAppends
        let! prefixPagesOk = checkPrefixPages (Map.toList logs)
        let! suffixOk = appendLoop split suffixAppends
        let! stableOk = checkStability (List.ofSeq beforePages)
        let! pageSeedsOk = checkPageSeeds (List.truncate 3 case.Pages)
        let! entitiesOk = checkEntities (Map.toList logs)

        return
            emptyOk
            && prefixOk
            && prefixPagesOk
            && suffixOk
            && stableOk
            && pageSeedsOk
            && entitiesOk
    }

/// Renders a shrunk counterexample.
let private renderHistory (case: HistoryCase) : string =
    let appends =
        case.Appends
        |> List.truncate 8
        |> List.map (fun a -> $"({a.Entity},{a.Payload},{a.Status},{a.EffectiveAt})")

    let pages =
        case.Pages
        |> List.truncate 3
        |> List.map (fun p -> $"({p.Entity},{p.Cursor},{p.Limit})")

    let appendsText = String.concat " " appends
    let pagesText = String.concat " " pages

    $"appends=[{appendsText}] split={case.GrowthSplit} pages=[{pagesText}]"

/// The shared history property. The law is a task, so every generated case, and every rerun a
/// shrunk counterexample produces, takes a fresh fixture and runs against it for real.
let historyProperty name maxTests (env: ContractHistoryEnv) =
    testCase name (fun () ->
        let config = Config.QuickThrowOnFailure.WithMaxTest(maxTests).WithEndSize(12)

        Check.One(
            config,
            fun (case: HistoryCase) ->
                task {
                    let! stores = env.Fresh()
                    let! verdict = runHistory env stores case

                    if not verdict then
                        eprintfn "history case falsified:\n%s" (renderHistory case)

                    return verdict
                }
        ))

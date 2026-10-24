namespace ByzantineSystems.Automata.Storage.Internal

open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>Provider-specific evidence for one fenced lease update.</summary>
type internal LeaseUpdateInstruction<'Params, 'Reader> =
    | ChangedRows of key: StatementKey * parameters: 'Params
    | ReturnedOutcome of key: StatementKey * parameters: 'Params * read: Read<'Reader, LeaseUpdateOutcome>

/// <summary>Provider-specific evidence and writes for one idempotent submission.</summary>
type internal SubmitInstruction<'Params, 'Reader> =
    | AtomicSubmission of StoreProgram<'Params, 'Reader, SubmissionOutcome>
    | CheckedSubmission of
        existing: StoreProgram<'Params, 'Reader, CommandId option> *
        registered: StoreProgram<'Params, 'Reader, bool> *
        unregistered: StoreError *
        insert: StoreProgram<'Params, 'Reader, CommandId> *
        correction: (CommandId -> StoreProgram<'Params, 'Reader, unit>) option

[<RequireQualifiedAccess>]
module internal CommandInboxProgram =

    let submit (instruction: SubmitInstruction<'Params, 'Reader>) : StoreProgram<'Params, 'Reader, SubmissionOutcome> =
        store {
            match instruction with
            | AtomicSubmission program -> return! program
            | CheckedSubmission(existing, registered, unregistered, insert, correction) ->
                match! existing with
                | Some commandId -> return AlreadySubmitted commandId
                | None ->
                    match! registered with
                    | false -> return! StoreProgram.fail unregistered
                    | true ->
                        let! commandId = insert

                        match correction with
                        | Some write -> do! write commandId
                        | None -> ()

                        return Accepted commandId
        }

    let leaseUpdate
        (instruction: LeaseUpdateInstruction<'Params, 'Reader>)
        : StoreProgram<'Params, 'Reader, LeaseUpdateOutcome> =
        store {
            match instruction with
            | ChangedRows(key, parameters) ->
                let! changed = StoreProgram.executeCount key parameters

                return
                    if changed = 1 then
                        LeaseUpdateOutcome.Updated
                    else
                        LeaseUpdateOutcome.LeaseLost
            | ReturnedOutcome(key, parameters, read) -> return! StoreProgram.queryOne key parameters read
        }

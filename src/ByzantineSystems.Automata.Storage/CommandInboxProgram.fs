namespace ByzantineSystems.Automata.Storage.Internal

open ByzantineSystems.Automata.Storage

/// <summary>Provider-specific evidence for one fenced lease update.</summary>
type internal LeaseUpdateInstruction<'Params, 'Reader> =
    | ChangedRows of key: StatementKey * parameters: 'Params
    | ReturnedOutcome of key: StatementKey * parameters: 'Params * read: Read<'Reader, LeaseUpdateOutcome>

[<RequireQualifiedAccess>]
module internal CommandInboxProgram =

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

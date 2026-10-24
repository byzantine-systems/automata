namespace ByzantineSystems.Automata.Storage.Internal

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>A stable semantic name resolved to provider SQL by an interpreter.</summary>
[<Struct>]
type internal StatementKey = private StatementKey of string

[<RequireQualifiedAccess>]
module internal StatementKey =

    let create (domain: string) (operation: string) : StatementKey =
        if System.String.IsNullOrWhiteSpace domain then
            invalidArg (nameof domain) "A statement domain must be non-blank."

        if System.String.IsNullOrWhiteSpace operation then
            invalidArg (nameof operation) "A statement operation must be non-blank."

        StatementKey($"%s{domain.Trim()}/%s{operation.Trim()}")

    let value (StatementKey value) = value

    let parts (StatementKey value) =
        match value.Split('/', 2) with
        | [| domain; operation |] -> domain, operation
        | _ -> invalidOp $"Invalid statement key '%s{value}'."

/// <summary>Decodes one provider row without exposing its representation to a program.</summary>
type internal Read<'Reader, 'T> = 'Reader -> Result<'T, StoreError>

/// <summary>The statement algebra implemented once per provider session.</summary>
type internal IStoreInterpreter<'Params, 'Reader> =

    abstract ExecuteAsync: StatementKey * 'Params * CancellationToken -> Task<Result<int, StoreError>>

    abstract QueryOneAsync<'T> :
        StatementKey * 'Params * Read<'Reader, 'T> * CancellationToken -> Task<Result<'T, StoreError>>

    abstract TryQueryOneAsync<'T> :
        StatementKey * 'Params * Read<'Reader, 'T> * CancellationToken -> Task<Result<'T option, StoreError>>

    abstract QueryRowsAsync<'T> :
        StatementKey * 'Params * Read<'Reader, 'T> * CancellationToken -> Task<Result<'T list, StoreError>>

/// <summary>A cold, provider-generic sequence of store instructions.</summary>
type internal StoreProgram<'Params, 'Reader, 'T> =
    private | StoreProgram of (IStoreInterpreter<'Params, 'Reader> -> CancellationToken -> Task<Result<'T, StoreError>>)

[<RequireQualifiedAccess>]
module internal StoreProgram =

    let run
        (interpreter: IStoreInterpreter<'Params, 'Reader>)
        (cancellationToken: CancellationToken)
        (StoreProgram program: StoreProgram<'Params, 'Reader, 'T>)
        : Task<Result<'T, StoreError>> =
        program interpreter cancellationToken

    let ok (value: 'T) : StoreProgram<'Params, 'Reader, 'T> =
        StoreProgram(fun _ _ -> Task.FromResult(Ok value))

    let fail (error: StoreError) : StoreProgram<'Params, 'Reader, 'T> =
        StoreProgram(fun _ _ -> Task.FromResult(Error error))

    let ofResult (result: Result<'T, StoreError>) : StoreProgram<'Params, 'Reader, 'T> =
        StoreProgram(fun _ _ -> Task.FromResult result)

    let bind
        (next: 'T -> StoreProgram<'Params, 'Reader, 'U>)
        (StoreProgram program: StoreProgram<'Params, 'Reader, 'T>)
        : StoreProgram<'Params, 'Reader, 'U> =
        StoreProgram(fun interpreter cancellationToken ->
            backgroundTask {
                match! program interpreter cancellationToken with
                | Ok value -> return! run interpreter cancellationToken (next value)
                | Error error -> return Error error
            })

    let map (mapping: 'T -> 'U) (program: StoreProgram<'Params, 'Reader, 'T>) : StoreProgram<'Params, 'Reader, 'U> =
        bind (mapping >> ok) program

    let executeCount (key: StatementKey) (parameters: 'Params) : StoreProgram<'Params, 'Reader, int> =
        StoreProgram(fun interpreter cancellationToken -> interpreter.ExecuteAsync(key, parameters, cancellationToken))

    let executeDiscard (key: StatementKey) (parameters: 'Params) : StoreProgram<'Params, 'Reader, unit> =
        executeCount key parameters |> map ignore

    let queryOne
        (key: StatementKey)
        (parameters: 'Params)
        (read: Read<'Reader, 'T>)
        : StoreProgram<'Params, 'Reader, 'T> =
        StoreProgram(fun interpreter cancellationToken ->
            interpreter.QueryOneAsync(key, parameters, read, cancellationToken))

    let tryQueryOne
        (key: StatementKey)
        (parameters: 'Params)
        (read: Read<'Reader, 'T>)
        : StoreProgram<'Params, 'Reader, 'T option> =
        StoreProgram(fun interpreter cancellationToken ->
            interpreter.TryQueryOneAsync(key, parameters, read, cancellationToken))

    let queryRows
        (key: StatementKey)
        (parameters: 'Params)
        (read: Read<'Reader, 'T>)
        : StoreProgram<'Params, 'Reader, 'T list> =
        StoreProgram(fun interpreter cancellationToken ->
            interpreter.QueryRowsAsync(key, parameters, read, cancellationToken))

/// <summary>Computation expression for one sequential store program.</summary>
type internal StoreProgramBuilder() =

    member _.Return(value: 'T) : StoreProgram<'Params, 'Reader, 'T> = StoreProgram.ok value

    member _.ReturnFrom(program: StoreProgram<'Params, 'Reader, 'T>) = program

    member _.Zero() : StoreProgram<'Params, 'Reader, unit> = StoreProgram.ok ()

    member _.Bind
        (program: StoreProgram<'Params, 'Reader, 'T>, next: 'T -> StoreProgram<'Params, 'Reader, 'U>)
        : StoreProgram<'Params, 'Reader, 'U> =
        StoreProgram.bind next program

    member _.Bind
        (result: Result<'T, StoreError>, next: 'T -> StoreProgram<'Params, 'Reader, 'U>)
        : StoreProgram<'Params, 'Reader, 'U> =
        result |> StoreProgram.ofResult |> StoreProgram.bind next

    member _.Delay(body: unit -> StoreProgram<'Params, 'Reader, 'T>) =
        StoreProgram(fun interpreter cancellationToken -> StoreProgram.run interpreter cancellationToken (body ()))

    member _.Combine
        (first: StoreProgram<'Params, 'Reader, unit>, next: StoreProgram<'Params, 'Reader, 'T>)
        : StoreProgram<'Params, 'Reader, 'T> =
        first |> StoreProgram.bind (fun () -> next)

[<AutoOpen>]
module internal StoreProgramBuilders =

    let store = StoreProgramBuilder()

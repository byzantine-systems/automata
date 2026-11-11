namespace ByzantineSystems.Automata.Storage.Internal

open System
open ByzantineSystems.Automata.Core
open ByzantineSystems.Automata.Storage

/// <summary>
/// One database enumeration coded by a single table: each case of a union beside the text both
/// schemas store for it, with encode and decode derived from that one table. The case function
/// given to <see cref="M:ByzantineSystems.Automata.Storage.Internal.EnumCodec.create``1" /> is an
/// exhaustive match, so a case added to the union is a compile error there rather than a decode
/// failure on a live row.
/// </summary>
type internal EnumCodec<'T when 'T: equality> =
    private
        { Name: string
          Cases: ('T * string) list }

[<RequireQualifiedAccess>]
module internal EnumCodec =

    /// <summary>
    /// Builds an enum codec. Refuses a blank name, an empty table, a blank case text, and any
    /// value or text listed twice: each is a defect in the table itself, and a table that is not
    /// a bijection would make one direction lie.
    /// </summary>
    let create (name: string) (all: 'T list) (encode: 'T -> string) : EnumCodec<'T> =
        if String.IsNullOrWhiteSpace name then
            invalidArg (nameof name) "An enum codec name must be non-blank."

        let cases = all |> List.map (fun value -> value, encode value)

        if List.isEmpty cases then
            invalidArg (nameof all) "An enum codec table must not be empty."

        if cases |> List.exists (snd >> String.IsNullOrWhiteSpace) then
            invalidArg (nameof encode) "An enum codec table must not code a case as blank text."

        if cases |> List.distinctBy fst |> List.length <> List.length cases then
            invalidArg (nameof all) "An enum codec table must list each case once."

        if cases |> List.distinctBy snd |> List.length <> List.length cases then
            invalidArg (nameof encode) "An enum codec table must give each case its own text."

        { Name = name; Cases = cases }

    /// <summary>Every case the codec covers, in declaration order.</summary>
    let values (codec: EnumCodec<'T>) : 'T list = codec.Cases |> List.map fst

    /// <summary>
    /// The text both schemas store for a case. A case missing from the table cannot happen once
    /// the completeness test has passed, so reaching the failure is a defect in the table.
    /// </summary>
    /// <exception cref="T:System.InvalidOperationException">
    /// Raised when the value is not one the table codes.
    /// </exception>
    let encode (codec: EnumCodec<'T>) (value: 'T) : string =
        match codec.Cases |> List.tryFind (fun (case, _) -> case = value) with
        | Some(_, text) -> text
        | None -> invalidOp $"The %s{codec.Name} codec does not encode one of its own cases."

    /// <summary>
    /// The case a stored text names. An unknown text is corrupt data in a row, not a defect in
    /// the table, so it answers <c>Error</c> as a serialization failure naming the codec.
    /// </summary>
    let decode (codec: EnumCodec<'T>) (text: string) : Result<'T, StoreError> =
        match codec.Cases |> List.tryFind (fun (_, coded) -> coded = text) with
        | Some(value, _) -> Ok value
        | None -> Error(StoreError.Serialization(codec.Name, FormatException $"unknown %s{codec.Name} %s{text}"))

    /// <summary>
    /// A row reader for one column, over the provider's own column accessor, so a case decode is
    /// part of the read rather than a follow-up step.
    /// </summary>
    let read (text: 'Reader -> string -> string) (codec: EnumCodec<'T>) (column: string) : Read<'Reader, 'T> =
        fun reader -> text reader column |> decode codec

    /// <summary>The command lifecycle, as <c>status</c> codes it in both schemas.</summary>
    let commandStatus: EnumCodec<CommandStatus> =
        create
            "command status"
            [ CommandStatus.Ready
              CommandStatus.Leased
              CommandStatus.Succeeded
              CommandStatus.Rejected
              CommandStatus.DeadLettered ]
            (function
             | CommandStatus.Ready -> "ready"
             | CommandStatus.Leased -> "leased"
             | CommandStatus.Succeeded -> "succeeded"
             | CommandStatus.Rejected -> "rejected"
             | CommandStatus.DeadLettered -> "dead_letter")

    /// <summary>What a fenced write did, as the lease-update statements code it.</summary>
    let leaseOutcome: EnumCodec<LeaseUpdateOutcome> =
        create "lease outcome" [ LeaseUpdateOutcome.Updated; LeaseUpdateOutcome.LeaseLost ] (function
            | LeaseUpdateOutcome.Updated -> "updated"
            | LeaseUpdateOutcome.LeaseLost -> "lease_lost")

    /// <summary>What a correction does on divergence, as <c>on_divergence</c> codes it.</summary>
    let divergence: EnumCodec<Divergence> =
        create "divergence policy" [ Divergence.Fail; Divergence.Truncate ] (function
            | Divergence.Fail -> "fail"
            | Divergence.Truncate -> "truncate")

    /// <summary>The instance lifecycle, as <c>instance_status</c> codes it in both schemas.</summary>
    let instanceStatus: EnumCodec<InstanceStatus> =
        create
            "instance status"
            [ InstanceStatus.Running; InstanceStatus.Suspended; InstanceStatus.Terminated ]
            (function
             | InstanceStatus.Running -> "running"
             | InstanceStatus.Suspended -> "suspended"
             | InstanceStatus.Terminated -> "terminated")

    /// <summary>What happened to a supervised child, as the audit log's <c>kind</c> codes it.</summary>
    let supervisionKind: EnumCodec<SupervisionAuditKind> =
        create
            "supervision audit kind"
            [ SupervisionAuditKind.Started
              SupervisionAuditKind.Restarted
              SupervisionAuditKind.Escalated
              SupervisionAuditKind.Stopped ]
            (function
             | SupervisionAuditKind.Started -> "Started"
             | SupervisionAuditKind.Restarted -> "Restarted"
             | SupervisionAuditKind.Escalated -> "Escalated"
             | SupervisionAuditKind.Stopped -> "Stopped")

    /// <summary>Which children a restart affects, as the audit log's <c>strategy</c> codes it.</summary>
    let supervisionStrategy: EnumCodec<SupervisionAuditStrategy> =
        create
            "supervision audit strategy"
            [ SupervisionAuditStrategy.OneForOne
              SupervisionAuditStrategy.OneForAll
              SupervisionAuditStrategy.RestForOne ]
            (function
             | SupervisionAuditStrategy.OneForOne -> "OneForOne"
             | SupervisionAuditStrategy.OneForAll -> "OneForAll"
             | SupervisionAuditStrategy.RestForOne -> "RestForOne")

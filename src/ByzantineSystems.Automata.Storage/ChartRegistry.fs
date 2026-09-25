namespace ByzantineSystems.Automata.Storage

open System.Threading
open System.Threading.Tasks
open ByzantineSystems.Automata.Core

/// <summary>
/// What a machine claims to be running: a declared version, and the structural fingerprint of the
/// chart the developer declared it for.
///
/// The version is declared and never inferred. Inferring it would make an accidental edit
/// indistinguishable from a deliberate one, which is the whole failure this is here to catch.
/// </summary>
type ChartIdentity =
    { MachineId: MachineId
      Version: ChartVersion
      Fingerprint: ChartFingerprint }

/// <summary>What registering an identity found. Only <c>Mismatched</c> is a problem.</summary>
[<RequireQualifiedAccess>]
type ChartRegistration =
    /// <summary>Nothing had claimed this version before, so it now belongs to this fingerprint.</summary>
    | Registered

    /// <summary>This version was already claimed, by a chart with the same structure.</summary>
    | Matched

    /// <summary>
    /// This version was already claimed by a chart with a different structure. Either the chart
    /// was edited without bumping its version, or two machines share an id. Reported as data
    /// rather than raised: the caller decides whether that is fatal, and a boot check deciding it
    /// is fatal is policy that belongs to the host.
    /// </summary>
    | Mismatched of stored: ChartFingerprint

/// <summary>
/// The durable record of which chart structure each declared version belongs to.
///
/// One entry per <c>(machine, version)</c>, written the first time that version is seen and never
/// overwritten. A later registration compares against it, which is what turns "somebody edited a
/// chart and forgot to bump it" from a silent replay bug into something a process can refuse to
/// start on.
/// </summary>
type IChartRegistry =

    /// <summary>
    /// Claims a version for a fingerprint, or reports what the stored one already says.
    /// Idempotent, and safe to call concurrently from every process that runs the machine.
    /// </summary>
    abstract Register: identity: ChartIdentity * ct: CancellationToken -> Task<Result<ChartRegistration, StoreError>>

    /// <summary>Reads the fingerprint a version is registered under, if any.</summary>
    abstract TryGet:
        machineId: MachineId * version: ChartVersion * ct: CancellationToken ->
            Task<Result<ChartFingerprint option, StoreError>>

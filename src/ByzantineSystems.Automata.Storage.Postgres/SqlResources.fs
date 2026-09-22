namespace ByzantineSystems.Automata.Storage.Postgres

open System.IO
open System.Reflection

/// <summary>
/// Every statement this assembly sends, loaded once from the embedded <c>sql/</c> tree.
///
/// SQL living in files rather than in F# string literals is what makes it reviewable: pg_format
/// reaches it through <c>nix fmt</c>, and a WHERE clause that must not drift from an index
/// predicate can carry the paragraph that says so. A query keyed by domain and operation also
/// stops the same statement from being written twice in two shapes.
/// </summary>
[<RequireQualifiedAccess>]
module internal SqlResources =

    let private assembly = Assembly.GetExecutingAssembly()

    /// <summary>
    /// Turns a manifest resource name into its key.
    ///
    /// MSBuild names an embedded resource after its path, with separators replaced by dots, so
    /// <c>sql/command/claim.sql</c> arrives as <c>&lt;root&gt;.sql.command.claim.sql</c>. Reading
    /// the last four segments backwards gives the extension, the operation, the domain and the
    /// tree root, and requiring that root to be <c>sql</c> is what keeps the migration scripts,
    /// which are embedded from a sibling tree, out of the map.
    /// </summary>
    let private keyOf (resourceName: string) : (string * string) option =
        let parts = resourceName.Split '.'

        if
            parts.Length >= 4
            && parts[parts.Length - 1] = "sql"
            && parts[parts.Length - 4] = "sql"
        then
            Some(parts[parts.Length - 3], parts[parts.Length - 2])
        else
            None

    let private read (resourceName: string) : string =
        use stream = assembly.GetManifestResourceStream resourceName
        use reader = new StreamReader(stream)
        reader.ReadToEnd()

    let private statements: Map<string * string, string> =
        assembly.GetManifestResourceNames()
        |> Array.choose (fun name -> keyOf name |> Option.map (fun key -> key, read name))
        |> Map.ofArray

    /// <summary>The keys this assembly ships, for diagnostics.</summary>
    let keys: (string * string) list = statements |> Map.toList |> List.map fst

    /// <summary>Looks up a statement, or <c>None</c> when nothing is embedded under that key.</summary>
    let tryGet (domain: string) (operation: string) : string option =
        Map.tryFind (domain, operation) statements

    /// <summary>
    /// Looks up a statement that must exist. A miss is a packaging fault rather than a runtime
    /// condition, so it raises, and it names both the file that was expected and every key that
    /// is present, because the failure is almost always a typo or a missing
    /// <c>EmbeddedResource</c> glob.
    /// </summary>
    let get (domain: string) (operation: string) : string =
        match tryGet domain operation with
        | Some statement -> statement
        | None ->
            let known = keys |> List.map (fun (d, o) -> $"%s{d}/%s{o}") |> String.concat ", "

            failwith
                $"No SQL resource for %s{domain}/%s{operation}. Expected an embedded file at sql/%s{domain}/%s{operation}.sql. Embedded keys: %s{known}."

namespace ByzantineSystems.Automata.Storage.Postgres

open System
open System.Text

/// <summary>A reason a PostgreSQL schema name cannot be used by the Automata store.</summary>
type SchemaNameError =
    /// <summary>The name was null or empty.</summary>
    | Empty
    /// <summary>The UTF-8 name was longer than PostgreSQL's safe derived-identifier limit.</summary>
    | TooLong of length: int
    /// <summary>The name was not a lower-case, unquoted PostgreSQL identifier.</summary>
    | InvalidCharacters
    /// <summary>The name belongs to PostgreSQL or to another protected database component.</summary>
    | Reserved of name: string

/// <summary>
/// A validated lower-case PostgreSQL schema identifier safe for unquoted SQL template substitution.
/// </summary>
[<Struct>]
type SchemaName = private SchemaName of string

/// <summary>Validation and access operations for <see cref="T:ByzantineSystems.Automata.Storage.Postgres.SchemaName" />.</summary>
[<RequireQualifiedAccess>]
module SchemaName =

    let internal reservedKeywords =
        Set.ofList
            [ "all"
              "analyse"
              "analyze"
              "and"
              "any"
              "array"
              "as"
              "asc"
              "asymmetric"
              "both"
              "case"
              "cast"
              "check"
              "collate"
              "column"
              "constraint"
              "create"
              "current_catalog"
              "current_date"
              "current_role"
              "current_time"
              "current_timestamp"
              "current_user"
              "default"
              "deferrable"
              "desc"
              "distinct"
              "do"
              "else"
              "end"
              "except"
              "false"
              "fetch"
              "for"
              "foreign"
              "from"
              "grant"
              "group"
              "having"
              "in"
              "initially"
              "intersect"
              "into"
              "lateral"
              "leading"
              "limit"
              "localtime"
              "localtimestamp"
              "not"
              "null"
              "offset"
              "on"
              "only"
              "or"
              "order"
              "placing"
              "primary"
              "references"
              "returning"
              "select"
              "session_user"
              "some"
              "symmetric"
              "system_user"
              "table"
              "then"
              "to"
              "trailing"
              "true"
              "union"
              "unique"
              "user"
              "using"
              "variadic"
              "when"
              "where"
              "window"
              "with" ]

    let private protectedNames =
        Set.ofList [ "public"; "information_schema"; "pgmq"; "cron" ]

    let private isFirst character =
        character = '_' || (character >= 'a' && character <= 'z')

    let private isRest character =
        isFirst character || (character >= '0' && character <= '9')

    /// <summary>The default Automata schema, <c>fsm</c>.</summary>
    let defaults = SchemaName "fsm"

    /// <summary>
    /// Validates a lower-case ASCII schema name, including PostgreSQL's reserved and protected names.
    /// </summary>
    let tryCreate (value: string) : Result<SchemaName, SchemaNameError> =
        if String.IsNullOrEmpty value then
            Error SchemaNameError.Empty
        else
            let length = Encoding.UTF8.GetByteCount value

            if length > 49 then
                Error(SchemaNameError.TooLong length)
            elif not (isFirst value[0] && Seq.forall isRest value) then
                Error SchemaNameError.InvalidCharacters
            elif
                value.StartsWith("pg_", StringComparison.Ordinal)
                || Set.contains value protectedNames
                || Set.contains value reservedKeywords
            then
                Error(SchemaNameError.Reserved value)
            else
                Ok(SchemaName value)

    /// <summary>Returns the validated identifier text.</summary>
    let value (SchemaName value) = value

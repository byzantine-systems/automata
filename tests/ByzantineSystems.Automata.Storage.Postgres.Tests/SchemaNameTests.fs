module ByzantineSystems.Automata.Storage.Postgres.Tests.SchemaNameTests

open System
open System.Text
open System.Text.RegularExpressions
open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open FsCheck

let private check name property =
    testCase name (fun () -> Check.One(Config.QuickThrowOnFailure, property))

let tests =
    testList
        "SchemaName"
        [ test "the default is fsm" {
              Expect.equal (SchemaName.value SchemaName.defaults) "fsm" "the public default remains stable"
          }

          check "every accepted value obeys the injection-boundary rules" (fun (candidate: string) ->
              match SchemaName.tryCreate candidate with
              | Error _ -> true
              | Ok schema ->
                  not (isNull candidate)
                  && Regex.IsMatch(candidate, "^[a-z_][a-z0-9_]*$", RegexOptions.CultureInvariant)
                  && Encoding.UTF8.GetByteCount candidate <= 49
                  && not (candidate.StartsWith("pg_", StringComparison.Ordinal))
                  && not (Set.contains candidate SchemaName.reservedKeywords)
                  && SchemaName.value schema = candidate)

          check "generated valid identifiers round trip" (fun (seed: uint) ->
              let candidate = "app_" + String('a', int (seed % 46u))

              match SchemaName.tryCreate candidate with
              | Ok schema -> SchemaName.value schema = candidate
              | Error _ -> false)

          test "null and empty names are empty" {
              Expect.equal (SchemaName.tryCreate null) (Error SchemaNameError.Empty) "null"
              Expect.equal (SchemaName.tryCreate "") (Error SchemaNameError.Empty) "empty"
          }

          test "49 bytes fit and 50 do not" {
              let fits = String('a', 49)
              let truncated = String('a', 50)

              Expect.isOk (SchemaName.tryCreate fits) "the longest safe derived identifier fits"

              Expect.equal
                  (SchemaName.tryCreate truncated)
                  (Error(SchemaNameError.TooLong 50))
                  "PostgreSQL must never truncate a derived identifier"
          }

          test "invalid identifier characters are rejected" {
              for candidate in [ "Fsm"; "0fsm"; "has-dash"; "has space"; "has.dot" ] do
                  Expect.equal
                      (SchemaName.tryCreate candidate)
                      (Error SchemaNameError.InvalidCharacters)
                      $"{candidate} is not a lower-case unquoted identifier"
          }

          test "protected schemas and the pg prefix are reserved" {
              for candidate in [ "public"; "information_schema"; "pgmq"; "cron"; "pg_catalog"; "pg_private" ] do
                  Expect.equal
                      (SchemaName.tryCreate candidate)
                      (Error(SchemaNameError.Reserved candidate))
                      $"{candidate} belongs to PostgreSQL or another database component"
          }

          test "every shipped PostgreSQL reserved keyword is rejected" {
              for keyword in SchemaName.reservedKeywords do
                  Expect.equal
                      (SchemaName.tryCreate keyword)
                      (Error(SchemaNameError.Reserved keyword))
                      $"{keyword} is reserved"
          } ]

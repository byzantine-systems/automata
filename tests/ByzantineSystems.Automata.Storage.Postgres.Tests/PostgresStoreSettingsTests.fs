module ByzantineSystems.Automata.Storage.Postgres.Tests.PostgresStoreSettingsTests

open ByzantineSystems.Automata.Storage.Postgres
open Expecto
open Microsoft.Extensions.Options

/// The settings resolve or refuse without a database, which is the property startup
/// validation depends on: no connection is opened to learn the configuration is wrong.
let tests =
    testList
        "PostgresStoreSettings"
        [ test "belief models have stable configuration round trips" {
              for model in [ BeliefModel.Snapshot; BeliefModel.Bitemporal ] do
                  Expect.equal (BeliefModel.tryCreate (BeliefModel.value model)) (Ok model) "round trip"

              let settings = PostgresStoreSettings()
              Expect.equal settings.Beliefs "snapshot" "the default changed deliberately"
              settings.Beliefs <- "unknown"
              Expect.isError (PostgresStoreSettings.tryResolve settings) "an unknown model is a configuration refusal"
          }

          test "the defaults resolve to the default schema and the shared listener" {
              match PostgresStoreSettings.tryResolve (PostgresStoreSettings()) with
              | Ok(schema, listener) ->
                  Expect.equal (SchemaName.value schema) "fsm" "the default schema"
                  Expect.equal listener ListenerMode.SameDataSource "the default listener"
              | Error failures -> failtestf "the defaults must resolve, got %A" failures
          }

          test "a chosen schema resolves" {
              let settings = PostgresStoreSettings(Schema = "automata_ci")

              match PostgresStoreSettings.tryResolve settings with
              | Ok(schema, ListenerMode.SameDataSource) ->
                  Expect.equal (SchemaName.value schema) "automata_ci" "the chosen schema"
              | other -> failtestf "a valid schema must resolve, got %A" other
          }

          test "an unusable schema is refused with a named failure" {
              let settings = PostgresStoreSettings(Schema = "Public")

              match PostgresStoreSettings.tryResolve settings with
              | Ok _ -> failtest "an uppercase schema must not resolve"
              | Error failures ->
                  Expect.isNonEmpty failures "the refusal says why"

                  Expect.isTrue
                      (failures |> List.exists (fun failure -> failure.Contains "Schema"))
                      "the failure names the schema setting"
          }

          test "a blank connection-string name is refused" {
              let settings = PostgresStoreSettings(ConnectionStringName = " ")

              match PostgresStoreSettings.tryResolve settings with
              | Ok _ -> failtest "a blank name must not resolve"
              | Error failures ->
                  Expect.isTrue
                      (failures |> List.exists (fun failure -> failure.Contains "ConnectionStringName"))
                      "the failure names the connection-string setting"
          }

          test "direct listening resolves by connection-string name" {
              let settings = PostgresStoreSettings()
              settings.Listener.Mode <- "direct"
              settings.Listener.ConnectionStringName <- "automata_listener"

              match PostgresStoreSettings.tryResolve settings with
              | Ok(_, ListenerMode.Direct name) ->
                  Expect.equal name "automata_listener" "the listener's own connection string"
              | other -> failtestf "direct mode with a name must resolve, got %A" other
          }

          test "direct listening without a name is refused" {
              let settings = PostgresStoreSettings()
              settings.Listener.Mode <- "direct"

              match PostgresStoreSettings.tryResolve settings with
              | Ok _ -> failtest "direct mode without a name must not resolve"
              | Error failures ->
                  Expect.isTrue
                      (failures
                       |> List.exists (fun failure -> failure.Contains "Listener.ConnectionStringName"))
                      "the failure names the listener setting"
          }

          test "an unknown listener mode is refused" {
              let settings = PostgresStoreSettings()
              settings.Listener.Mode <- "telepathy"

              match PostgresStoreSettings.tryResolve settings with
              | Ok _ -> failtest "an unknown mode must not resolve"
              | Error failures ->
                  Expect.isTrue
                      (failures |> List.exists (fun failure -> failure.Contains "same-data-source"))
                      "the failure lists the modes that exist"
          }

          test "off is a setting, not an absence" {
              let settings = PostgresStoreSettings()
              settings.Listener.Mode <- "off"

              match PostgresStoreSettings.tryResolve settings with
              | Ok(_, ListenerMode.Off) -> ()
              | other -> failtestf "off must resolve, got %A" other
          }

          test "the validator reports the settings failures, success intact" {
              let validator =
                  PostgresStoreSettingsValidator() :> IValidateOptions<PostgresStoreSettings>

              Expect.equal
                  (validator.Validate("any", PostgresStoreSettings()))
                  ValidateOptionsResult.Success
                  "the defaults pass"

              let refused = validator.Validate("any", PostgresStoreSettings(Schema = "select"))

              Expect.isFalse refused.Succeeded "a reserved schema fails"

              Expect.isTrue
                  (refused.Failures |> Seq.exists (fun failure -> failure.Contains "Schema"))
                  "the failure reaches the operator"
          } ]

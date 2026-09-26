{
  description = "F# Development Environment";

  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.zst";

    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
    };

    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs =
    inputs@{
      self,
      devenv,
      flake-parts,
      nixpkgs,
      ...
    }:
    let
      readXmlElement =
        element: file:
        let
          contents = builtins.readFile file;
          opening = "<${element}>";
          closing = "</${element}>";
          openingParts = nixpkgs.lib.splitString opening contents;
          closingParts = nixpkgs.lib.splitString closing contents;
        in
        if builtins.length openingParts != 2 || builtins.length closingParts != 2 then
          throw "readXmlElement: expected exactly one ${opening} and ${closing} in ${toString file}"
        else
          let
            valueParts = nixpkgs.lib.splitString closing (builtins.elemAt openingParts 1);
            valueMatch =
              if builtins.length valueParts == 2 then
                builtins.match "[[:space:]]*([^[:space:]<]+)[[:space:]]*" (builtins.elemAt valueParts 0)
              else
                null;
          in
          if valueMatch == null then
            throw "readXmlElement: ${opening} in ${toString file} must contain one non-empty value"
          else
            builtins.head valueMatch;
    in
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv.flakeModule
        inputs.treefmt-nix.flakeModule
      ];
      systems = nixpkgs.lib.systems.flakeExposed;

      perSystem =
        {
          config,
          self',
          inputs',
          pkgs,
          lib,
          system,
          ...
        }:
        let
          app_name = "bs-automata";
          db_connection_string = "Host=127.0.0.1;Port=5432;Database=${app_name};Username=${app_name};Password=${app_name}";
          net10 = pkgs.dotnet-sdk_10;
          version = readXmlElement "Version" ./Directory.Build.props;
          source = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              let
                name = baseNameOf path;
              in
              !(
                type == "directory"
                && builtins.elem name [
                  ".config"
                  ".tools"
                  "coverage"
                  "out"
                ]
              );
          };
          mkExample =
            {
              pname,
              projectFile,
              executable,
            }:
            pkgs.buildDotnetModule {
              inherit pname version projectFile;
              src = source;
              nugetDeps = ./deps.json;
              dotnet-sdk = net10;
              dotnet-runtime = pkgs.dotnet-aspnetcore_10;
              executables = [ executable ];
              doCheck = false;
            };
          paymentProcessor = mkExample {
            pname = "byzantine-systems-automata-payment-processor";
            projectFile = "examples/ByzantineSystems.Automata.Examples.PaymentProcessor/ByzantineSystems.Automata.Examples.PaymentProcessor.fsproj";
            executable = "ByzantineSystems.Automata.Examples.PaymentProcessor";
          };
          supervision = mkExample {
            pname = "byzantine-systems-automata-supervision";
            projectFile = "examples/ByzantineSystems.Automata.Examples.Supervision/ByzantineSystems.Automata.Examples.Supervision.fsproj";
            executable = "ByzantineSystems.Automata.Examples.Supervision";
          };
          hosted = mkExample {
            pname = "byzantine-systems-automata-hosted";
            projectFile = "examples/ByzantineSystems.Automata.Examples.Hosted/ByzantineSystems.Automata.Examples.Hosted.fsproj";
            executable = "ByzantineSystems.Automata.Examples.Hosted";
          };
        in
        {
          # This sets `pkgs` to a nixpkgs with allowUnfree option set.
          _module.args.pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };

          packages = {
            payment-processor = paymentProcessor;
            inherit supervision hosted;

            # `nix build` builds and exposes every example executable.
            default = pkgs.symlinkJoin {
              name = "${app_name}-examples-${version}";
              paths = [
                paymentProcessor
                supervision
                hosted
              ];
            };
          };

          # checks.application = app;

          # nix fmt + nix flake check (auto-wired by flakeModule)
          treefmt = {
            projectRootFile = "flake.nix";
            programs.actionlint.enable = true;
            programs.fantomas.enable = true;
            programs.nixfmt.enable = true;

            settings.formatter.pg_format = {
              command = "${pkgs.pgformatter}/bin/pg_format";
              options = [
                "--inplace"
                "-f"
                "2"
              ];
              includes = [ "*.sql" ];
              # pg_format parses PostgreSQL. The SQLite store's scripts use syntax it does not
              # know (STRICT tables, RAISE in trigger bodies, the -> JSON operator). Its output
              # for them still loads today, but a formatter guessing at another dialect is one
              # release away from rewriting a statement it does not understand.
              excludes = [ "src/ByzantineSystems.Automata.Storage.Sqlite/**/*.sql" ];
            };
          };

          # Native Nix devShells replacement
          devShells = {
            # nix develop .#ci
            ci = pkgs.mkShell {
              name = "ci-shell";
              buildInputs = [
                net10
                pkgs.gnumake
                # make schema-check formats the regenerated types before diffing them.
                pkgs.fantomas
              ];

              shellHook = ''
                echo "Entering CI shell..."
                dotnet --info
              '';
            };
          };

          devenv.shells.default = {
            devenv.root =
              let
                workingDirectory = builtins.getEnv "PWD";
              in
              if workingDirectory == "" then builtins.toString ./. else workingDirectory;

            # OCI packaging is defined above; disable devenv's implicit shell
            # containers so flake checks do not require unrelated container inputs.
            containers = lib.mkForce { };

            packages = with pkgs; [
              bash
              gnumake
              postgresql_19

              # for dotnet
              netcoredbg
              fsautocomplete
              fantomas
            ];

            languages.dotnet = {
              enable = true;
              package = net10;
            };

            services.postgres = {
              enable = true;
              package = pkgs.postgresql_19;
              extensions = ext: [
                ext.pg_cron
                ext.pgmq
              ];
              initdbArgs = [
                "--locale=C"
                "--encoding=UTF8"
              ];
              initialDatabases = [
                {
                  name = app_name;
                  user = app_name;
                  pass = app_name;
                }
              ];
              settings = {
                shared_preload_libraries = pkgs.lib.concatStringsSep "," [
                  "auto_explain"
                  "pg_cron"
                  "pg_stat_statements"
                ];
                session_preload_libraries = "auto_explain";
                "auto_explain.log_min_duration" = 150;
                "auto_explain.log_analyze" = true;
                log_min_duration_statement = 0;
                log_statement = "all";
                # pg_cron's background worker only runs in a single database.
                # Point it at the app's dev DB so `CREATE EXTENSION pg_cron`
                # and cron.schedule() operate on app tables.
                "cron.database_name" = "${app_name}";
                # pg_stat_statements config, nested attr sets need to be
                # converted to strings, otherwise postgresql.conf fails
                # to be generated.
                compute_query_id = "on";
                "pg_stat_statements.max" = 10000;
                "pg_stat_statements.track" = "all";
                # Adjust shared buffers
                shared_buffers = "1GB";
                # Increase work memory for large operations
                work_mem = "16MB";
                # Enable huge pages if available
                huge_pages = "try";
                # Adjust I/O concurrency settings
                effective_io_concurrency = 16;
                maintenance_io_concurrency = 16;
              }
              // lib.optionalAttrs pkgs.stdenv.isLinux {
                # Async IO, io_uring or workers
                # For io_uring method (Linux only, requires liburing)
                io_method = "io_uring";
              }
              // lib.optionalAttrs pkgs.stdenv.isDarwin {
                # in case "io_uring" is not available
                io_method = "worker";
                # For systems with many CPU cores and high I/O latency
                io_workers = 8;
                # For smaller systems or fast local storage
                # io_workers = 2;
              };
              port = 5432;
              listen_addresses = "127.0.0.1";
              initialScript = ''
                ALTER USER ${app_name} CREATEDB CREATEROLE;
              '';
            };

            env = {
              AUTOMATA_TEST_DB = db_connection_string;
              ConnectionStrings__BS_AUTOMATA_CONN = db_connection_string;
            };

            scripts = {
              migrate.exec = "make migrate";
              db-connect.exec = "make db";
              db-reset.exec = "make db-reset";
              run-example.exec = "dotnet run --project examples/ByzantineSystems.Automata.Examples.PaymentProcessor/ByzantineSystems.Automata.Examples.PaymentProcessor.fsproj";
            };

            enterShell = ''
              echo "Starting Development Environment..."
            '';
          };
        };
    };
}

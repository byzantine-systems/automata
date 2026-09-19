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
          version = "0.5.0";
          # app = pkgs.buildDotnetModule {
          #   pname = app_name;
          #   inherit version;
          #   src = pkgs.lib.cleanSourceWith {
          #     src = ./.;
          #     filter =
          #       path: type:
          #       let
          #         name = baseNameOf path;
          #       in
          #       !(
          #         type == "directory"
          #         && builtins.elem name [
          #           ".config"
          #           "out"
          #         ]
          #       );
          #   };
          #   projectFile = "examples/ByzantineSystems.Automata.Examples.PaymentProcessor/ByzantineSystems.Automata.Examples.PaymentProcessor.fsproj";
          #   nugetDeps = ./deps.json;
          #   dotnet-sdk = net10;
          #   dotnet-runtime = pkgs.dotnet-aspnetcore_10;
          #   executables = [ "ByzantineSystems.Automata.Examples.PaymentProcessor" ];
          #   doCheck = false;
          # };
        in
        {
          # This sets `pkgs` to a nixpkgs with allowUnfree option set.
          _module.args.pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };

          packages = {
            # default = app;
          };

          # checks.application = app;

          # nix fmt + nix flake check (auto-wired by flakeModule)
          treefmt = {
            projectRootFile = "flake.nix";
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
              postgresql_18

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
              package = pkgs.postgresql_18;
              extensions = ext: [
                ext.pg_cron
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

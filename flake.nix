# Essentially copy-pasted from nix-community/naersk
{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nix-community/naersk";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    dotfiles.url = "github:SirStoke/dotfiles-nix";
    lldb-nixpkgs.url = "github:patryk4815/nixpkgs/74616a18ab828ff01ff9c9b050974ffaa8b98862";
  };

  outputs = {
    self,
    flake-utils,
    naersk,
    nixpkgs,
    lldb-nixpkgs,
    dotfiles
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;

          config.allowUnfree = true;
        };

        lldb-pkgs = (import lldb-nixpkgs) {
          inherit system;

          config.allowUnfree = true;
        };

        naersk' = pkgs.callPackage naersk {};
      in rec {
        formatter = pkgs.alejandra;

        # For `nix build` & `nix run`:
        defaultPackage = naersk'.buildPackage {
          src = ./.;
          gitAllRefs = true; # We use our fork of arrow-rs

          buildInputs = with pkgs; [ pkgconfig openssl libiconv ];
        };

        packages.docker-image = pkgs.dockerTools.buildImage {
          name = "airquery";

          config = {
            Cmd = [  "${defaultPackage}/bin/airquery" ];
          };

          created = "now";
          tag = if self ? rev then self.rev else null;
        };

        # For `nix develop` (optional, can be skipped):
        devShell = pkgs.mkShell {
          packages = (with pkgs; [ rustup dotfiles.packages.${system}.idea-ultimate cargo-watch lldb-pkgs.lldb ]) ++ (pkgs.lib.lists.optional pkgs.stdenv.isLinux pkgs.autoPatchelfHook);

          shellHook =
            (
              pkgs.lib.strings.optionalString pkgs.stdenv.isLinux ''
                [[ -d "$HOME/.local/share/JetBrains/IntelliJIdea2022.3/intellij-rust" ]] && autoPatchelf $HOME/.local/share/JetBrains/IntelliJIdea2022.3/intellij-rust
              ''
            )
            + "rustup install nightly";

          nativeBuildInputs = with pkgs;
            (
              if stdenv.isDarwin
              then [darwin.apple_sdk.frameworks.Security]
              else []
            )
            ++ [ pkgconfig openssl libiconv lldb ];
        };
      }
    );
}

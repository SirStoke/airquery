# Essentially copy-pasted from nix-community/naersk
{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nix-community/naersk";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    dotfiles.url = "github:SirStoke/dotfiles-nix";
  };

  outputs = {
    self,
    flake-utils,
    naersk,
    nixpkgs,
    dotfiles,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;

          config.allowUnfree = true;
        };

        naersk' = pkgs.callPackage naersk {};
      in rec {
        formatter = pkgs.alejandra;

        # For `nix build` & `nix run`:
        defaultPackage = naersk'.buildPackage {
          src = ./.;
        };

        # For `nix develop` (optional, can be skipped):
        devShell = pkgs.mkShell {
          packages = with pkgs; [rustup dotfiles.packages.${system}.idea-ultimate autoPatchelfHook];

          shellHook =
            (
              if pkgs.stdenv.isLinux
              then ''
                [[ -d "$HOME/.local/share/JetBrains/IntelliJIdea2022.3/intellij-rust" ]] && autoPatchelf $HOME/.local/share/JetBrains/IntelliJIdea2022.3/intellij-rust
              ''
              else ""
            )
            + "rustup install stable";

          nativeBuildInputs = with pkgs;
            (
              if stdenv.isDarwin
              then [darwin.apple_sdk.frameworks.Security]
              else []
            )
            ++ [pkgconfig openssl libiconv];
        };
      }
    );
}

{
  inputs = {
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "nixpkgs/nixos-unstable";
  };

  outputs = {
    self,
    fenix,
    flake-utils,
    nixpkgs,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      inherit (pkgs) lib;
      inherit (fenix.packages.${system}.minimal) toolchain;
      pkgs = import nixpkgs {inherit system;};
      deps = with pkgs; [openssl];
      buildInputs =
        deps
        ++ (with pkgs; [
          pkg-config
        ]);
    in {
      packages.default =
        (pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        })
        .buildRustPackage {
          pname = "inat";
          version = "0.1.0";
          cargoLock.lockFile = ./Cargo.lock;

          src = lib.cleanSource ./.;

          buildInputs = deps;
          nativeBuildInputs = buildInputs;
        };

      devShells.default = pkgs.mkShell {
        inherit buildInputs;
        LD_LIBRARY_PATH = lib.makeLibraryPath deps;
      };
    });
}

# Agent instructions

Use the Nix development environment for tools provided by `flake.nix`
(currently Go). Run them with `nix develop -c <command>` or from a
`nix develop` shell. Other commands, such as Git and file operations,
can run directly.

Examples:

- `nix develop -c go run ./cmd/tasker`
- `nix develop -c go test ./...`
- `nix develop -c go fmt ./...`
- `git status`

# Agent instructions

Always execute commands through the Nix development environment defined in
`flake.nix`. Use `nix develop -c <command>` or, for shell expressions,
`nix develop -c bash -c '<commands>'`. Do not use host-installed development tools.

Examples:
- `nix develop -c go run ./cmd/tasker`
- `nix develop -c go test ./...`
- `nix develop -c go fmt ./...`

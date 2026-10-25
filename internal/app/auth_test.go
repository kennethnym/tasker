package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tasker/internal/tasker"
)

func TestAuthConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "password"), []byte(" secret \r\n"), 0600); err != nil {
		t.Fatal(err)
	}

	lookup := func(key string) (string, bool) { return "env-secret", key == "PASSWORD" }

	for _, tc := range []struct {
		name, yaml string
		valid      bool
	}{
		{"optional", "{}", true},
		{"both directions", "auth:\n  username: caller\n  password_env: PASSWORD\noutbound_auth:\n  - origin: https://worker.example\n    username: tasker\n    password_file: password\n", true},
		{"missing source", "auth:\n  username: caller", false},
		{"two sources", "auth:\n  username: caller\n  password_env: PASSWORD\n  password_file: password", false},
		{"missing env", "auth:\n  username: caller\n  password_env: MISSING", false},
		{"missing file", "auth:\n  username: caller\n  password_file: missing", false},
		{"bad username", "auth:\n  username: 'a:b'\n  password_env: PASSWORD", false},
		{"unknown field", "auth:\n  username: caller\n  password: secret", false},
		{"multiple documents", "{}\n---\n{}", false},
		{"missing origin", "outbound_auth:\n  - username: tasker\n    password_env: PASSWORD", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}

			parsed, err := loadConfig(path)
			var config tasker.Config
			if err == nil {
				config, err = resolveAuth(parsed, path, lookup)
			}

			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}

			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("error leaked secret")
			}

			if tc.name == "both directions" && (config.Auth.Password != "env-secret" || config.OutboundAuth["https://worker.example"].Password != " secret ") {
				t.Fatal("incorrect secret resolution")
			}
		})
	}

	for _, password := range []string{"", "\n", "\r\n", "secret\n\n", "secret\r"} {
		if err := os.WriteFile(filepath.Join(dir, "password"), []byte(password), 0600); err != nil {
			t.Fatal(err)
		}

		if _, err := (credentialSource{Username: "caller", PasswordFile: "password"}).resolve(dir, lookup); err == nil {
			t.Fatal("invalid password accepted")
		}
	}
}

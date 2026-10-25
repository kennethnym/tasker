package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"tasker/internal/tasker"
)

type credentialSource struct {
	Username     string `yaml:"username"`
	PasswordEnv  string `yaml:"password_env"`
	PasswordFile string `yaml:"password_file"`
}

type outboundCredential struct {
	Origin           string `yaml:"origin"`
	credentialSource `yaml:",inline"`
}

// resolveAuth loads secrets separately from parsing the application's config.
func resolveAuth(parsed appConfig, path string, lookup func(string) (string, bool)) (tasker.Config, error) {
	var config tasker.Config

	if parsed.Auth != nil {
		auth, err := parsed.Auth.resolve(filepath.Dir(path), lookup)
		if err != nil {
			return config, fmt.Errorf("auth: %w", err)
		}

		config.Auth = &auth
	}

	config.OutboundAuth = make(map[string]tasker.BasicAuth)
	for i, entry := range parsed.OutboundAuth {
		if entry.Origin == "" {
			return config, fmt.Errorf("outbound_auth[%d]: origin is required", i)
		}

		if _, exists := config.OutboundAuth[entry.Origin]; exists {
			return config, fmt.Errorf("outbound_auth[%d]: duplicate origin", i)
		}

		auth, err := entry.credentialSource.resolve(filepath.Dir(path), lookup)
		if err != nil {
			return config, fmt.Errorf("outbound_auth[%d]: %w", i, err)
		}

		config.OutboundAuth[entry.Origin] = auth
	}

	return config, nil
}

func (source credentialSource) resolve(dir string, lookup func(string) (string, bool)) (tasker.BasicAuth, error) {
	auth := tasker.BasicAuth{Username: source.Username}
	if (source.PasswordEnv == "") == (source.PasswordFile == "") {
		return auth, fmt.Errorf("provide exactly one of password_env or password_file")
	}

	if source.PasswordEnv != "" {
		var ok bool
		auth.Password, ok = lookup(source.PasswordEnv)
		if !ok {
			return auth, fmt.Errorf("password environment variable is not set")
		}
	} else {
		path := source.PasswordFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return auth, fmt.Errorf("cannot read password file")
		}

		auth.Password = string(data)
		if strings.HasSuffix(auth.Password, "\r\n") {
			auth.Password = strings.TrimSuffix(auth.Password, "\r\n")
		} else {
			auth.Password = strings.TrimSuffix(auth.Password, "\n")
		}
	}

	return auth, auth.Validate()
}

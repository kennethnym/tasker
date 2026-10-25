package tasker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestBasicAuthHandler(t *testing.T) {
	for _, path := range []string{"/tasks", "/tasks/id"} {
		for _, header := range []string{"", "Bearer secret", "Basic !!!", "Basic dXNlcjp3cm9uZw==", "Basic d3Jvbmc6c2VjcmV0", "Basic dXNlcjpzZWNyZXQ="} {
			called := false

			handler := basicAuthHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(204)
			}), &BasicAuth{"user", "secret"})

			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", header)

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)

			valid := header == "Basic dXNlcjpzZWNyZXQ="
			if called != valid {
				t.Fatalf("unexpected access for %s with %q", path, header)
			}

			if !valid && (response.Code != 401 || response.Header().Get("WWW-Authenticate") == "") {
				t.Fatal("missing authentication challenge")
			}
		}
	}

	handler := basicAuthHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/tasks", nil))
	if response.Code != 204 {
		t.Fatal("optional auth rejected request")
	}
}

func TestOrigin(t *testing.T) {
	for raw, want := range map[string]string{
		"https://EXAMPLE.com/path?q=1": "https://example.com:443",
		"https://example.com:443":      "https://example.com:443",
		"http://example.com":           "http://example.com:80",
		"https://example.com:8443":     "https://example.com:8443",
		"http://İ.example":             "http://xn--i-9bb.example:80",
		"http://i.example":             "http://i.example:80",
		"http://[FE80::1%25Eth0]":      "http://[fe80::1%Eth0]:80",
		"http://[fe80::1%25eth0]":      "http://[fe80::1%eth0]:80",
		"http://[::1]:3001/path":       "http://[::1]:3001",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}

		got, err := Origin(u)
		if err != nil || got != want {
			t.Errorf("%s: got %s, %v", raw, got, err)
		}
	}

	for _, raw := range []string{"http://user:secret@example.com", "ftp://example.com", "http://example.com:99999", "http://a\u200db.example", "http://[not:ipv6]"} {
		u, err := url.Parse(raw)
		if err != nil {
			continue
		}

		if _, err := Origin(u); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestOutboundAuthentication(t *testing.T) {
	var received string
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.Header.Get("Authorization")
		w.WriteHeader(204)
	}))
	defer destination.Close()

	u, _ := url.Parse(destination.URL)
	origin, _ := Origin(u)

	for _, configuredOrigin := range []string{"", origin, strings.Replace(origin, "http://", "https://", 1), "http://other.example:" + u.Port(), "http://127.0.0.1:1"} {
		matched := configuredOrigin == origin
		received = ""

		config := DefaultConfig
		config.OutboundAuth = map[string]BasicAuth{}
		if configuredOrigin != "" {
			config.OutboundAuth[configuredOrigin] = BasicAuth{"tasker", "secret"}
		}

		e := newExecutor(config, 0, nil)
		_, err := e.deliver(context.Background(), Task{URL: destination.URL + "/work", Input: []byte(`{}`), DispatchDeadline: "1s"})
		if err != nil {
			t.Fatal(err)
		}

		if (received != "") != matched {
			t.Fatalf("matched=%v received=%q", matched, received)
		}

		if matched {
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("Authorization", received)

			user, pass, ok := req.BasicAuth()
			if !ok || user != "tasker" || pass != "secret" {
				t.Fatal("wrong outbound credentials")
			}
		}
	}

	received = ""

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, 307)
	}))
	defer redirect.Close()

	ru, _ := url.Parse(redirect.URL)
	ro, _ := Origin(ru)

	config := DefaultConfig
	config.OutboundAuth = map[string]BasicAuth{ro: {"tasker", "secret"}}
	_, err := newExecutor(config, 0, nil).deliver(context.Background(), Task{URL: redirect.URL, Input: []byte(`{}`), DispatchDeadline: "1s"})
	if err == nil || received != "" {
		t.Fatal("redirect followed or not reported as failure")
	}

	u.User = url.UserPassword("user", "secret")
	_, err = newExecutor(config, 0, nil).deliver(context.Background(), Task{URL: u.String(), DispatchDeadline: "1s"})
	if err == nil || received != "" {
		t.Fatal("URL credentials accepted")
	}
}

func TestOutboundOriginConfiguration(t *testing.T) {
	// Construction does not call storage; embedding satisfies its interface.
	storage := struct{ Storage }{}
	for _, origins := range [][]string{
		{"https://İ.example", "https://xn--i-9bb.example"},
		{"https://worker/path"}, {"https://worker?query"}, {"https://worker#"},
		{"https://user:password@worker"}, {"https://worker", "https://WORKER:443/"},
	} {
		config := Config{Storage: storage, OutboundAuth: map[string]BasicAuth{}}
		for _, origin := range origins {
			config.OutboundAuth[origin] = BasicAuth{"user", "password"}
		}

		if _, err := New(config); err == nil {
			t.Errorf("accepted origins %v", origins)
		}
	}

	service, err := New(Config{Storage: storage, OutboundAuth: map[string]BasicAuth{"https://WORKER/": {"user", "password"}}})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := service.config.OutboundAuth["https://worker:443"]; !ok {
		t.Fatal("origin was not normalized")
	}
}

// Exercise the real HTTP transport so matching is checked against its dial
// destination, without external DNS or requiring local IPv6 interfaces.
func TestOutboundAuthMatchesTransportHost(t *testing.T) {
	for _, tc := range []struct {
		name, configured, target, dialAddress string
		wantAuth                              bool
	}{
		{"unicode does not match ASCII", "http://i.example", "http://İ.example", "xn--i-9bb.example:80", false},
		{"ASCII does not match unicode", "http://İ.example", "http://i.example", "i.example:80", false},
		{"unicode matches punycode", "http://xn--i-9bb.example", "http://İ.example", "xn--i-9bb.example:80", true},
		{"punycode matches unicode", "http://İ.example", "http://xn--i-9bb.example", "xn--i-9bb.example:80", true},
		{"distinct zones", "http://[fe80::1%25eth0]", "http://[fe80::1%25Eth0]", "[fe80::1%Eth0]:80", false},
		{"same zone", "http://[fe80::1%25Eth0]", "http://[fe80::1%25Eth0]", "[fe80::1%Eth0]:80", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Get("Authorization")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			addresses := make(chan string, 1)
			transport := &http.Transport{
				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					addresses <- address
					return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
				},
			}
			defer transport.CloseIdleConnections()

			config, err := validateAndNormalizeConfig(Config{
				Storage:      struct{ Storage }{},
				OutboundAuth: map[string]BasicAuth{tc.configured: {"tasker", "secret"}},
			})
			if err != nil {
				t.Fatal(err)
			}

			executor := newExecutor(config, 0, nil)
			executor.client.Transport = transport
			_, err = executor.deliver(context.Background(), Task{URL: tc.target, Input: []byte(`{}`), DispatchDeadline: "1s"})
			if err != nil {
				t.Fatal(err)
			}

			if got := <-addresses; got != tc.dialAddress {
				t.Fatalf("transport dialed %q, want %q", got, tc.dialAddress)
			}

			if got := <-headers; (got != "") != tc.wantAuth {
				t.Fatalf("Authorization present = %v, want %v", got != "", tc.wantAuth)
			}
		})
	}
}

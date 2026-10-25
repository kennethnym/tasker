package e2e_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBasicAuthentication(t *testing.T) {
	worker := newExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "tasker" || password != "worker-secret" {
			w.WriteHeader(401)

			return
		}

		w.WriteHeader(204)
	})

	dir := t.TempDir()
	for name, value := range map[string]string{"api-password": "api-secret\n", "worker-password": "worker-secret\r\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}

	p := startTasker(t, "", nil, map[string]any{
		"metrics": map[string]any{"enabled": true},
		"auth": map[string]any{
			"username":      "caller",
			"password_file": filepath.Join(dir, "api-password"),
		},
		"outbound_auth": []map[string]any{{
			"origin":        worker.server.URL,
			"username":      "tasker",
			"password_file": filepath.Join(dir, "worker-password"),
		}},
	})

	for _, endpoint := range []struct{ method, path string }{{"POST", "/tasks"}, {"GET", "/tasks/missing"}, {"GET", "/metrics"}} {
		status, headers, _ := p.request(t, endpoint.method, endpoint.path, nil)
		if status != 401 || headers.Get("WWW-Authenticate") == "" {
			t.Fatalf("unprotected endpoint: %s %d", endpoint.path, status)
		}
	}

	request := func(method, path string, body []byte) (int, []byte) {
		t.Helper()

		req, err := http.NewRequest(method, p.url+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}

		req.SetBasicAuth("caller", "api-secret")

		resp, err := p.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		defer resp.Body.Close()

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		return resp.StatusCode, data
	}

	if status, body := request("GET", "/metrics", nil); status != 200 {
		t.Fatalf("authenticated scrape: %d %s", status, body)
	}

	payload, _ := json.Marshal(map[string]any{"url": worker.server.URL + "/work", "input": map[string]any{}})
	status, body := request("POST", "/tasks", payload)
	if status != 201 {
		t.Fatalf("create: %d %s", status, body)
	}

	var created task
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}

	eventually(t, waitTimeout, "authenticated task completion", func() bool {
		status, body := request("GET", "/tasks/"+created.ID, nil)
		if status != 200 {
			t.Fatalf("get: %d %s", status, body)
		}

		var result task
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatal(err)
		}

		return result.Status == "succeeded"
	})

	delivered := worker.next(t, time.Second)
	req := &http.Request{Header: delivered.Header}
	user, pass, ok := req.BasicAuth()
	if !ok || user != "tasker" || pass != "worker-secret" {
		t.Fatal("outbound credentials not applied")
	}
}

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const waitTimeout = 10 * time.Second

var binaryPath string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "tasker-e2e-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)
	binaryPath = filepath.Join(dir, "tasker")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Build the real executable once for the entire suite.
	build := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/tasker")
	build.Dir = "../.."
	if output, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build tasker: %v\n%s", err, output)
		return 1
	}
	return m.Run()
}

// These wire types deliberately do not import Tasker's implementation.
type task struct {
	ID               string          `json:"id"`
	Kind             string          `json:"kind"`
	URL              string          `json:"url"`
	Input            json.RawMessage `json:"input"`
	Status           string          `json:"status"`
	ScheduleTime     time.Time       `json:"scheduleTime"`
	DispatchDeadline string          `json:"dispatchDeadline"`
	Attempt          int             `json:"attempt"`
	MaxAttempts      int             `json:"maxAttempts"`
	Result           json.RawMessage `json:"result"`
	LastError        string          `json:"lastError"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
	StartedAt        *time.Time      `json:"startedAt"`
	FinishedAt       *time.Time      `json:"finishedAt"`
}

type process struct {
	cmd          *exec.Cmd
	done         chan struct{}
	exitErr      error // Published by closing done.
	killed       bool
	output       *processOutput
	url          string
	client       *http.Client
	lastResponse string
}

type processOutput struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	ready chan string
}

var listenAddress = regexp.MustCompile(`address=(127\.0\.0\.1:\d+)\r?\n`)

func (o *processOutput) Write(b []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.buf.Write(b)
	if match := listenAddress.FindStringSubmatch(o.buf.String()); len(match) == 2 {
		select {
		case o.ready <- "http://" + match[1]:
		default:
		}
	}
	return n, err
}

func (o *processOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

func startTasker(t *testing.T, database string, overrides map[string]string) *process {
	t.Helper()
	if database == "" {
		database = filepath.Join(t.TempDir(), "tasks.db")
	}
	settings := map[string]string{
		"listen-address": "127.0.0.1:0", "database-path": database,
		"shutdown-timeout": "2s", "dispatch-deadline": "5s",
		"max-attempts": "3", "min-backoff": "100ms", "max-backoff": "100ms",
		"max-concurrent-dispatches": "4", "max-dispatches-per-second": "1000",
		"task-retention": "1h",
	}
	for key, value := range overrides {
		settings[key] = value
	}
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var args []string
	for _, key := range keys {
		args = append(args, "--"+key+"="+settings[key])
	}
	p := &process{
		cmd: exec.Command(binaryPath, args...), done: make(chan struct{}),
		output: &processOutput{ready: make(chan string, 1)},
		client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{}},
	}
	p.cmd.Dir = filepath.Dir(database)
	p.cmd.Env = []string{}
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "TASKER_") {
			p.cmd.Env = append(p.cmd.Env, value)
		}
	}
	p.cmd.Stdout, p.cmd.Stderr = p.output, p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start tasker: %v", err)
	}
	go func() {
		p.exitErr = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		p.stop(t, false)
		p.client.CloseIdleConnections()
		if t.Failed() {
			t.Logf("tasker %s logs:\n%s\nlast API response: %s", p.url, p.output.String(), p.lastResponse)
		}
	})
	select {
	case p.url = <-p.output.ready:
	case <-p.done:
		t.Fatalf("tasker exited during startup: %v\n%s", p.exitErr, p.output.String())
	case <-time.After(waitTimeout):
		t.Fatalf("tasker did not report its listen address\n%s", p.output.String())
	}
	eventually(t, waitTimeout, "HTTP readiness", func() bool {
		response, err := p.client.Get(p.url + "/tasks/readiness-probe")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode == http.StatusNotFound
	})
	return p
}

func (p *process) stop(t *testing.T, force bool) {
	t.Helper()
	select {
	case <-p.done:
	default:
		var err error
		if force {
			p.killed = true
			err = p.cmd.Process.Kill()
		} else {
			err = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		if err != nil {
			t.Errorf("stop tasker: %v", err)
		}
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("tasker did not exit within 5s")
			p.killed = true
			_ = p.cmd.Process.Kill()
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				t.Error("tasker did not exit after kill")
				return
			}
		}
	}
	if !p.killed && p.exitErr != nil {
		t.Errorf("tasker exited unsuccessfully: %v", p.exitErr)
	}
}

func (p *process) request(t *testing.T, method, path string, body []byte) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, p.url+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := p.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	p.lastResponse = fmt.Sprintf("%s %s: %d %s", method, path, response.StatusCode, abbreviate(string(data)))
	return response.StatusCode, response.Header, data
}

func (p *process) create(t *testing.T, url string, input string, fields map[string]any) task {
	t.Helper()
	payload := map[string]any{"url": url, "input": json.RawMessage(input)}
	for key, value := range fields {
		payload[key] = value
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	status, headers, body := p.request(t, http.MethodPost, "/tasks", body)
	if status != http.StatusCreated {
		t.Fatalf("create: HTTP %d: %s", status, body)
	}
	created := decodeTask(t, headers, body)
	if created.ID == "" || headers.Get("Location") != "/tasks/"+created.ID {
		t.Fatalf("invalid creation identity: id=%q location=%q", created.ID, headers.Get("Location"))
	}
	if created.Status != "queued" || created.Attempt != 0 {
		t.Fatalf("creation must return queued attempt zero: %+v", created)
	}
	if created.URL != url {
		t.Errorf("created URL = %q, want %q", created.URL, url)
	}
	assertJSON(t, created.Input, input)
	return created
}

func (p *process) get(t *testing.T, id string) task {
	t.Helper()
	status, headers, body := p.request(t, http.MethodGet, "/tasks/"+id, nil)
	if status != http.StatusOK {
		t.Fatalf("get %s: HTTP %d: %s", id, status, body)
	}
	return decodeTask(t, headers, body)
}

func decodeTask(t *testing.T, headers http.Header, body []byte) task {
	t.Helper()
	if headers.Get("Content-Type") != "application/json" {
		t.Fatalf("task response Content-Type = %q", headers.Get("Content-Type"))
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode task fields: %v", err)
	}
	for key := range fields {
		if strings.Contains(strings.ToLower(key), "lease") {
			t.Errorf("internal lease field exposed: %s", key)
		}
	}
	var result task
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	return result
}

func (p *process) awaitStatus(t *testing.T, id, status string, timeout time.Duration) task {
	t.Helper()
	var latest task
	eventually(t, timeout, id+" becomes "+status, func() bool {
		latest = p.get(t, id)
		return latest.Status == status
	})
	return latest
}

func eventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %s waiting for %s", timeout, description)
		case <-ticker.C:
		}
	}
}

type delivery struct {
	Method  string
	URI     string
	Header  http.Header
	Body    []byte
	At      time.Time
	ReadErr error
}

type executor struct {
	server     *httptest.Server
	deliveries chan delivery
	mu         sync.Mutex
	history    []delivery
}

func newExecutor(t *testing.T, respond func(http.ResponseWriter, *http.Request)) *executor {
	t.Helper()
	e := &executor{deliveries: make(chan delivery, 256)}
	e.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		d := delivery{Method: r.Method, URI: r.URL.RequestURI(), Header: r.Header.Clone(), Body: body, At: time.Now(), ReadErr: err}
		e.mu.Lock()
		e.history = append(e.history, d)
		e.mu.Unlock()
		select {
		case e.deliveries <- d:
		default:
			t.Error("executor delivery buffer overflow")
		}
		if respond != nil {
			respond(w, r)
		} else {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	}))
	t.Cleanup(func() {
		e.server.CloseClientConnections()
		e.server.Close()
		if t.Failed() {
			for _, d := range e.recorded() {
				t.Logf("executor at %s: %s %s task=%s attempt=%s body=%s readError=%v", d.At.Format(time.RFC3339Nano), d.Method, d.URI, d.Header.Get("X-Task-Id"), d.Header.Get("X-Task-Attempt"), abbreviate(string(d.Body)), d.ReadErr)
			}
		}
	})
	return e
}

func (e *executor) recorded() []delivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]delivery(nil), e.history...)
}

func (e *executor) next(t *testing.T, timeout time.Duration) delivery {
	t.Helper()
	select {
	case d := <-e.deliveries:
		return d
	case <-time.After(timeout):
		t.Fatalf("no executor delivery within %s", timeout)
		return delivery{}
	}
}

// Negative assertions require a bounded observation window. Callers choose one
// that spans the relevant schedule, backoff, or dispatcher poll interval.
func (e *executor) quiet(t *testing.T, duration time.Duration) {
	t.Helper()
	select {
	case d := <-e.deliveries:
		t.Fatalf("unexpected delivery: task=%s attempt=%s", d.Header.Get("X-Task-Id"), d.Header.Get("X-Task-Attempt"))
	case <-time.After(duration):
	}
}

func assertDelivery(t *testing.T, d delivery, created task, attempt int, uri string) {
	t.Helper()
	if d.ReadErr != nil || d.Method != http.MethodPost || d.URI != uri {
		t.Errorf("delivery: method=%q uri=%q readError=%v", d.Method, d.URI, d.ReadErr)
	}
	for header, want := range map[string]string{
		"Content-Type": "application/json", "X-Task-Id": created.ID, "X-Task-Attempt": strconv.Itoa(attempt),
	} {
		if got := d.Header.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	assertJSON(t, d.Body, string(created.Input))
}

func assertJSON(t *testing.T, actual []byte, expected string) {
	t.Helper()
	decode := func(data []byte) any {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("invalid JSON %q: %v", abbreviate(string(data)), err)
		}
		return value
	}
	if !reflect.DeepEqual(decode(actual), decode([]byte(expected))) {
		t.Errorf("JSON = %s, want %s", abbreviate(string(actual)), abbreviate(expected))
	}
}

func assertTerminal(t *testing.T, result task, status string, attempts int) {
	t.Helper()
	if result.Status != status || result.Attempt != attempts {
		t.Errorf("terminal state = %s attempt %d, want %s attempt %d", result.Status, result.Attempt, status, attempts)
	}
	if result.CreatedAt.IsZero() || result.StartedAt == nil || result.FinishedAt == nil {
		t.Fatalf("missing lifecycle timestamps: %+v", result)
	}
	if result.StartedAt.Before(result.CreatedAt) || result.FinishedAt.Before(*result.StartedAt) || result.UpdatedAt.Before(*result.FinishedAt) {
		t.Errorf("timestamps out of order: %+v", result)
	}
	if status == "succeeded" && result.LastError != "" {
		t.Errorf("successful task retained error: %s", result.LastError)
	}
	if status == "failed" && result.LastError == "" {
		t.Error("failed task has no error")
	}
}

func abbreviate(s string) string {
	if len(s) > 2048 {
		return s[:2048] + "..."
	}
	return s
}

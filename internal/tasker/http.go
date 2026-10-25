package tasker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func (t *Tasker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", t.handleCreateTask)
	mux.HandleFunc("GET /tasks/{id}", t.handleFindTask)

	if t.metrics != nil {
		mux.Handle("GET /metrics", t.metrics.handler)
	}

	return t.observeRequests(basicAuthHandler(mux, t.config.Auth), mux)
}

func (t *Tasker) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request struct {
		CreateTaskConfig
		DispatchDeadline json.RawMessage `json:"dispatchDeadline"`
	}

	if err := decoder.Decode(&request); err != nil {
		writeDecodeError(w, err)
		return
	}

	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		writeDecodeError(w, err)
		return
	}

	config := request.CreateTaskConfig
	if request.DispatchDeadline != nil {
		if err := json.Unmarshal(request.DispatchDeadline, &config.DispatchDeadline); err != nil || config.DispatchDeadline == "" {
			http.Error(w, ErrInvalidDispatchDeadline.Error(), http.StatusBadRequest)
			return
		}
	}

	task, err := t.CreateTask(r.Context(), config)
	if err != nil {
		if errors.Is(err, ErrInvalidTaskURL) || errors.Is(err, ErrInvalidTaskInput) || errors.Is(err, ErrInvalidDispatchDeadline) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			requestLogger(r).ErrorContext(r.Context(), "failed to create task", "error", err)
			http.Error(w, "failed to create task", http.StatusInternalServerError)
		}

		return
	}

	w.Header().Set("Location", "/tasks/"+task.ID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(task)
}

func (t *Tasker) handleFindTask(w http.ResponseWriter, r *http.Request) {
	task, err := t.FindTask(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			http.Error(w, "task not found", http.StatusNotFound)
		} else {
			requestLogger(r).ErrorContext(r.Context(), "failed to find task", "task_id", r.PathValue("id"), "error", err)
			http.Error(w, "failed to find task", http.StatusInternalServerError)
		}

		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(task)
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		http.Error(w, "request body exceeds 1 MiB", http.StatusRequestEntityTooLarge)
		return
	}

	http.Error(w, "body must contain one JSON object with valid task fields", http.StatusBadRequest)
}

type requestLoggerKey struct{}

func requestLogger(r *http.Request) *slog.Logger {
	return r.Context().Value(requestLoggerKey{}).(*slog.Logger)
}

type accessResponse struct {
	http.ResponseWriter
	status int
	size   int
}

func (w *accessResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *accessResponse) WriteHeader(status int) {
	if w.status != 0 {
		return
	}

	if status >= 200 {
		w.status = status
	}

	w.ResponseWriter.WriteHeader(status)
}

func (w *accessResponse) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}

	n, err := w.ResponseWriter.Write(b)
	w.size += n

	return n, err
}

func (t *Tasker) observeRequests(next http.Handler, mux *http.ServeMux) http.Handler {
	logger := slog.With("component", "http")
	accessLogger := slog.With("component", "http.access")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := uuid.NewString()
		w.Header().Set("X-Request-Id", requestID)

		r = r.WithContext(context.WithValue(r.Context(), requestLoggerKey{}, logger.With("request_id", requestID)))

		measure := t.metrics != nil && r.URL.Path != "/metrics"
		if !t.config.AccessLog && !measure {
			next.ServeHTTP(w, r)
			return
		}

		response := &accessResponse{ResponseWriter: w}
		defer func() {
			recovered := recover()
			status := response.status
			if status == 0 {
				status = http.StatusOK
			}

			if recovered != nil {
				status = http.StatusInternalServerError
			}

			duration := time.Since(started)
			if measure {
				// Resolve before-auth rejections too, without labeling by raw paths.
				_, pattern := mux.Handler(r)
				method := metricMethod(r.Method)
				route := metricRoute(pattern)
				t.metrics.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
				t.metrics.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
			}

			if t.config.AccessLog {
				level := slog.LevelInfo
				if status >= 500 {
					level = slog.LevelError
				}

				// Use the route pattern, never raw URLs, query strings, headers or bodies.
				accessLogger.Log(r.Context(), level, "handled request", "request_id", requestID, "method", r.Method, "route", r.Pattern, "status", status, "size", response.size, "duration", duration)
			}

			if recovered != nil {
				panic(recovered)
			}
		}()

		next.ServeHTTP(response, r)
	})
}

func metricMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		return method

	default:
		return "other"
	}
}

func metricRoute(pattern string) string {
	switch pattern {
	case "POST /tasks":
		return "/tasks"

	case "GET /tasks/{id}":
		return "/tasks/{id}"

	default:
		return "unmatched"
	}
}

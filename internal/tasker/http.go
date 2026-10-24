package tasker

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

func (t *Tasker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", t.handleCreateTask)
	mux.HandleFunc("GET /tasks/{id}", t.handleFindTask)

	return mux
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

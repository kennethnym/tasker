package tasker

import "net/http"

func (t *Tasker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", t.handleCreateTask)
	mux.HandleFunc("GET /tasks/{id}", t.handleFindTask)
	return mux
}

func (t *Tasker) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	http.Error(w, ErrNotImplemented.Error(), http.StatusNotImplemented)
}

func (t *Tasker) handleFindTask(w http.ResponseWriter, r *http.Request) {
	http.Error(w, ErrNotImplemented.Error(), http.StatusNotImplemented)
}

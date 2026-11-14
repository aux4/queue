package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type queueConfig struct {
	Name    string   `json:"name"`
	WS      string   `json:"ws"`
	Webhook string   `json:"webhook"`
	Header  []string `json:"header"`
}

func startServer(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	queuesJSON := getArg(args, 2, "")

	manager := NewQueueManager(token)

	if queuesJSON != "" {
		var configs []queueConfig
		if err := json.Unmarshal([]byte(queuesJSON), &configs); err != nil {
			fmt.Fprintf(os.Stderr, "invalid queues JSON: %v\n", err)
			os.Exit(1)
		}
		for _, cfg := range configs {
			if cfg.Name == "" {
				continue
			}
			if err := manager.CreateWithProxy(cfg.Name, cfg.WS, cfg.Webhook, cfg.Header); err != nil {
				fmt.Fprintf(os.Stderr, "failed to create queue %q: %v\n", cfg.Name, err)
			}
		}
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required")
			return
		}
		wsURL := r.URL.Query().Get("ws")
		webhookURL := r.URL.Query().Get("webhook")
		headerValues := r.URL.Query()["header"]

		var headers []string
		for _, h := range headerValues {
			parts := strings.Split(h, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					headers = append(headers, p)
				}
			}
		}

		if wsURL != "" || webhookURL != "" {
			if err := manager.CreateWithProxy(name, wsURL, webhookURL, headers); err != nil {
				httpError(w, http.StatusConflict, err.Error())
				return
			}
		} else {
			if err := manager.Create(name); err != nil {
				httpError(w, http.StatusConflict, err.Error())
				return
			}
		}

		// Work-queue opt-in: ack/nack/lease semantics instead of pub/sub
		// fan-out. Defaults (workQueue=false) leave every pre-existing queue
		// untouched.
		if r.URL.Query().Get("workQueue") == "true" {
			maxAttempts := 0
			if v := r.URL.Query().Get("maxAttempts"); v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					maxAttempts = n
				}
			}
			if q, err := manager.Get(name); err == nil {
				q.ConfigureWorkQueue(true, maxAttempts)
			}
		}

		httpJSON(w, http.StatusCreated, map[string]string{"status": "CREATED", "name": name})
	})

	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required")
			return
		}
		if err := manager.Delete(name); err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		httpJSON(w, http.StatusOK, map[string]string{"status": "DELETED", "name": name})
	})

	mux.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		httpJSON(w, http.StatusOK, manager.List())
	})

	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			httpError(w, http.StatusBadRequest, "failed to read body")
			return
		}
		if !json.Valid(body) {
			httpError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		if q.IsWorkQueue() {
			// Work-queue mode: append to the FIFO backlog instead of fanning
			// out to live consumers. --group/--id are optional query params
			// (never required, so this stays backward compatible for every
			// pre-existing non-work queue).
			id := q.EnqueueWork(body, r.URL.Query().Get("id"), r.URL.Query().Get("group"))
			httpJSON(w, http.StatusOK, map[string]interface{}{"status": "SENT", "id": id})
			return
		}

		delivered := q.Publish(body)
		httpJSON(w, http.StatusOK, map[string]interface{}{"status": "SENT", "consumers": delivered})
	})

	mux.HandleFunc("/lease", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		if !q.IsWorkQueue() {
			httpError(w, http.StatusBadRequest, fmt.Sprintf("queue %q is not a work queue", name))
			return
		}
		visibilitySeconds, err := strconv.Atoi(r.URL.Query().Get("visibility"))
		if err != nil || visibilitySeconds <= 0 {
			httpError(w, http.StatusBadRequest, "visibility (seconds) is required and must be > 0")
			return
		}
		msg, leaseID, ok := q.Lease(time.Duration(visibilitySeconds) * time.Second)
		if !ok {
			httpError(w, http.StatusNotFound, "no message available")
			return
		}
		httpJSON(w, http.StatusOK, map[string]interface{}{
			"id":       msg.id,
			"leaseId":  leaseID,
			"group":    msg.group,
			"attempts": msg.attempts,
			"body":     json.RawMessage(msg.body),
		})
	})

	mux.HandleFunc("/renew", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		leaseID := r.URL.Query().Get("leaseId")
		if name == "" || leaseID == "" {
			httpError(w, http.StatusBadRequest, "name and leaseId are required")
			return
		}
		visibilitySeconds, err := strconv.Atoi(r.URL.Query().Get("visibility"))
		if err != nil || visibilitySeconds <= 0 {
			httpError(w, http.StatusBadRequest, "visibility (seconds) is required and must be > 0")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		if !q.Renew(leaseID, time.Duration(visibilitySeconds)*time.Second) {
			httpError(w, http.StatusNotFound, "leaseId not found or already expired")
			return
		}
		httpJSON(w, http.StatusOK, map[string]string{"status": "RENEWED"})
	})

	mux.HandleFunc("/ack", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		leaseID := r.URL.Query().Get("leaseId")
		if name == "" || leaseID == "" {
			httpError(w, http.StatusBadRequest, "name and leaseId are required")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		if !q.Ack(leaseID) {
			httpError(w, http.StatusNotFound, "leaseId not found")
			return
		}
		httpJSON(w, http.StatusOK, map[string]string{"status": "ACKED"})
	})

	mux.HandleFunc("/nack", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		leaseID := r.URL.Query().Get("leaseId")
		if name == "" || leaseID == "" {
			httpError(w, http.StatusBadRequest, "name and leaseId are required")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		found, movedToDLQ := q.Nack(leaseID)
		if !found {
			httpError(w, http.StatusNotFound, "leaseId not found")
			return
		}
		status := "NACKED"
		if movedToDLQ {
			status = "DEAD_LETTERED"
		}
		httpJSON(w, http.StatusOK, map[string]interface{}{"status": status})
	})

	mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		out := make([]map[string]interface{}, 0)
		for _, m := range q.DLQ() {
			out = append(out, map[string]interface{}{
				"id":       m.id,
				"group":    m.group,
				"attempts": m.attempts,
				"body":     json.RawMessage(m.body),
			})
		}
		httpJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			httpError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !manager.ValidateToken(r.URL.Query().Get("token")) {
			httpError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required")
			return
		}
		q, err := manager.Get(name)
		if err != nil {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			httpError(w, http.StatusInternalServerError, "streaming not supported")
			return
		}

		id, ch := q.Subscribe()
		defer q.Unsubscribe(id)

		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				w.Write(msg)
				w.Write([]byte("\n"))
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	pidFile := pidFilePath(port)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write pid file: %v\n", err)
		os.Exit(1)
	}

	addr := fmt.Sprintf(":%s", port)
	if token != "" {
		fmt.Fprintf(os.Stderr, "queue server started on port %s (token required)\n", port)
	} else {
		fmt.Fprintf(os.Stderr, "queue server started on port %s\n", port)
	}

	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nshutting down...")
		os.Remove(pidFile)
		server.Close()
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		os.Remove(pidFile)
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func pidFilePath(port string) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf(".aux4-queue-%s.pid", port))
}

func httpError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func httpJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

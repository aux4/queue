package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type Queue struct {
	name      string
	mu        sync.RWMutex
	consumers map[string]chan []byte
	counter   atomic.Uint64

	wsURL      string
	wsHeaders  http.Header
	wsConn     *websocket.Conn
	wsMu       sync.Mutex
	wsStop     chan struct{}
	webhookURL string
	webhookHeaders http.Header
}

func NewQueue(name string) *Queue {
	return &Queue{
		name:      name,
		consumers: make(map[string]chan []byte),
	}
}

func (q *Queue) ConfigureWebSocket(wsURL string, headers http.Header) error {
	if wsURL == "" {
		return nil
	}
	q.wsMu.Lock()
	defer q.wsMu.Unlock()
	q.wsURL = wsURL
	q.wsHeaders = headers
	q.wsStop = make(chan struct{})
	return q.connectWS()
}

func (q *Queue) connectWS() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.Dial(q.wsURL, q.wsHeaders)
	if err != nil {
		return fmt.Errorf("websocket connect failed: %w", err)
	}
	q.wsConn = conn

	go q.wsReadLoop()

	return nil
}

func (q *Queue) wsReadLoop() {
	for {
		select {
		case <-q.wsStop:
			return
		default:
		}
		if q.wsConn == nil {
			return
		}
		_, _, err := q.wsConn.ReadMessage()
		if err != nil {
			q.wsMu.Lock()
			if q.wsConn != nil {
				q.wsConn.Close()
				q.wsConn = nil
			}
			q.wsMu.Unlock()
			q.reconnectWS()
			return
		}
	}
}

func (q *Queue) reconnectWS() {
	for i := 0; i < 5; i++ {
		select {
		case <-q.wsStop:
			return
		case <-time.After(time.Duration(i+1) * time.Second):
		}
		q.wsMu.Lock()
		err := q.connectWS()
		q.wsMu.Unlock()
		if err == nil {
			return
		}
	}
}

func (q *Queue) ConfigureWebhook(webhookURL string, headers http.Header) {
	if webhookURL == "" {
		return
	}
	q.webhookURL = webhookURL
	q.webhookHeaders = headers
}

func (q *Queue) Close() {
	q.wsMu.Lock()
	if q.wsStop != nil {
		select {
		case <-q.wsStop:
		default:
			close(q.wsStop)
		}
	}
	if q.wsConn != nil {
		q.wsConn.Close()
		q.wsConn = nil
	}
	q.wsMu.Unlock()
}

func (q *Queue) Subscribe() (string, chan []byte) {
	id := fmt.Sprintf("consumer-%d", q.counter.Add(1))
	ch := make(chan []byte, 256)
	q.mu.Lock()
	q.consumers[id] = ch
	q.mu.Unlock()
	return id, ch
}

func (q *Queue) Unsubscribe(id string) {
	q.mu.Lock()
	if ch, ok := q.consumers[id]; ok {
		close(ch)
		delete(q.consumers, id)
	}
	q.mu.Unlock()
}

func (q *Queue) Publish(msg []byte) int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	delivered := 0
	for _, ch := range q.consumers {
		select {
		case ch <- msg:
			delivered++
		default:
		}
	}

	q.proxyToWebSocket(msg)
	q.proxyToWebhook(msg)

	return delivered
}

func (q *Queue) proxyToWebSocket(msg []byte) {
	q.wsMu.Lock()
	defer q.wsMu.Unlock()
	if q.wsConn == nil {
		return
	}
	if err := q.wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
		q.wsConn.Close()
		q.wsConn = nil
		go q.reconnectWS()
	}
}

func (q *Queue) proxyToWebhook(msg []byte) {
	if q.webhookURL == "" {
		return
	}
	go func() {
		req, err := http.NewRequest(http.MethodPost, q.webhookURL, bytes.NewReader(msg))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		for key, vals := range q.webhookHeaders {
			for _, val := range vals {
				req.Header.Set(key, val)
			}
		}
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}

func (q *Queue) ConsumerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.consumers)
}

type QueueManager struct {
	mu     sync.RWMutex
	queues map[string]*Queue
	token  string
}

func NewQueueManager(token string) *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
		token:  token,
	}
}

func (qm *QueueManager) ValidateToken(token string) bool {
	if qm.token == "" {
		return true
	}
	return qm.token == token
}

func (qm *QueueManager) Create(name string) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if _, ok := qm.queues[name]; ok {
		return fmt.Errorf("queue %q already exists", name)
	}
	qm.queues[name] = NewQueue(name)
	return nil
}

func (qm *QueueManager) CreateWithProxy(name, wsURL, webhookURL string, headers []string) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if _, ok := qm.queues[name]; ok {
		return fmt.Errorf("queue %q already exists", name)
	}
	q := NewQueue(name)

	httpHeaders := parseHeaders(headers)

	if wsURL != "" {
		if err := q.ConfigureWebSocket(wsURL, httpHeaders); err != nil {
			return fmt.Errorf("failed to connect websocket: %w", err)
		}
	}
	if webhookURL != "" {
		q.ConfigureWebhook(webhookURL, httpHeaders)
	}

	qm.queues[name] = q
	return nil
}

func (qm *QueueManager) Delete(name string) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	q, ok := qm.queues[name]
	if !ok {
		return fmt.Errorf("queue %q not found", name)
	}
	q.Close()
	q.mu.Lock()
	for id, ch := range q.consumers {
		close(ch)
		delete(q.consumers, id)
	}
	q.mu.Unlock()
	delete(qm.queues, name)
	return nil
}

func (qm *QueueManager) Get(name string) (*Queue, error) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	q, ok := qm.queues[name]
	if !ok {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	return q, nil
}

func (qm *QueueManager) List() []map[string]interface{} {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	list := make([]map[string]interface{}, 0, len(qm.queues))
	for name, q := range qm.queues {
		entry := map[string]interface{}{
			"name":      name,
			"consumers": q.ConsumerCount(),
		}
		if q.wsURL != "" {
			entry["ws"] = q.wsURL
		}
		if q.webhookURL != "" {
			entry["webhook"] = q.webhookURL
		}
		list = append(list, entry)
	}
	return list
}

func parseHeaders(headers []string) http.Header {
	h := http.Header{}
	for _, header := range headers {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			if key != "" {
				h.Set(key, val)
			}
		}
	}
	return h
}

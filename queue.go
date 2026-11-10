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

	// Work-queue mode (opt-in per queue at creation time via create --workQueue
	// true). A work-queue queue never fans out through Publish/consumers -- its
	// messages live in the FIFO backlog below and are only delivered through
	// lease/ack/nack, exactly once at a time (per message, and per group when a
	// message has one). Pre-existing pub/sub queues are never affected: this
	// field defaults to false and every new field below is only read when it is
	// true.
	workQueue   bool
	maxAttempts int
	msgCounter  atomic.Uint64
	backlog     []*workMessage
	dlq         []*workMessage
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

// workMessage is one message in a work-queue's FIFO backlog.
type workMessage struct {
	id          string
	body        []byte
	group       string
	attempts    int
	createdAt   time.Time
	leaseID     string
	leaseExpiry time.Time
}

// ConfigureWorkQueue opts a queue into work-queue mode: ack/nack/lease
// semantics instead of pub/sub fan-out. maxAttempts <= 0 means unlimited
// attempts (never moved to the dead-letter queue by nack).
func (q *Queue) ConfigureWorkQueue(workQueue bool, maxAttempts int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workQueue = workQueue
	q.maxAttempts = maxAttempts
}

// IsWorkQueue reports whether this queue was created with --workQueue true.
func (q *Queue) IsWorkQueue() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.workQueue
}

// EnqueueWork appends a message to the FIFO backlog. id defaults to an
// internally generated id when empty; group is optional (empty = no group
// serialization for this message).
func (q *Queue) EnqueueWork(body []byte, id, group string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if id == "" {
		id = fmt.Sprintf("msg-%d", q.msgCounter.Add(1))
	}
	q.backlog = append(q.backlog, &workMessage{
		id:        id,
		body:      body,
		group:     group,
		createdAt: time.Now(),
	})
	return id
}

// siblingGroupLeased reports whether any message OTHER than the one at index
// skip, sharing the given group, is currently under a live (unexpired)
// lease. Must be called with q.mu held.
func (q *Queue) siblingGroupLeased(group string, skip int, now time.Time) bool {
	if group == "" {
		return false
	}
	for i, m := range q.backlog {
		if i == skip {
			continue
		}
		if m.group == group && m.leaseID != "" && m.leaseExpiry.After(now) {
			return true
		}
	}
	return false
}

// Lease picks the oldest eligible message in the backlog (not under a live
// lease, and -- when the message has a group -- no sibling in the same group
// is under a live lease either), assigns it a new lease (visibility
// timeout), and returns a copy. Returns ok=false when nothing is eligible.
func (q *Queue) Lease(visibility time.Duration) (workMessage, string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for i, m := range q.backlog {
		if m.leaseID != "" && m.leaseExpiry.After(now) {
			continue // already leased, not yet expired
		}
		if q.siblingGroupLeased(m.group, i, now) {
			continue // a sibling in the same group is actively leased
		}
		leaseID := fmt.Sprintf("lease-%d", q.counter.Add(1))
		m.leaseID = leaseID
		m.leaseExpiry = now.Add(visibility)
		return *m, leaseID, true
	}
	return workMessage{}, "", false
}

// Ack removes a message from the backlog once its lease matches. Returns
// false when the leaseId does not match any current message (already
// acked/nacked-to-dlq, or never existed).
func (q *Queue) Ack(leaseID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, m := range q.backlog {
		if m.leaseID == leaseID {
			q.backlog = append(q.backlog[:i], q.backlog[i+1:]...)
			return true
		}
	}
	return false
}

// Nack releases a message for immediate redelivery and increments its
// attempts counter. Once attempts reaches the queue's maxAttempts (when
// configured > 0), the message is moved to the dead-letter queue instead of
// staying eligible. Returns (found, movedToDLQ).
func (q *Queue) Nack(leaseID string) (bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, m := range q.backlog {
		if m.leaseID == leaseID {
			m.attempts++
			m.leaseID = ""
			m.leaseExpiry = time.Time{}
			if q.maxAttempts > 0 && m.attempts >= q.maxAttempts {
				q.backlog = append(q.backlog[:i], q.backlog[i+1:]...)
				q.dlq = append(q.dlq, m)
				return true, true
			}
			return true, false
		}
	}
	return false, false
}

// DLQ returns a snapshot of the dead-letter queue.
func (q *Queue) DLQ() []workMessage {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]workMessage, len(q.dlq))
	for i, m := range q.dlq {
		out[i] = *m
	}
	return out
}

// BacklogDepth returns the number of messages currently in the FIFO backlog
// (leased or not).
func (q *Queue) BacklogDepth() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.backlog)
}

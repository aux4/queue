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

	// webhookPump delivers a work queue's messages to its webhookURL through
	// lease/ack/nack instead of the pub/sub fire-and-forget proxyToWebhook
	// below, so a push delivery that fails (non-2xx response or a timeout) is
	// retry-safe: the message is nacked (redelivered, attempts incremented)
	// instead of being silently dropped, and still reaches the dead-letter
	// queue once maxAttempts is exhausted, exactly like a pulled lease would.
	webhookPumpMu   sync.Mutex
	webhookPumpStop chan struct{}
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
	q.stopWebhookPump()
}

// startWebhookPump launches (once) the background loop that leases this
// work queue's own messages and pushes each one to webhookURL, ack'ing on a
// 2xx response and nack'ing on anything else (non-2xx status or a timeout/
// connection error) -- see the retry-safety note on the webhookPump* fields.
// A no-op when already running, when this is not a work queue, or when no
// webhook is configured.
func (q *Queue) startWebhookPump() {
	q.webhookPumpMu.Lock()
	defer q.webhookPumpMu.Unlock()
	if q.webhookPumpStop != nil {
		return
	}
	if !q.IsWorkQueue() || q.webhookURL == "" {
		return
	}
	stop := make(chan struct{})
	q.webhookPumpStop = stop
	go q.webhookPumpLoop(stop)
}

func (q *Queue) stopWebhookPump() {
	q.webhookPumpMu.Lock()
	defer q.webhookPumpMu.Unlock()
	if q.webhookPumpStop == nil {
		return
	}
	close(q.webhookPumpStop)
	q.webhookPumpStop = nil
}

// webhookPumpVisibility is the lease visibility timeout the pump holds a
// message under while delivering it. It must comfortably exceed the HTTP
// client timeout used for the webhook POST so a message is never nacked by
// a sibling concern (visibility expiry) before the delivery attempt itself
// has a chance to time out and nack it for the real reason.
const webhookPumpVisibility = 30 * time.Second
const webhookPumpClientTimeout = 10 * time.Second
const webhookPumpIdleInterval = 200 * time.Millisecond

func (q *Queue) webhookPumpLoop(stop chan struct{}) {
	client := &http.Client{Timeout: webhookPumpClientTimeout}
	for {
		select {
		case <-stop:
			return
		default:
		}

		msg, leaseID, ok := q.Lease(webhookPumpVisibility)
		if !ok {
			select {
			case <-stop:
				return
			case <-time.After(webhookPumpIdleInterval):
			}
			continue
		}

		if q.deliverWebhook(client, msg) {
			q.Ack(leaseID)
		} else {
			q.Nack(leaseID)
		}
	}
}

// deliverWebhook POSTs one message to the queue's webhookURL and reports
// whether delivery succeeded (a 2xx response). A request error (including a
// client-side timeout) and any non-2xx response both count as failure, so
// the caller nacks -- the message is redelivered (attempts incremented) and
// eventually dead-lettered once maxAttempts is reached, same as a lease
// pulled by a consumer that then nacks it.
func (q *Queue) deliverWebhook(client *http.Client, msg workMessage) bool {
	req, err := http.NewRequest(http.MethodPost, q.webhookURL, bytes.NewReader(msg.body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	for key, vals := range q.webhookHeaders {
		for _, val := range vals {
			req.Header.Set(key, val)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
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
// attempts (never moved to the dead-letter queue by nack). When the queue
// also has a webhookURL configured (create --webhook), this starts the
// retry-safe webhook pump (see startWebhookPump) instead of leaving push
// delivery to the pub/sub-only, fire-and-forget proxyToWebhook.
func (q *Queue) ConfigureWorkQueue(workQueue bool, maxAttempts int) {
	q.mu.Lock()
	q.workQueue = workQueue
	q.maxAttempts = maxAttempts
	q.mu.Unlock()
	if workQueue {
		q.startWebhookPump()
	}
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

// Renew extends a message's current lease to a new visibility timeout,
// counted from now -- a heartbeat for a caller that is still processing a
// message it already leased and expects to run longer than the original
// visibility timeout. Only extends a still-live lease: if leaseID does not
// match any message, or the message's lease already expired (and may since
// have been picked up by a different Lease call), it is NOT renewed, so a
// slow caller can never silently steal a message back from whoever now holds
// it. Returns false (no error) in that case.
func (q *Queue) Renew(leaseID string, visibility time.Duration) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for _, m := range q.backlog {
		if m.leaseID == leaseID {
			if m.leaseExpiry.Before(now) {
				return false // lease already expired -- do not resurrect it
			}
			m.leaseExpiry = now.Add(visibility)
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

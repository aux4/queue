# aux4/queue

In-memory message queue with pub/sub support for aux4.

## Installation

```bash
aux4 aux4 pkger install aux4/queue
```

## Usage

### Start the server

```bash
aux4 queue start
aux4 queue start --port 9000 --token mysecret
```

### Create a queue

```bash
aux4 queue create --name jobs
```

### Send messages

```bash
echo '{"task":"build"}' | aux4 queue send --name jobs
cat events.jsonl | aux4 queue send --name jobs
```

### Receive messages

```bash
aux4 queue receive --name jobs
aux4 queue receive --name jobs --limit 10
aux4 queue receive --name jobs | jq .
```

### Stop the server

```bash
aux4 queue stop
aux4 queue stop --port 9000
```

### List queues

```bash
aux4 queue list
```

### Delete a queue

```bash
aux4 queue delete --name jobs
```

## Proxy (WebSocket & Webhook)

Queues can be configured to automatically forward every published message to a WebSocket or webhook endpoint. This enables real-time integration with external systems.

### Create a queue with webhook proxy

Every message sent to this queue will also be POSTed to the webhook URL.

```bash
aux4 queue create --name events --webhook https://api.example.com/ingest
```

### Create a queue with WebSocket proxy

Every message sent to this queue will also be written to the WebSocket connection.

```bash
aux4 queue create --name logs --ws wss://stream.example.com/logs
```

### Custom headers

Attach HTTP headers to proxy requests (applied to both WebSocket handshake and webhook POST).

```bash
aux4 queue create --name secure-events \
  --webhook https://api.example.com/ingest \
  --header "Authorization: Bearer tok123"
```

### Pre-configured queues on startup

Use `--queues` to create queues with proxy settings when the server starts.

```bash
aux4 queue start --queues '[
  {"name": "events", "webhook": "https://api.example.com/ingest", "header": ["Authorization: Bearer tok123"]},
  {"name": "logs", "ws": "wss://stream.example.com/logs"}
]'
```

## Work queue mode

By default, `aux4 queue` is a pub/sub fan-out broker: every connected `receive` gets every message,
nothing is retried, and nothing survives a restart. **Work queue mode** adds the semantics a real
job/task queue needs on top of the same package: a message is delivered to exactly one consumer at a
time, failures are retried with a visibility timeout, repeated failures land in a dead-letter queue,
and messages for the same logical conversation (e.g. a chat) are always processed one at a time.

Work queue mode is opt-in per queue (`--workQueue true` at `create` time) and never changes how an
existing pub/sub queue behaves — `send`/`receive`/`create`/`delete`/`list` are untouched for queues
that don't opt in.

### Storage: memory vs repository

```bash
aux4 queue start --storage memory      # default
aux4 queue start --storage repository --db .queue.db
```

| | `--storage memory` (default) | `--storage repository` |
|---|---|---|
| Durability | Volatile — lost on restart | Durable — every message is a row in a SQLite (or Turso/libSQL) database |
| Process | A long-running `queue start` server; every command is a network round trip to it | No server process at all; every command (`send`/`lease`/`ack`/`nack`) opens the database directly |
| Latency | Fast — in-process memory | One `aux4 repository` subprocess + a database round trip per operation |
| Best for | High-throughput, short-lived jobs where losing the queue on a crash is acceptable | Jobs that must survive a crash/restart, or need to be inspected/queried as data (e.g. a chatbot's inbound message backlog) |

Pick `memory` when speed matters more than surviving a restart; pick `repository` when the queue
itself needs to be durable. `--storage`/`--db` are accepted on every work-queue command
(`create`, `delete`, `list`, `send`, `lease`, `ack`, `nack`, `dlq`) — pick the same storage/db for
every call against a given queue.

### Create a work queue

```bash
aux4 queue create --name jobs --workQueue true --maxAttempts 5
aux4 queue create --storage repository --db .queue.db --name jobs --maxAttempts 5
```

`--maxAttempts` caps how many times a message is redelivered before it is dead-lettered (`0`, the
default, means unlimited retries).

### Send, lease, ack, nack

```bash
echo '{"task":"build"}' | aux4 queue send --name jobs
echo '{"task":"reply"}' | aux4 queue send --name jobs --group chat-42   # same group = one at a time
```

```bash
aux4 queue lease --name jobs --visibility 30
# {"id":"msg-1","leaseId":"lease-1","group":"","attempts":0,"body":{"task":"build"}}
```

`lease` returns the next eligible message (oldest first) and holds it under a visibility timeout:
while the lease is active, no other `lease` call can pick up the same message, and — when the
message has a `--group` — no sibling in the same group either. If nothing is eligible, `lease`
prints `{}` (memory) or `[]` (repository) and exits non-zero.

```bash
aux4 queue ack --name jobs --leaseId lease-1     # done — remove it
aux4 queue nack --name jobs --leaseId lease-1    # failed — redeliver now, bump attempts
```

`nack` releases the message for immediate redelivery and increments its attempts counter. Once
`maxAttempts` is reached, the message stops being eligible for `lease` and shows up in `aux4 queue
dlq` instead.

```bash
aux4 queue dlq --name jobs | jq .
```

### Renewing a lease for a slow consumer

A consumer that is still processing a message and expects to run longer than its original
`--visibility` can extend the lease in place with `renew` — a heartbeat, so it is not redelivered to
a second consumer while the first is still working on it:

```bash
LEASE=$(aux4 queue lease --name jobs --visibility 30)
LEASE_ID=$(echo "$LEASE" | jq -r '.leaseId')
# ... still working ...
aux4 queue renew --name jobs --leaseId "$LEASE_ID" --visibility 30
```

`renew` only extends a lease that is still live. If it already expired — and especially if the
message has since been leased by someone else — `renew` fails instead of resurrecting it (`{}` /
`[]`, non-zero exit, same convention as `lease`). In `--storage repository`, identify the message
with `--id` (the id `lease` returned) instead of `--leaseId`.

### Pushing to a webhook is retry-safe on a work queue

A queue that is both a work queue and has `--webhook` configured (see [create](#create-a-work-queue))
pushes each message through lease/ack/nack instead of the plain pub/sub fire-and-forget proxy: a 2xx
response acks it, anything else (non-2xx or a timeout) nacks it, so a failed delivery is retried and
eventually dead-lettered — never silently dropped. A **non-work** queue's `--webhook` is unaffected
(still fire-and-forget), since it never holds a lease.

### Group key: one-at-a-time delivery per conversation

Pass `--group <key>` on `send` to serialize delivery of everything sharing that key — the classic
use case is a chatbot that must finish replying to one message in a conversation before starting the
next one, even though many conversations are processed concurrently:

```bash
echo '{"text":"hi"}'  | aux4 queue send --name inbox --group chat-42
echo '{"text":"bye"}' | aux4 queue send --name inbox --group chat-42
```

A `lease` call will only ever return one `chat-42` message at a time; the second stays ineligible
until the first is acked (or its lease expires).

### Repository storage: the underlying database

In `--storage repository`, each queue's messages live in a `<name>_messages` table managed by
`aux4/repository`, and the queue registry itself lives in a `_queues` table — both are visible to
`aux4 repository find`/`read` for inspection like any other repository data. There is no separate
dead-letter table: a dead-lettered message is simply a message whose `attempts` has reached
`maxAttempts`, which `aux4 queue dlq`/`lease` account for via `--maxAttempts`.

Any queue name is accepted, including one with dashes or dots (`bot-inbox`, `bot.inbox`). An
underscore-only name maps to its messages table unchanged (`bot_inbox` → `bot_inbox_messages`,
exactly as before); any other name is sanitized into a safe table identifier and disambiguated with
a short hash of the original name, so it works end to end and two different names are never mapped
to the same table even if they sanitize to the same text.

## Authentication

Pass `--token` to `start` to require authentication. All clients must then include the same `--token`.

```bash
aux4 queue start --token mysecret
aux4 queue create --name jobs --token mysecret
```

#### Description

Create a new queue on the server. The queue must have a unique name.

Optionally, configure a WebSocket URL (`--ws`) and/or webhook URL (`--webhook`) to proxy every published message to an external endpoint. Use `--header` to attach custom HTTP headers to the proxy requests.

#### Usage

```bash
aux4 queue create --name <queue-name>
aux4 queue create --name <queue-name> --token <token>
aux4 queue create --name <queue-name> --webhook <url>
aux4 queue create --name <queue-name> --ws <url>
aux4 queue create --name <queue-name> --webhook <url> --header "Authorization: Bearer token"
```

#### Example

```bash
aux4 queue create --name jobs
```
```text
{"name":"jobs","status":"CREATED"}
```

#### Example with webhook proxy

```bash
aux4 queue create --name events --webhook https://api.example.com/ingest --header "Authorization: Bearer tok123"
```
```text
{"name":"events","status":"CREATED"}
```

#### Example with WebSocket proxy

```bash
aux4 queue create --name logs --ws wss://stream.example.com/logs
```
```text
{"name":"logs","status":"CREATED"}
```

#### Work queue mode

Pass `--workQueue true` to opt this queue into ack/nack/lease semantics instead of pub/sub fan-out
(memory storage only — a queue created with `--storage repository` is always a work queue, there is
no pub/sub concept without a server process). `--maxAttempts` caps delivery attempts before a
message is dead-lettered (`0`, the default, means unlimited).

```bash
aux4 queue create --name jobs --workQueue true --maxAttempts 5
aux4 queue create --storage repository --db .queue.db --name jobs --maxAttempts 5
```

#### Webhook push on a work queue is retry-safe

When a queue is both a work queue (`--workQueue true`, or any `--storage repository` queue) *and*
has `--webhook` configured, each message is pushed through the same lease/ack/nack machinery a pull
consumer (`queue lease` + `queue ack`/`queue nack`) would use, instead of the plain pub/sub
fire-and-forget proxy: a 2xx response from the webhook acks the message, and anything else — a
non-2xx status or a timeout/connection error — nacks it, so the message is redelivered (attempts
incremented) and eventually reaches the dead-letter queue once `--maxAttempts` is exhausted, exactly
as if a pull consumer had nacked it. A **non-work queue's** `--webhook` stays the original
fire-and-forget pub/sub proxy — unaffected by this, since it never holds a lease at all.

```bash
aux4 queue create --name jobs --workQueue true --maxAttempts 3 --webhook https://api.example.com/ingest
```

A delivery that returns 500 (or times out) is retried automatically; after 3 failed attempts the
message shows up in `aux4 queue dlq --name jobs` instead of being silently dropped.

#### Queue names under `--storage repository`

Any name is accepted, including one with dashes or dots (`bot-inbox`, `bot.inbox`) — both common and
both already fine under `--storage memory`. Internally, `--storage repository` maps the name to a
SQL table identifier for its messages: an underscore-only name maps unchanged (`bot_inbox` stays
`bot_inbox_messages`, exactly as before); any other name is sanitized and disambiguated with a short
hash of the original name, so it works end to end and two different names never collide on the same
table even if they sanitize to the same text.

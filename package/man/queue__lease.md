#### Description

Lease the next eligible message from a work queue (a queue created with `--workQueue true`, or any
queue under `--storage repository`). Leasing holds the message under a visibility timeout: while the
lease is active, no other `lease` call can pick it up — and when the message was sent with a
`--group`, no sibling in the same group either, so messages for the same conversation are always
processed one at a time.

Messages are leased oldest-first. If the lease isn't acked or nacked before the visibility timeout
elapses, the message becomes eligible again automatically — there is no separate "release" step and
no background sweeper to configure.

When nothing is eligible, `lease` prints `{}` (memory storage) or `[]` (repository storage) and
exits non-zero.

#### Usage

```bash
aux4 queue lease --name <queue-name> --visibility <seconds>
aux4 queue lease --storage repository --db <path> --name <queue-name> --visibility <seconds> [--maxAttempts <n>]
```

--storage       `memory` (default) or `repository`
--db            Database path or `libsql://` URL, only used with `--storage repository`
--name          Queue name
--visibility    Visibility timeout in seconds (default `30`)
--owner         Opaque identifier of the leasing caller (repository storage only)
--maxAttempts   Exclude messages that have already reached this many attempts (repository storage only; must match the queue's `--maxAttempts`, 0 means unlimited)

#### Example

```bash
echo '{"task":"send-email"}' | aux4 queue send --name jobs
aux4 queue lease --name jobs --visibility 30
```

```text
{"id":"msg-1","leaseId":"lease-1","group":"","attempts":0,"body":{"task":"send-email"}}
```

A second lease call inside the visibility window finds nothing else:

```bash
aux4 queue lease --name jobs --visibility 30
```

```text
{}
```

#### Example (repository storage)

```bash
echo '{"task":"send-email"}' | aux4 queue send --storage repository --db .queue.db --name jobs
aux4 queue lease --storage repository --db .queue.db --name jobs --visibility 30
```

```text
[{"id":"3f1a...","body":{"task":"send-email"},"group":"","attempts":0,"leaseExpiresAt":1732310430}]
```

Related commands: `aux4 queue ack` to complete a leased message, `aux4 queue nack` to retry it now,
`aux4 queue dlq` to see dead-lettered messages.

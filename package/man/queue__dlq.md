#### Description

List the dead-letter messages for a work queue: messages that were `nack`ed until their attempts
reached the queue's `--maxAttempts`. There is no separate dead-letter table — in repository storage
this is a query (`attempts >= maxAttempts`) over the same messages table, and in memory storage it's
the queue's own in-memory dead-letter list, populated by `nack` once the limit is reached.

#### Usage

```bash
aux4 queue dlq --name <queue-name>
aux4 queue dlq --storage repository --db <path> --name <queue-name> --maxAttempts <n>
```

--storage       `memory` (default) or `repository`
--db            Database path or `libsql://` URL, only used with `--storage repository`
--name          Queue name
--maxAttempts   The queue's `--maxAttempts` (repository storage only; must match the queue's configured value)

#### Example

```bash
aux4 queue dlq --name jobs
```

```text
[{"id":"msg-3","group":"","attempts":5,"body":{"task":"send-email"}}]
```

Related commands: `aux4 queue nack` to retry a leased message (and dead-letter it once
`maxAttempts` is reached), `aux4 queue lease`/`ack` for the normal processing path.

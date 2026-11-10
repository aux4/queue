#### Description

Release a message previously returned by `aux4 queue lease` for immediate redelivery, incrementing
its attempts counter. Once attempts reaches the queue's `--maxAttempts` (set at `queue create`
time), the message stops being eligible for `lease` and instead shows up in `aux4 queue dlq` — there
is no separate dead-letter table or move step, "dead-lettered" is simply `attempts >= maxAttempts`.

In repository storage, `--maxAttempts` must be supplied by the caller (the queue's configured
`--maxAttempts`) — repository storage has no running process to look this up on the caller's
behalf. The attempts counter itself is bumped server-side (`aux4 repository patch --increment`),
so `--attempts` is accepted for backward compatibility but ignored — you no longer need to pass the
value returned by `lease`.

#### Usage

```bash
aux4 queue nack --name <queue-name> --leaseId <leaseId>
aux4 queue nack --storage repository --db <path> --name <queue-name> --id <id> --attempts <n> --maxAttempts <n>
```

--storage       `memory` (default) or `repository`
--db            Database path or `libsql://` URL, only used with `--storage repository`
--name          Queue name
--leaseId       The `leaseId` returned by `queue lease` (memory storage only)
--id            The message `id` returned by `queue lease` (repository storage only)
--attempts      Deprecated, ignored (repository storage only); attempts is now bumped server-side
--maxAttempts   The queue's `--maxAttempts` (repository storage only; 0 means unlimited)

#### Example

```bash
LEASE=$(aux4 queue lease --name jobs --visibility 300)
LEASE_ID=$(echo "$LEASE" | jq -r .leaseId)
aux4 queue nack --name jobs --leaseId "$LEASE_ID"
```

```text
{"status":"NACKED"}
```

Once the queue's `maxAttempts` is reached, the same call instead reports:

```text
{"status":"DEAD_LETTERED"}
```

Related commands: `aux4 queue lease` to obtain a leaseId/id, `aux4 queue ack` to complete instead of
retrying, `aux4 queue dlq` to inspect dead-lettered messages.

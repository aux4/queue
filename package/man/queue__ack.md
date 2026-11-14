#### Description

Acknowledge (complete) a message previously returned by `aux4 queue lease`, removing it from the
queue for good. Acking a leaseId/id that no longer exists (already acked, or the lease has since
expired and the message was re-leased and completed by someone else) is reported as an error.

#### Usage

```bash
aux4 queue ack --name <queue-name> --leaseId <leaseId>
aux4 queue ack --storage repository --db <path> --name <queue-name> --id <id>
```

--storage   `memory` (default) or `repository`
--db        Database path or `libsql://` URL, only used with `--storage repository`
--name      Queue name
--leaseId   The `leaseId` returned by `queue lease` (memory storage only)
--id        The message `id` returned by `queue lease` (repository storage only)

#### Example

```bash
LEASE=$(aux4 queue lease --name jobs --visibility 30)
LEASE_ID=$(echo "$LEASE" | jq -r .leaseId)
aux4 queue ack --name jobs --leaseId "$LEASE_ID"
```

```text
{"status":"ACKED"}
```

`ack` is a hot-path command — unlike `create`/`send`/`lease`/`list`/`dlq`, it does not probe for
whether `--provider`/`--storage` is installed, so an unknown provider here falls through to aux4's
own generic "command not found" instead of the friendly `install aux4/queue-<provider>` message
(the provider was already proven to exist by the time there is a lease to ack).

Related commands: `aux4 queue lease` to obtain a leaseId/id, `aux4 queue nack` to retry instead of
completing.

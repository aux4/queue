#### Description

Extend a still-live lease to a new visibility timeout, counted from now — a heartbeat for a caller
that is still processing a message it already leased (via `aux4 queue lease`) and expects to run
longer than the lease's original visibility timeout. Without `renew`, a slow consumer risks its
lease expiring mid-processing and the same message being leased to a second worker while the first
is still working on it.

`renew` only ever extends a **still-live** lease. If the lease already expired — and especially if
the message has since been picked up by a different `lease` call — `renew` fails instead of
resurrecting it, so a slow caller can never silently steal the message back from whoever now holds
it.

- **Memory storage:** identify the lease with `--leaseId` (the value returned by `lease`).
- **Repository storage:** identify the message with `--id` (the value returned by `lease`).

When the lease is not found (unknown id/leaseId) or has already expired, `renew` prints `{}` (memory
storage) or `[]` (repository storage) and exits non-zero — the same convention `lease` uses for an
empty result.

#### Usage

```bash
aux4 queue renew --name <queue-name> --leaseId <lease-id> --visibility <seconds>
aux4 queue renew --storage repository --db <path> --name <queue-name> --id <message-id> --visibility <seconds>
```

--storage       `memory` (default) or `repository`
--db            Database path or `libsql://` URL, only used with `--storage repository`
--name          Queue name
--leaseId       The leaseId returned by `queue lease` (memory storage only)
--id            The message id returned by `queue lease` (repository storage only)
--visibility    New visibility timeout in seconds, counted from now (required)

#### Example

```bash
echo '{"task":"send-email"}' | aux4 queue send --name jobs
LEASE=$(aux4 queue lease --name jobs --visibility 30)
LEASE_ID=$(echo "$LEASE" | jq -r '.leaseId')
aux4 queue renew --name jobs --leaseId "$LEASE_ID" --visibility 30
```

```text
{"status":"RENEWED"}
```

#### Example (repository storage)

```bash
echo '{"task":"send-email"}' | aux4 queue send --storage repository --db .queue.db --name jobs
LEASE=$(aux4 queue lease --storage repository --db .queue.db --name jobs --visibility 30)
ID=$(echo "$LEASE" | jq -r '.[0].id')
aux4 queue renew --storage repository --db .queue.db --name jobs --id "$ID" --visibility 30
```

```text
3f1a...
```

`renew` is a hot-path command — unlike `create`/`send`/`lease`/`list`/`dlq`, it does not probe for
whether `--provider`/`--storage` is installed, so an unknown provider here falls through to aux4's
own generic "command not found" instead of the friendly `install aux4/queue-<provider>` message
(the provider was already proven to exist by the time there is a lease to renew). This also keeps
`renew` cheap for a frequent lease-heartbeat, since every provider-existence probe spawns a process.

Related commands: `aux4 queue lease` to acquire the lease in the first place, `aux4 queue ack` to
complete it, `aux4 queue nack` to release it for immediate redelivery.

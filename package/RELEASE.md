# aux4/queue 0.2.6 (unreleased)

## Changed

- The reserved `cloud` provider's "not installed" install hint now points at `aux4/cloud-queue`
  (not `aux4/queue-cloud`, which was renamed to match the aux4 `cloud-` prefix convention used by
  `aux4/cloud-file-sync` and `aux4/cloud-state-provider`). Every other provider keeps the existing
  `aux4/queue-<provider>` hint (e.g. `aux4/queue-sqs`) unchanged.

# aux4/queue 0.2.5

## Changed

- `renew`, `ack` and `nack` no longer run the `--provider`/`--storage` install-check probe
  (`aux4 queue <provider> --help`) that `create`/`send`/`lease`/`list`/`dlq` still run before
  dispatching. These three are the hot path of an active lease (hit once per lease-renew heartbeat
  interval, potentially every few seconds, under `aux4/bot`'s `work --mode pull`) and the provider
  has already been proven to exist by the time there is a lease to renew/ack/nack. An unknown
  `--provider` on these three now falls straight through to aux4's own generic "command not found"
  instead of the friendly `install aux4/queue-<provider>` message — the first real fix for a renew
  heartbeat occasionally missing its window on a short `--visibility` under load (each renew used to
  spawn an extra `--help` probe process before the real renew call).

2 new tests (`queue__renew.test.md`, `queue__workqueue.test.md`); all 53 previously-shipped tests
pass unmodified (55/56 total — the one pre-existing failure, `queue__provider.test.md`'s reserved
`cloud` provider check, is a stale local `aux4/queue-cloud` install on the test machine, unrelated
to this change).

# aux4/queue 0.2.3

## Added

- `renew --leaseId/--id --visibility`: extends a still-live lease to a new visibility timeout,
  counted from now — a heartbeat for a caller still processing a message that expects to run longer
  than the lease's original visibility. Only ever extends a lease that is still live; a lease that
  already expired (and may have been picked up by a different `lease` call) is not resurrected,
  same empty-result convention as `lease` (`{}`/`[]`, non-zero exit). Works in both `--storage
  memory` (new `Queue.Renew`/`/renew` route) and `--storage repository` (new `aux4/repository renew`
  primitive, pinned `aux4/repository@>=1.3.9`).
- A work queue with `--webhook` configured now pushes each message through the same lease/ack/nack
  machinery a pull consumer would use, instead of the plain pub/sub fire-and-forget proxy: a 2xx
  response acks the message, anything else (non-2xx status or a timeout/connection error) nacks it —
  redelivered with attempts incremented, eventually dead-lettered once `maxAttempts` is reached. A
  **non-work** queue's `--webhook` is unaffected (still fire-and-forget pub/sub), since it never
  holds a lease.

## Fixed

- `--storage repository` queue names may now contain a dash or a dot (`bot-inbox`, `bot.inbox`) —
  both common and both already fine under `--storage memory` — instead of failing with a SQL syntax
  error. An underscore-only name still maps to exactly the same `<name>_messages` table it always
  has (no migration needed); any other name is sanitized into a safe identifier and disambiguated
  with a short hash of the original name, so two different names can never collide on the same
  table.

## Changed

- `package/.aux4`: the remaining inline shell `if [ ... ]; then ... fi` storage dispatch (one per
  public command) is now a single literal dispatch line (`aux4 queue value(storage) <cmd> ...`,
  already injection-safe via `value()`) with no shell conditional at all — an unknown `--storage`
  value simply resolves to a nonexistent subcommand ("Command not found") instead of a custom error
  message. `aux4 lint run --strict true` is clean; no behavior change for a valid `--storage`.

12 new tests (`queue__renew.test.md`, `queue__webhook-workqueue.test.md`,
`queue__dashed-names.test.md`); all 36 previously-shipped tests pass unmodified (48/48 total).

# aux4/queue 0.2.2

## Changed

- Rewrote `--storage repository` as pure aux4 DSL composition: every command now validates
  `--storage` and dispatches with a dynamic subcommand (`aux4 queue value(storage) <cmd>`) to an
  internal `queue:memory` or `queue:repository` profile, instead of inline shell (`if`/`then`/`fi`,
  `$(cat)`, `$((...))`). All user-controlled values (`name`, `id`, `db`, `group`, ...) are passed
  through `value()`/`param()`, never interpolated raw into a shell string, so a queue/message name
  containing shell metacharacters can no longer execute a second command — the worst case is a
  rejected/invalid identifier. `nack`'s attempts counter is now bumped server-side via
  `aux4/repository`'s new `patch --increment` (no more shell `$((attempts + 1))`); `--attempts` is
  still accepted for backward compatibility but ignored. No behavior change for `--storage memory`
  (unchanged Go binary calls) or for the `--storage repository` public CLI surface. `aux4 lint run
  --strict true` is clean (no `unsafe-*-interpolation`). 1 new test (shell-metacharacter safety); all
  35 previously-shipped tests pass unmodified.

# aux4/queue 0.2.0

## Added

- Work-queue mode (`create --workQueue true --maxAttempts <n>`): visibility-timeout `lease`, `ack`,
  `nack` (immediate redelivery with an attempts counter), automatic dead-lettering once
  `maxAttempts` is reached (`queue dlq`), and a `--group <key>` on `send` to serialize delivery of
  everything sharing that key (e.g. one chat conversation processed at a time) while unrelated
  groups still lease concurrently. Pre-existing pub/sub queues (no `--workQueue`) are completely
  unaffected — all 17 previously-shipped tests stay green unchanged.
- `--storage memory|repository` on every queue command (`start`, `create`, `delete`, `list`,
  `send`, `lease`, `ack`, `nack`, `dlq`). `memory` (default) is the existing in-process server,
  now volatile-but-fast with the work-queue primitives added above. `repository` is durable: no
  server process is started at all — every command talks directly to a SQLite (or Turso/libSQL)
  database via `aux4/repository`, so messages survive a restart (there's nothing to restart).
  Durability on the repository side is built on a new generic primitive, `aux4 repository lease`
  (added to `aux4/repository`), not on any queue-specific database code.

## Notes

- `aux4/repository` is now a declared dependency (used only by `--storage repository`).
- Repository storage models a dead-letter queue as a query (`attempts >= maxAttempts`) over the
  same messages table rather than a physical move to a second table — simpler, and avoids a
  write-then-delete race between `nack` and `dlq`.

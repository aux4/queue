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

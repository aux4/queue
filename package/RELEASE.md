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

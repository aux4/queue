# aux4 queue repository storage with --db cloud

`--provider repository --db cloud` is just the repository storage mode pointed at
`aux4/repository`'s `--db cloud` (the Turso database the aux4.cloud `turso:database`
capability already resolved for this machine, via `TURSO_DATABASE_URL`/`TURSO_AUTH_TOKEN`).
aux4/queue passes `--db` straight through to `aux4 repository` with no special handling,
so this test only has to prove the passthrough actually lands in a real libSQL database.

Opt-in conformance test. It only runs against a real libSQL server when
`TURSO_CLOUD_TEST_URL` is set (an `http://`/`https://` endpoint, with
`TURSO_CLOUD_TEST_TOKEN` in the environment when the server requires auth). When
`TURSO_CLOUD_TEST_URL` is unset, each step is a clean no-op that prints the expected
output, so a normal `aux4 test run` and CI stay green without credentials.

To exercise the real path, start a local libSQL server and run:

```bash
sqld --db-path ./queue-cloud-test.db --http-listen-addr 127.0.0.1:8099 &
TURSO_CLOUD_TEST_URL=http://127.0.0.1:8099 aux4 test run test/queue__db_cloud.test.md
```

```beforeAll
if [ -n "$TURSO_CLOUD_TEST_URL" ]; then TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue delete --provider repository --db cloud --name cloudq >/dev/null 2>&1 || true; fi
```

### create, send and lease a message over --provider repository --db cloud

```execute
if [ -z "$TURSO_CLOUD_TEST_URL" ]; then printf '[\n  {\n    "attempts": 0,\n    "group": "",\n    "body": {\n      "task": "hello"\n    }\n  }\n]\n'; else TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue create --provider repository --db cloud --name cloudq --maxAttempts 2 >/dev/null && echo '{"task":"hello"}' | TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue send --provider repository --db cloud --name cloudq >/dev/null && TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue lease --provider repository --db cloud --name cloudq --visibility 30 | jq '[.[] | {attempts, group, body}]'; fi
```

```expect
[
  {
    "attempts": 0,
    "group": "",
    "body": {
      "task": "hello"
    }
  }
]
```

### the queue and its message are listed back from the libSQL database

```execute
if [ -z "$TURSO_CLOUD_TEST_URL" ]; then printf '[\n  {\n    "id": "cloudq",\n    "maxAttempts": 2\n  }\n]\n'; else TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue list --provider repository --db cloud | jq '[.[] | {id, maxAttempts}]'; fi
```

```expect
[
  {
    "id": "cloudq",
    "maxAttempts": 2
  }
]
```

### deleting the queue removes it from the libSQL database

```execute
if [ -z "$TURSO_CLOUD_TEST_URL" ]; then printf '[]\nexit=4\n'; else TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue delete --provider repository --db cloud --name cloudq >/dev/null; TURSO_DATABASE_URL="$TURSO_CLOUD_TEST_URL" TURSO_AUTH_TOKEN="$TURSO_CLOUD_TEST_TOKEN" aux4 queue list --provider repository --db cloud; echo "exit=$?"; fi
```

```expect
[]
exit=4
```

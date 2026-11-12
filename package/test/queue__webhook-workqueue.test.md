# aux4 queue webhook push on a work queue (retry-safe)

A work queue with `--webhook` configured pushes each message through the same lease/ack/nack
machinery a pull consumer would use: a 2xx response acks it, anything else (non-2xx or a
timeout/connection error) nacks it -- redelivered, attempts incremented, and dead-lettered once
`maxAttempts` is reached. This is the retry-safety a pub/sub `--webhook` proxy (fire-and-forget,
unaffected by this change) cannot offer.

The webhook target is a tiny `aux4 api` route, not `aux4 mock` -- combining `aux4 mock`'s own
detached server with a `nohup`-backgrounded daemon already running in the same test harness
process hangs the harness (unrelated to this feature); plain `aux4 api` does not have that issue.

````beforeAll
nohup aux4 queue start --port 18450 >/dev/null 2>&1 &
nohup aux4 api start --port 18451 --api '{"POST /fail":{"command":"false"},"POST /ok":{"command":"true"}}' >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18450
pkill -f "18451" 2>/dev/null || true
````

### a failing webhook (non-2xx) is retried and eventually dead-lettered, never silently dropped

````execute
aux4 queue create --storage memory --port 18450 --name pushq1 --workQueue true --maxAttempts 2 --webhook "http://localhost:18451/fail" >/dev/null
echo '{"event":"ping"}' | aux4 queue send --storage memory --port 18450 --name pushq1 >/dev/null
sleep 1
aux4 queue dlq --storage memory --port 18450 --name pushq1 | jq '[.[] | {attempts, body}]'
````

````expect
[
  {
    "attempts": 2,
    "body": {
      "event": "ping"
    }
  }
]
````

### a succeeding webhook (2xx) acks the message -- never redelivered, never dead-lettered

````execute
aux4 queue create --storage memory --port 18450 --name pushq2 --workQueue true --webhook "http://localhost:18451/ok" >/dev/null
echo '{"event":"ping"}' | aux4 queue send --storage memory --port 18450 --name pushq2 >/dev/null
sleep 1
aux4 queue dlq --storage memory --port 18450 --name pushq2
aux4 queue lease --storage memory --port 18450 --name pushq2 --visibility 5
echo "exit=$?"
````

````expect
[]
{}
exit=4
````

### a non-work queue's webhook proxy stays fire-and-forget (pub/sub, unaffected by this change)

````execute
aux4 queue create --storage memory --port 18450 --name pubsubq --webhook "http://localhost:18451/fail" >/dev/null
echo '{"event":"ping"}' | aux4 queue send --storage memory --port 18450 --name pubsubq >/dev/null
echo done
````

````expect
done
````

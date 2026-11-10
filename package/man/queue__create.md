#### Description

Create a new queue on the server. The queue must have a unique name.

Optionally, configure a WebSocket URL (`--ws`) and/or webhook URL (`--webhook`) to proxy every published message to an external endpoint. Use `--header` to attach custom HTTP headers to the proxy requests.

#### Usage

```bash
aux4 queue create --name <queue-name>
aux4 queue create --name <queue-name> --token <token>
aux4 queue create --name <queue-name> --webhook <url>
aux4 queue create --name <queue-name> --ws <url>
aux4 queue create --name <queue-name> --webhook <url> --header "Authorization: Bearer token"
```

#### Example

```bash
aux4 queue create --name jobs
```
```text
{"name":"jobs","status":"CREATED"}
```

#### Example with webhook proxy

```bash
aux4 queue create --name events --webhook https://api.example.com/ingest --header "Authorization: Bearer tok123"
```
```text
{"name":"events","status":"CREATED"}
```

#### Example with WebSocket proxy

```bash
aux4 queue create --name logs --ws wss://stream.example.com/logs
```
```text
{"name":"logs","status":"CREATED"}
```

#### Work queue mode

Pass `--workQueue true` to opt this queue into ack/nack/lease semantics instead of pub/sub fan-out
(memory storage only — a queue created with `--storage repository` is always a work queue, there is
no pub/sub concept without a server process). `--maxAttempts` caps delivery attempts before a
message is dead-lettered (`0`, the default, means unlimited).

```bash
aux4 queue create --name jobs --workQueue true --maxAttempts 5
aux4 queue create --storage repository --db .queue.db --name jobs --maxAttempts 5
```

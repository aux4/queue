#### Description

Start the in-memory queue server. The server holds all queues and routes messages between producers and consumers over HTTP.

Optionally, pass `--queues` with a JSON array to pre-configure queues on startup. Each entry can include WebSocket and webhook proxy URLs so that every published message is automatically forwarded.

#### Usage

```bash
aux4 queue start
aux4 queue start --port 9000
aux4 queue start --port 8420 --token mysecret
aux4 queue start --queues '[{"name":"jobs","webhook":"https://example.com/hook"}]'
```

#### Queues JSON Format

The `--queues` parameter accepts a JSON array of objects with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Queue name (required) |
| `ws` | string | WebSocket URL to proxy messages to |
| `webhook` | string | Webhook URL to proxy messages to |
| `header` | string[] | HTTP headers for WS/webhook (format: `Key: Value`) |

#### Example

```bash
aux4 queue start --token s3cret
```
```text
queue server started on port 8420 (token required)
```

#### Example with pre-configured queues

```bash
aux4 queue start --queues '[{"name":"events","webhook":"https://api.example.com/ingest","header":["Authorization: Bearer tok123"]},{"name":"logs","ws":"wss://stream.example.com/logs"}]'
```

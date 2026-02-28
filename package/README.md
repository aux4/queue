# aux4/queue

In-memory message queue with pub/sub support for aux4.

## Install

```bash
aux4 install aux4/queue
```

## Usage

### Start the server

```bash
aux4 queue start
aux4 queue start --port 9000 --token mysecret
```

### Create a queue

```bash
aux4 queue create --name jobs
```

### Send messages

```bash
echo '{"task":"build"}' | aux4 queue send --name jobs
cat events.jsonl | aux4 queue send --name jobs
```

### Receive messages

```bash
aux4 queue receive --name jobs
aux4 queue receive --name jobs --limit 10
aux4 queue receive --name jobs | jq .
```

### Stop the server

```bash
aux4 queue stop
aux4 queue stop --port 9000
```

### List queues

```bash
aux4 queue list
```

### Delete a queue

```bash
aux4 queue delete --name jobs
```

## Proxy (WebSocket & Webhook)

Queues can be configured to automatically forward every published message to a WebSocket or webhook endpoint. This enables real-time integration with external systems.

### Create a queue with webhook proxy

Every message sent to this queue will also be POSTed to the webhook URL.

```bash
aux4 queue create --name events --webhook https://api.example.com/ingest
```

### Create a queue with WebSocket proxy

Every message sent to this queue will also be written to the WebSocket connection.

```bash
aux4 queue create --name logs --ws wss://stream.example.com/logs
```

### Custom headers

Attach HTTP headers to proxy requests (applied to both WebSocket handshake and webhook POST).

```bash
aux4 queue create --name secure-events \
  --webhook https://api.example.com/ingest \
  --header "Authorization: Bearer tok123"
```

### Pre-configured queues on startup

Use `--queues` to create queues with proxy settings when the server starts.

```bash
aux4 queue start --queues '[
  {"name": "events", "webhook": "https://api.example.com/ingest", "header": ["Authorization: Bearer tok123"]},
  {"name": "logs", "ws": "wss://stream.example.com/logs"}
]'
```

## Authentication

Pass `--token` to `start` to require authentication. All clients must then include the same `--token`.

```bash
aux4 queue start --token mysecret
aux4 queue create --name jobs --token mysecret
```

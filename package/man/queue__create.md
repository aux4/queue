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

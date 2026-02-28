#### Description

Receive messages from a queue. Streams messages to stdout as newline-delimited JSON (NDJSON). The connection stays open until interrupted or the limit is reached. Multiple consumers can receive from the same queue simultaneously; each gets a copy of every message.

Use `--limit` to stop after receiving a specific number of messages. Default is `-1` (unlimited).

#### Usage

```bash
aux4 queue receive --name <queue-name>
aux4 queue receive --name <queue-name> --limit <number>
aux4 queue receive --name <queue-name> --token <token>
aux4 queue receive --name <queue-name> | jq .
```

#### Example

```bash
aux4 queue receive --name jobs --limit 3
```
```text
{"task":"build","project":"api"}
{"task":"test","project":"api"}
{"task":"deploy","project":"api"}
```

#### Description

Send JSON messages to a queue. Reads from stdin, each line must be valid JSON. Messages are delivered to all connected consumers (fan-out). Supports piping from other commands.

#### Usage

```bash
echo '{"key":"value"}' | aux4 queue send --name <queue-name>
cat messages.jsonl | aux4 queue send --name <queue-name>
some-command | aux4 queue send --name <queue-name> --token <token>
```

#### Example

```bash
echo '{"task":"build","project":"api"}' | aux4 queue send --name jobs
```
```text
{"errors":0,"sent":1}
```

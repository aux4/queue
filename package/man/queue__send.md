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

#### Work queue mode

On a work queue (`create --workQueue true`, or any queue under `--storage repository`), pass
`--group <key>` to serialize delivery of everything sharing that key (e.g. a chat id) — only one
message per group is ever leased at a time. `--id` sets the message id explicitly (memory storage
only; auto-generated when omitted).

```bash
echo '{"text":"hi"}' | aux4 queue send --name inbox --group chat-42
echo '{"text":"hi"}' | aux4 queue send --storage repository --db .queue.db --name inbox --group chat-42
```

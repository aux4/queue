#### Description

Stop a running queue server. Reads the PID file and sends a termination signal to the server process.

#### Usage

```bash
aux4 queue stop
aux4 queue stop --port 9000
```

#### Example

```bash
aux4 queue stop --port 8420
```
```text
{"status":"STOPPED","port":"8420","pid":12345}
```

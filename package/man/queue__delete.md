#### Description

Delete a queue from the server. All connected consumers are disconnected.

#### Usage

```bash
aux4 queue delete --name <queue-name>
aux4 queue delete --name <queue-name> --token <token>
```

#### Example

```bash
aux4 queue delete --name jobs
```
```text
{"name":"jobs","status":"DELETED"}
```

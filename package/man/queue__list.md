#### Description

List all queues on the server with their consumer count.

#### Usage

```bash
aux4 queue list
aux4 queue list --token <token>
```

#### Example

```bash
aux4 queue list
```
```text
[{"consumers":0,"name":"jobs"},{"consumers":2,"name":"events"}]
```

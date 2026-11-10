# aux4 queue work queue (repository storage)

Repository storage never runs a server process -- every command below talks directly to a sqlite
file, so `queue start`/`stop` are no-ops and durability "across a restart" is implicit (there is no
process to restart; the data already lives in the file).

````beforeEach
rm -f .queue.db
aux4 queue start --storage repository --db .queue.db >/dev/null
aux4 queue create --storage repository --db .queue.db --name wq --maxAttempts 2 >/dev/null
````

````afterEach
aux4 queue delete --storage repository --db .queue.db --name wq >/dev/null 2>/dev/null
rm -f .queue.db
````

### should lease a sent message

````execute
echo '{"task":"hello"}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30 | jq '[.[] | {attempts, group, body}]'
````

````expect
[
  {
    "attempts": 0,
    "group": "",
    "body": {
      "task": "hello"
    }
  }
]
````

### should not lease the same message again while its lease is active

````execute
echo '{"task":"hello"}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30 >/dev/null
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30
echo "exit=$?"
````

````expect
[]
exit=4
````

### should ack a leased message, removing it for good

````execute
echo '{"task":"once"}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
LEASE=$(aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30)
ID=$(echo "$LEASE" | jq -r '.[0].id')
aux4 queue ack --storage repository --db .queue.db --name wq --id "$ID"
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30
echo "exit=$?"
````

````expect
[]
exit=4
````

### should nack a leased message for immediate redelivery, bumping attempts

````execute
echo '{"task":"retry"}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
LEASE=$(aux4 queue lease --storage repository --db .queue.db --name wq --visibility 300)
ID=$(echo "$LEASE" | jq -r '.[0].id')
aux4 queue nack --storage repository --db .queue.db --name wq --id "$ID" --attempts 0 >/dev/null
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30 --maxAttempts 2 | jq '[.[] | {attempts}]'
````

````expect
[
  {
    "attempts": 1
  }
]
````

### should become a dead-letter once maxAttempts is reached, and stop being leasable

````execute
echo '{"task":"doomed"}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
LEASE1=$(aux4 queue lease --storage repository --db .queue.db --name wq --visibility 300 --maxAttempts 2)
ID1=$(echo "$LEASE1" | jq -r '.[0].id')
aux4 queue nack --storage repository --db .queue.db --name wq --id "$ID1" --attempts 0 >/dev/null
LEASE2=$(aux4 queue lease --storage repository --db .queue.db --name wq --visibility 300 --maxAttempts 2)
ID2=$(echo "$LEASE2" | jq -r '.[0].id')
aux4 queue nack --storage repository --db .queue.db --name wq --id "$ID2" --attempts 1 >/dev/null
aux4 queue dlq --storage repository --db .queue.db --name wq --maxAttempts 2 | jq '[.[] | {attempts, body}]'
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30 --maxAttempts 2
echo "exit=$?"
````

````expect
[
  {
    "attempts": 2,
    "body": {
      "task": "doomed"
    }
  }
]
[]
exit=4
````

### group serialization: only one message per group is leased at a time

Repository timestamps are second-granularity, so the two chatA messages may tie on createdAt --
either can legitimately win the FIFO tiebreak. What must hold is group exclusivity: only one of the
two chatA messages is ever leased while the other is still outstanding, and the unrelated chatB
message leases independently.

````execute
echo '{"n":1}' | aux4 queue send --storage repository --db .queue.db --name wq --group chatA >/dev/null
echo '{"n":2}' | aux4 queue send --storage repository --db .queue.db --name wq --group chatA >/dev/null
echo '{"n":3}' | aux4 queue send --storage repository --db .queue.db --name wq --group chatB >/dev/null
FIRST=$(aux4 queue lease --storage repository --db .queue.db --name wq --visibility 300)
SECOND=$(aux4 queue lease --storage repository --db .queue.db --name wq --visibility 300)
echo "$FIRST" | jq '[.[] | .group]'
echo "$SECOND" | jq '[.[] | .group]'
````

````expect
[
  "chatA"
]
[
  "chatB"
]
````

### durability: a message sent before stays leasable after a simulated restart

Repository storage keeps no server process, so a "restart" is just the next command invocation
against the same database file.

````execute
echo '{"task":"survives"}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
aux4 queue stop --storage repository --db .queue.db >/dev/null
aux4 queue start --storage repository --db .queue.db >/dev/null
aux4 queue lease --storage repository --db .queue.db --name wq --visibility 30 | jq '[.[] | {body}]'
````

````expect
[
  {
    "body": {
      "task": "survives"
    }
  }
]
````

### concurrency: concurrent receivers never get the same message

````execute
echo '{"n":1}' | aux4 queue send --storage repository --db .queue.db --name wq >/dev/null
for i in 1 2 3 4 5 6 7 8 9 10; do
  (aux4 queue lease --storage repository --db .queue.db --name wq --visibility 300 2>/dev/null | grep -q '"id"' && echo "$i" >> /tmp/queue-workqueue-repo-winners.txt) &
done
wait
wc -l < /tmp/queue-workqueue-repo-winners.txt | tr -d ' '
rm -f /tmp/queue-workqueue-repo-winners.txt
````

````expect
1
````

### listing and deleting a queue

````execute
aux4 queue list --storage repository --db .queue.db | jq '[.[] | {id, maxAttempts}]'
aux4 queue delete --storage repository --db .queue.db --name wq
aux4 queue list --storage repository --db .queue.db
echo "exit=$?"
````

````expect
[
  {
    "id": "wq",
    "maxAttempts": 2
  }
]
[]
exit=4
````

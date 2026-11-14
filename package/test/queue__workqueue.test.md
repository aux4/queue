# aux4 queue work queue (memory storage)

````beforeAll
nohup aux4 queue start --port 18430 >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18430
````

## lease, ack and nack

````beforeEach
aux4 queue create --name wq1 --port 18430 --workQueue true --maxAttempts 2
````

````afterEach
aux4 queue delete --name wq1 --port 18430
````

### should lease a sent message

````execute
echo '{"task":"hello"}' | aux4 queue send --name wq1 --port 18430 >/dev/null
aux4 queue lease --name wq1 --port 18430 --visibility 30 | jq '{id, attempts, group, body}'
````

````expect
{
  "id": "msg-1",
  "attempts": 0,
  "group": "",
  "body": {
    "task": "hello"
  }
}
````

### should not lease the same message again while its lease is active

````execute
echo '{"task":"hello"}' | aux4 queue send --name wq1 --port 18430 >/dev/null
aux4 queue lease --name wq1 --port 18430 --visibility 30 >/dev/null
aux4 queue lease --name wq1 --port 18430 --visibility 30
echo "exit=$?"
````

````expect
{}
exit=4
````

### should ack a leased message, removing it for good

````execute
echo '{"task":"once"}' | aux4 queue send --name wq1 --port 18430 >/dev/null
LEASE=$(aux4 queue lease --name wq1 --port 18430 --visibility 30)
LEASEID=$(echo "$LEASE" | jq -r .leaseId)
aux4 queue ack --name wq1 --port 18430 --leaseId "$LEASEID" | jq .
aux4 queue lease --name wq1 --port 18430 --visibility 30
echo "exit=$?"
````

````expect
{
  "status": "ACKED"
}
{}
exit=4
````

### should nack a leased message for immediate redelivery, bumping attempts

````execute
echo '{"task":"retry"}' | aux4 queue send --name wq1 --port 18430 >/dev/null
LEASE=$(aux4 queue lease --name wq1 --port 18430 --visibility 300)
LEASEID=$(echo "$LEASE" | jq -r .leaseId)
aux4 queue nack --name wq1 --port 18430 --leaseId "$LEASEID" | jq .
aux4 queue lease --name wq1 --port 18430 --visibility 30 | jq '{attempts}'
````

````expect
{
  "status": "NACKED"
}
{
  "attempts": 1
}
````

### should move a message to the dead-letter queue once maxAttempts is reached

````execute
echo '{"task":"doomed"}' | aux4 queue send --name wq1 --port 18430 >/dev/null
LEASE1=$(aux4 queue lease --name wq1 --port 18430 --visibility 300)
LEASEID1=$(echo "$LEASE1" | jq -r .leaseId)
aux4 queue nack --name wq1 --port 18430 --leaseId "$LEASEID1" >/dev/null
LEASE2=$(aux4 queue lease --name wq1 --port 18430 --visibility 300)
LEASEID2=$(echo "$LEASE2" | jq -r .leaseId)
aux4 queue nack --name wq1 --port 18430 --leaseId "$LEASEID2" | jq .
aux4 queue dlq --name wq1 --port 18430 | jq '[.[] | {attempts, body}]'
````

````expect
{
  "status": "DEAD_LETTERED"
}
[
  {
    "attempts": 2,
    "body": {
      "task": "doomed"
    }
  }
]
````

### should expose a visibility timeout -- an expired lease is eligible again

````execute
echo '{"task":"expiring"}' | aux4 queue send --name wq1 --port 18430 >/dev/null
aux4 queue lease --name wq1 --port 18430 --visibility 1 >/dev/null
sleep 2
aux4 queue lease --name wq1 --port 18430 --visibility 30 | jq '{attempts, body}'
````

````expect
{
  "attempts": 0,
  "body": {
    "task": "expiring"
  }
}
````

## group serialization

````beforeEach
aux4 queue create --name wq2 --port 18430 --workQueue true
````

````afterEach
aux4 queue delete --name wq2 --port 18430
````

### should lease only one message per group at a time

````execute
echo '{"n":1}' | aux4 queue send --name wq2 --port 18430 --group chatA >/dev/null
echo '{"n":2}' | aux4 queue send --name wq2 --port 18430 --group chatA >/dev/null
echo '{"n":3}' | aux4 queue send --name wq2 --port 18430 --group chatB >/dev/null
aux4 queue lease --name wq2 --port 18430 --visibility 300 | jq '{group, body}'
aux4 queue lease --name wq2 --port 18430 --visibility 300 | jq '{group, body}'
````

````expect
{
  "group": "chatA",
  "body": {
    "n": 1
  }
}
{
  "group": "chatB",
  "body": {
    "n": 3
  }
}
````

## concurrency

````beforeEach
aux4 queue create --name wq3 --port 18430 --workQueue true
````

````afterEach
aux4 queue delete --name wq3 --port 18430
````

### concurrent receivers never get the same message

````execute
echo '{"n":1}' | aux4 queue send --name wq3 --port 18430 >/dev/null
for i in 1 2 3 4 5 6 7 8 9 10; do
  (aux4 queue lease --name wq3 --port 18430 --visibility 300 2>/dev/null | grep -q '"id"' && echo "$i" >> /tmp/queue-workqueue-winners.txt) &
done
wait
wc -l < /tmp/queue-workqueue-winners.txt | tr -d ' '
rm -f /tmp/queue-workqueue-winners.txt
````

````expect
1
````

## backward compatibility

### a queue created without --workQueue keeps the original pub/sub behavior

````execute
aux4 queue create --name legacy --port 18430
echo '{"fanout":true}' | aux4 queue send --name legacy --port 18430 | jq .
aux4 queue lease --name legacy --port 18430 --visibility 30
aux4 queue delete --name legacy --port 18430
````

````expect
{"name":"legacy","status":"CREATED"}
{
  "errors": 0,
  "sent": 1
}
{"name":"legacy","status":"DELETED"}
````

````error:partial
*?is not a work queue*?
````

## hot-path dispatch has no install-check probe

`ack`/`nack` (like `renew`) skip the provider-install-check probe that `create`/`send`/`lease`/
`list`/`dlq` still run -- an unknown provider falls straight through to aux4's own generic error
instead of the friendly `install aux4/queue-<provider>` message.

### ack with an unknown provider fails with aux4's generic error, not the friendly install message

````execute
aux4 queue ack --provider bogus --name wq1 --leaseId x
````

````error:partial
Command not found*?
````

### nack with an unknown provider fails with aux4's generic error, not the friendly install message

````execute
aux4 queue nack --provider bogus --name wq1 --leaseId x
````

````error:partial
Command not found*?
````

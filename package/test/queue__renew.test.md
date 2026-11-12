# aux4 queue renew

Extending a still-live lease (a heartbeat) so a slow consumer is not redelivered to a second worker
while it is still processing.

## memory storage

````beforeAll
nohup aux4 queue start --port 18440 >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18440
````

````beforeEach
aux4 queue delete --storage memory --port 18440 --name renewq >/dev/null 2>&1 || true
aux4 queue create --storage memory --port 18440 --name renewq --workQueue true
echo '{"task":"slow"}' | aux4 queue send --storage memory --port 18440 --name renewq >/dev/null
````

### renewing before the original visibility expires keeps the lease held (not redelivered)

````execute
LEASE=$(aux4 queue lease --storage memory --port 18440 --name renewq --visibility 1)
LEASE_ID=$(echo "$LEASE" | jq -r '.leaseId')
aux4 queue renew --storage memory --port 18440 --name renewq --leaseId "$LEASE_ID" --visibility 5 >/dev/null
sleep 1.2
aux4 queue lease --storage memory --port 18440 --name renewq --visibility 1
echo "exit=$?"
````

````expect
{}
exit=4
````

### renewing a lease that already expired fails (not resurrected)

````execute
LEASE=$(aux4 queue lease --storage memory --port 18440 --name renewq --visibility 1)
LEASE_ID=$(echo "$LEASE" | jq -r '.leaseId')
sleep 1.2
aux4 queue renew --storage memory --port 18440 --name renewq --leaseId "$LEASE_ID" --visibility 5
echo "exit=$?"
````

````expect
{}
exit=4
````

## repository storage

````beforeEach
rm -f .renew.queue.db
aux4 queue start --storage repository --db .renew.queue.db >/dev/null
aux4 queue create --storage repository --db .renew.queue.db --name renewq --maxAttempts 2 >/dev/null
echo '{"task":"slow"}' | aux4 queue send --storage repository --db .renew.queue.db --name renewq >/dev/null
````

````afterEach
rm -f .renew.queue.db
````

### renewing before the original visibility expires keeps the lease held (not redelivered)

````execute
LEASE=$(aux4 queue lease --storage repository --db .renew.queue.db --name renewq --visibility 1)
ID=$(echo "$LEASE" | jq -r '.[0].id')
aux4 queue renew --storage repository --db .renew.queue.db --name renewq --id "$ID" --visibility 5 >/dev/null
sleep 1.2
aux4 queue lease --storage repository --db .renew.queue.db --name renewq --visibility 1
echo "exit=$?"
````

````expect
[]
exit=4
````

### renewing a lease that already expired fails (not resurrected)

````execute
LEASE=$(aux4 queue lease --storage repository --db .renew.queue.db --name renewq --visibility 1)
ID=$(echo "$LEASE" | jq -r '.[0].id')
sleep 1.2
aux4 queue renew --storage repository --db .renew.queue.db --name renewq --id "$ID" --visibility 5
````

````error:regex
^(\[\]|)$
````

# aux4 queue proxy

````beforeAll
nohup aux4 queue start --port 18430 >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18430
````

## webhook proxy

````beforeAll
aux4 queue create --name target --port 18430
aux4 queue create --name source --port 18430 --webhook "http://localhost:18430/send?name=target"
````

### should proxy message to webhook

````execute
aux4 queue receive --name target --port 18430 --limit 1 > /tmp/aux4-queue-proxy.out &
RECV_PID=$!
sleep 0.5
echo '{"event":"test"}' | aux4 queue send --name source --port 18430 > /dev/null
sleep 1
kill $RECV_PID 2>/dev/null || true
cat /tmp/aux4-queue-proxy.out | jq .
rm -f /tmp/aux4-queue-proxy.out
````

````expect
{
  "event": "test"
}
````

## pre-configured queues with webhook

````beforeAll
aux4 queue create --name preconfigured-target --port 18430
nohup aux4 queue start --port 18431 --queues '[{"name":"auto","webhook":"http://localhost:18430/send?name=preconfigured-target"}]' >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18431
````

### should proxy from pre-configured queue

````execute
aux4 queue receive --name preconfigured-target --port 18430 --limit 1 > /tmp/aux4-queue-proxy2.out &
RECV_PID=$!
sleep 0.5
echo '{"auto":"message"}' | aux4 queue send --name auto --port 18431 > /dev/null
sleep 1
kill $RECV_PID 2>/dev/null || true
cat /tmp/aux4-queue-proxy2.out | jq .
rm -f /tmp/aux4-queue-proxy2.out
````

````expect
{
  "auto": "message"
}
````

# aux4 queue receive

````beforeAll
nohup aux4 queue start --port 18426 >/dev/null 2>&1 &
sleep 1
aux4 queue create --name inbox --port 18426
````

````afterAll
aux4 queue stop --port 18426
````

## receive messages

### should receive a single message

````execute
aux4 queue receive --name inbox --port 18426 > /tmp/aux4-queue-recv.out &
RECV_PID=$!
sleep 0.5
echo '{"msg":"hello"}' | aux4 queue send --name inbox --port 18426 > /dev/null
sleep 0.5
kill $RECV_PID 2>/dev/null || true
cat /tmp/aux4-queue-recv.out | jq .
rm -f /tmp/aux4-queue-recv.out
````

````expect
{
  "msg": "hello"
}
````

### should receive multiple messages

````execute
aux4 queue receive --name inbox --port 18426 > /tmp/aux4-queue-recv.out &
RECV_PID=$!
sleep 0.5
printf '{"id":1}\n{"id":2}\n{"id":3}\n' | aux4 queue send --name inbox --port 18426 > /dev/null
sleep 0.5
kill $RECV_PID 2>/dev/null || true
cat /tmp/aux4-queue-recv.out | jq .
rm -f /tmp/aux4-queue-recv.out
````

````expect
{
  "id": 1
}
{
  "id": 2
}
{
  "id": 3
}
````

## receive with limit

### should stop after receiving limit messages

````execute
aux4 queue receive --name inbox --port 18426 --limit 2 > /tmp/aux4-queue-recv.out &
RECV_PID=$!
sleep 0.5
printf '{"id":1}\n{"id":2}\n{"id":3}\n' | aux4 queue send --name inbox --port 18426 > /dev/null
sleep 0.5
kill $RECV_PID 2>/dev/null || true
cat /tmp/aux4-queue-recv.out | jq .
rm -f /tmp/aux4-queue-recv.out
````

````expect
{
  "id": 1
}
{
  "id": 2
}
````

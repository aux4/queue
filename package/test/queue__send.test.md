# aux4 queue send

````beforeAll
nohup aux4 queue start --port 18424 >/dev/null 2>&1 &
sleep 1
aux4 queue create --name outbox --port 18424
````

````afterAll
aux4 queue stop --port 18424
````

## send messages

### should send a message

````execute
echo '{"hello":"world"}' | aux4 queue send --name outbox --port 18424 | jq .
````

````expect
{
  "errors": 0,
  "sent": 1
}
````

### should send multiple messages

````execute
printf '{"a":1}\n{"b":2}\n{"c":3}\n' | aux4 queue send --name outbox --port 18424 | jq .
````

````expect
{
  "errors": 0,
  "sent": 3
}
````

### should skip invalid JSON

````execute
printf '{"valid":true}\nnot json\n{"also":true}\n' | aux4 queue send --name outbox --port 18424 | jq .
````

````expect
{
  "errors": 1,
  "sent": 2
}
````

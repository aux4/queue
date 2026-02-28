# aux4 queue create

````beforeAll
nohup aux4 queue start --port 18420 >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18420
````

## create a queue

### should create a queue successfully

````execute
aux4 queue create --name myqueue --port 18420 | jq .
````

````expect
{
  "name": "myqueue",
  "status": "CREATED"
}
````

### should fail to create a duplicate queue

````execute
aux4 queue create --name myqueue --port 18420
````

````error:partial
already exists
````

## create with token

````beforeAll
nohup aux4 queue start --port 18421 --token testtoken >/dev/null 2>&1 &
sleep 1
````

````afterAll
aux4 queue stop --port 18421
````

### should fail without token

````execute
aux4 queue create --name secured --port 18421
````

````error:partial
invalid token
````

### should create with valid token

````execute
aux4 queue create --name secured --port 18421 --token testtoken | jq .
````

````expect
{
  "name": "secured",
  "status": "CREATED"
}
````

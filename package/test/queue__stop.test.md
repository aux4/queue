# aux4 queue stop

## stop a running server

````beforeAll
nohup aux4 queue start --port 18425 >/dev/null 2>&1 &
sleep 1
````

### should stop the server

````execute
aux4 queue stop --port 18425 | jq .
````

````expect:partial
{
  "status": "STOPPED",
  "port": "18425",
  "pid": *?
}
````

## stop a non-running server

### should fail when server is not running

````execute
aux4 queue stop --port 19999
````

````error:partial
not running
````

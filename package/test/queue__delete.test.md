# aux4 queue delete

````beforeAll
nohup aux4 queue start --port 18423 >/dev/null 2>&1 &
sleep 1
aux4 queue create --name to-delete --port 18423
````

````afterAll
aux4 queue stop --port 18423
````

## delete a queue

### should delete a queue

````execute
aux4 queue delete --name to-delete --port 18423 | jq .
````

````expect
{
  "name": "to-delete",
  "status": "DELETED"
}
````

### should fail to delete non-existent queue

````execute
aux4 queue delete --name nope --port 18423
````

````error:partial
not found
````

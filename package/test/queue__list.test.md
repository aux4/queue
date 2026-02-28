# aux4 queue list

````beforeAll
nohup aux4 queue start --port 18422 >/dev/null 2>&1 &
sleep 1
aux4 queue create --name alpha --port 18422
aux4 queue create --name beta --port 18422
````

````afterAll
aux4 queue stop --port 18422
````

## list queues

### should list all queues

````execute
aux4 queue list --port 18422 | jq 'sort_by(.name)'
````

````expect
[
  {
    "consumers": 0,
    "name": "alpha"
  },
  {
    "consumers": 0,
    "name": "beta"
  }
]
````

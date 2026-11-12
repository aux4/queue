# aux4 queue repository storage: queue names with dashes or dots

`--storage repository` maps a queue name to a SQL table identifier for its messages. Pre-existing
underscore-only names map unchanged (`<name>_messages`, byte for byte); a name containing any other
character (notably a dash or a dot -- both natural, common queue names, and both already fine under
`--storage memory`) is sanitized and disambiguated with a hash of the original name, so it works
end to end instead of failing with a SQL syntax error.

````beforeEach
rm -f .dashed.queue.db
aux4 queue start --storage repository --db .dashed.queue.db >/dev/null
````

````afterEach
rm -f .dashed.queue.db
````

### an underscore-only name keeps working exactly as before

````execute
aux4 queue create --storage repository --db .dashed.queue.db --name bot_inbox --maxAttempts 1 >/dev/null
echo '{"task":"hi"}' | aux4 queue send --storage repository --db .dashed.queue.db --name bot_inbox >/dev/null
aux4 queue lease --storage repository --db .dashed.queue.db --name bot_inbox --visibility 30 | jq '[.[] | {body}]'
````

````expect
[
  {
    "body": {
      "task": "hi"
    }
  }
]
````

### a dashed queue name works end to end: create, send, lease, ack

````execute
aux4 queue create --storage repository --db .dashed.queue.db --name bot-inbox --maxAttempts 2 >/dev/null
echo '{"task":"dashed"}' | aux4 queue send --storage repository --db .dashed.queue.db --name bot-inbox >/dev/null
LEASE=$(aux4 queue lease --storage repository --db .dashed.queue.db --name bot-inbox --visibility 30)
ID=$(echo "$LEASE" | jq -r '.[0].id')
aux4 queue ack --storage repository --db .dashed.queue.db --name bot-inbox --id "$ID"
aux4 queue lease --storage repository --db .dashed.queue.db --name bot-inbox --visibility 30
echo "exit=$?"
````

````expect
[]
exit=4
````

### a dotted queue name works end to end too

````execute
aux4 queue create --storage repository --db .dashed.queue.db --name bot.inbox --maxAttempts 2 >/dev/null
echo '{"task":"dotted"}' | aux4 queue send --storage repository --db .dashed.queue.db --name bot.inbox >/dev/null
aux4 queue lease --storage repository --db .dashed.queue.db --name bot.inbox --visibility 30 | jq '[.[] | {body}]'
````

````expect
[
  {
    "body": {
      "task": "dotted"
    }
  }
]
````

### dashed and dotted names that sanitize to the same text never collide (distinct tables)

````execute
aux4 queue create --storage repository --db .dashed.queue.db --name shared-key --maxAttempts 1 >/dev/null
aux4 queue create --storage repository --db .dashed.queue.db --name shared.key --maxAttempts 1 >/dev/null
echo '{"src":"dash"}' | aux4 queue send --storage repository --db .dashed.queue.db --name shared-key >/dev/null
echo '{"src":"dot"}' | aux4 queue send --storage repository --db .dashed.queue.db --name shared.key >/dev/null
aux4 queue lease --storage repository --db .dashed.queue.db --name shared-key --visibility 30 | jq '[.[] | {body}]'
aux4 queue lease --storage repository --db .dashed.queue.db --name shared.key --visibility 30 | jq '[.[] | {body}]'
````

````expect
[
  {
    "body": {
      "src": "dash"
    }
  }
]
[
  {
    "body": {
      "src": "dot"
    }
  }
]
````

### nack, dlq, and delete also work on a dashed queue name

````execute
aux4 queue create --storage repository --db .dashed.queue.db --name retry-queue --maxAttempts 1 >/dev/null
echo '{"task":"doomed"}' | aux4 queue send --storage repository --db .dashed.queue.db --name retry-queue >/dev/null
LEASE=$(aux4 queue lease --storage repository --db .dashed.queue.db --name retry-queue --visibility 300 --maxAttempts 1)
ID=$(echo "$LEASE" | jq -r '.[0].id')
aux4 queue nack --storage repository --db .dashed.queue.db --name retry-queue --id "$ID" >/dev/null
aux4 queue dlq --storage repository --db .dashed.queue.db --name retry-queue --maxAttempts 1 | jq '[.[] | {attempts}]'
aux4 queue delete --storage repository --db .dashed.queue.db --name retry-queue >/dev/null
aux4 queue list --storage repository --db .dashed.queue.db | jq '[.[] | select(.id == "retry-queue")]'
````

````expect
[
  {
    "attempts": 1
  }
]
[]
````

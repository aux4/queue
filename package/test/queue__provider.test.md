# aux4 queue provider

## --provider defaults and the --storage alias

### should default to the memory provider when neither --provider nor --storage is given

````execute
aux4 queue create --name providerq1 --port 18460 2>&1
aux4 queue delete --name providerq1 --port 18460 >/dev/null 2>&1
````

````error:partial
*?
````

### should still accept the legacy --storage flag (repository)

````execute
rm -f .provider.queue.db
aux4 queue start --storage repository --db .provider.queue.db
aux4 queue create --storage repository --db .provider.queue.db --name providerq2
aux4 queue list --storage repository --db .provider.queue.db
rm -f .provider.queue.db
````

````expect:partial
providerq2
````

### should accept the new --provider flag (repository)

````execute
rm -f .provider.queue.db
aux4 queue start --provider repository --db .provider.queue.db
aux4 queue create --provider repository --db .provider.queue.db --name providerq3
aux4 queue list --provider repository --db .provider.queue.db
rm -f .provider.queue.db
````

````expect:partial
providerq3
````

## unknown or not-installed providers

### should fail with a clear install message for an unknown provider

````execute
aux4 queue list --provider bogus
````

````error:partial
Error: queue provider bogus is not installed or unknown. Run: aux4 aux4 pkger install aux4/queue-bogus *?
````

### should fail with a clear install message for the reserved cloud provider when its plugin is not installed

````execute
aux4 queue list --provider cloud
````

````error:partial
Error: queue provider cloud is not installed or unknown. Run: aux4 aux4 pkger install aux4/queue-cloud *?
````

# Redis

Redis is a package built on top of redigo, providing a convenient and efficient way to interact with Redis.

## Installation

```shell
go get "github.com/sk-pkg/redis"
```

## Quick Start
```go
import (
	"fmt"
	"github.com/sk-pkg/redis"
	"log"
)

const (
	address = "localhost:6379"
	prefix  = "redisTest"
)

func main() {
	redisManager, err := redis.New(redis.WithAddress(address), redis.WithPrefix(prefix))
	if err != nil {
		log.Fatal(err)
	}

	err = redisManager.Set("key", "value", 10)
	if err != nil {
		log.Fatal(err)
	}

	value, err := redisManager.Get("key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("value:%v \n", string(value))
}
```

## Configuration Options
1. Address: Redis connection address (required)
```go
redis.WithAddress("localhost:6379")
```
2. Password: Redis authentication password (optional)
```go
redis.WithPassword("auth")
```
3. Prefix: Prefix for all keys (optional)
```go
redis.WithPrefix("prefix")
```
4. MaxIdle: Maximum number of idle connections in the pool (default: 30, optional)
```go
redis.WithMaxIdle(10)
```
5. MaxActive: Maximum number of connections allocated by the pool at a given time (default: 100, optional)
```go
redis.WithMaxActive(50)
```
6. DB: Redis database number (default: 0, optional)
```go
redis.WithDB(1)
```
7. IdleTimeout: Timeout for idle connections (default: 30 seconds, optional)
```go
redis.WithIdleTimeout(30 * time.Second)
```
8. Network: Network type for Redis connection (default: "tcp", optional)
```go
redis.WithNetwork("tcp")
```

## Available Methods
1. Set: Store a key-value pair with optional TTL
```go
err := redisManager.Set("key", "value", 10)
```
2. Exists: Check if a key exists
```go
exists, err := redisManager.Exists("key")
```
3. Get: Retrieve the value of a key
```go
value, err := redisManager.Get("key")
```
4. Del: Delete a key
```go
deleted, err := redisManager.Del("key")
```
5. BatchDel: Delete all keys matching a pattern
```go
err := redisManager.BatchDel("key*")
```
6. Ttl: Get the remaining time to live of a key
```go
ttl, err := redisManager.Ttl("key")
```
7. Expire: Set the expiration time for a key
```go
err := redisManager.Expire("key", 10)
```
8. Lua: Execute a Lua script
```go
result, err := redisManager.Lua(keyCount, script, []string{"key1", "key2", "args1", "args2"})
```
9. Zscore: Get the score of a member in a sorted set
```go
score, err := redisManager.Zscore("key", "member")
```
10. Hset: Set the value of a hash field
```go
err := redisManager.Hset("key", "field", "value")
```
11. HgetAll: Get all fields and values in a hash
```go
result, err := redisManager.HgetAll("key")
```
12. Zadd: Add one or more members to a sorted set
```go
result, err := redisManager.Zadd("key", 1, "1")
```
13. Zrange: Get a range of members in a sorted set (sorted by score ascending)
```go
result, err := redisManager.Zrange("key", 0, 1)
```
14. Zrevrange: Get a range of members in a sorted set (sorted by score descending)
```go
result, err := redisManager.Zrevrange("key", 0, 1)
```
15. Zcard: Get the number of members in a sorted set
```go
count, err := redisManager.Zcard("key")
```
16. Zincrby: Increment the score of a member in a sorted set
```go
newScore, err := redisManager.Zincrby("key", 1, "test")
```
17. Zrem: Remove one or more members from a sorted set
```go
removed, err := redisManager.Zrem("key", "member")
```
18. Zrank: Get the rank of a member in a sorted set (0-based, ascending order)
```go
rank, err := redisManager.Zrank("key", "member")
```
19. Hlen: Get the number of fields in a hash
```go
length, err := redisManager.Hlen("key")
```
20. SetString: Set a string value with optional TTL
```go
err := redisManager.SetString("key", "value", 60)
```
21. GetString: Get a string value
```go
value, err := redisManager.GetString("key")
```
22. Do: Execute a raw Redis command
```go
result, err := redisManager.Do("SET", "Key", "Value", "EX", 60)
```
23. Ping: Check the connection to Redis
```go
err := redisManager.Ping()
```

## Using Raw Redigo Methods
```go
redisManager, err := redis.New(redis.WithAddress(address), redis.WithPrefix(prefix))
if err != nil {
    log.Fatal(err)
}

conn := redisManager.ConnPool.Get()
defer conn.Close()

reply, err := conn.Do("EXPIRE", "key", 100)
```

Note: This package uses connection pooling for efficient Redis connections. Always remember to properly handle errors and close connections when using raw Redigo methods.
```
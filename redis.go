// Copyright 2024 Seakee.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

// Package redis provides a Redis client implementation with connection pooling.
package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"reflect"
	"strconv"
	"time"
)

// Default constants for Redis connection pool configuration
const (
	DefaultMaxIdle     = 30               // Maximum number of idle connections in the pool
	DefaultMaxActive   = 100              // Maximum number of connections allocated by the pool at a given time
	DefaultIdleTimeout = 30 * time.Second // Timeout for idle connections in the pool
	DefaultDB          = 0                // Default Redis database number
	DefaultNetwork     = "tcp"            // Default network type for Redis connection
)

// Option is a function type that modifies the option struct
type Option func(*option)

// option represents the configuration options for the Redis client
type option struct {
	network     string
	address     string
	password    string
	prefix      string
	maxIdle     int           // Maximum number of idle connections in the pool
	maxActive   int           // Maximum number of connections allocated by the pool at a given time
	db          int           // Redis database number
	idleTimeout time.Duration // Timeout for idle connections in the pool
}

// Manager represents a Redis client with a connection pool
type Manager struct {
	ConnPool *redis.Pool // Redis connection pool
	Prefix   string      // Prefix added to all keys
}

// ZSetMember represents a member of a sorted set with its score
type ZSetMember struct {
	Score  float64
	Member any
}

// WithAddress sets the Redis server address
func WithAddress(address string) Option {
	return func(o *option) {
		o.address = address
	}
}

// WithNetwork sets the network type for Redis connection
func WithNetwork(network string) Option {
	return func(o *option) {
		o.network = network
	}
}

// WithPassword sets the Redis server password
func WithPassword(password string) Option {
	return func(o *option) {
		o.password = password
	}
}

// WithPrefix sets the key prefix for all Redis operations
func WithPrefix(prefix string) Option {
	return func(o *option) {
		o.prefix = prefix + ":"
	}
}

// WithMaxActive sets the maximum number of connections allocated by the pool at a given time
func WithMaxActive(maxActive int) Option {
	return func(o *option) {
		o.maxActive = maxActive
	}
}

// WithDB sets the Redis database number
func WithDB(db int) Option {
	return func(o *option) {
		o.db = db
	}
}

// WithMaxIdle sets the maximum number of idle connections in the pool
func WithMaxIdle(maxIdle int) Option {
	return func(o *option) {
		o.maxIdle = maxIdle
	}
}

// WithIdleTimeout sets the timeout for idle connections in the pool
func WithIdleTimeout(idleTimeout time.Duration) Option {
	return func(o *option) {
		o.idleTimeout = idleTimeout
	}
}

// New creates a new Redis Manager with the given options
//
// This function initializes a new Redis Manager with a connection pool.
// It applies all provided options and sets default values for unspecified options.
//
// Parameters:
//   - opts: A list of Option functions to configure the Redis Manager
//
// Returns:
//   - *Manager: A new Redis Manager with the given options
//   - error: Any error that occurred during the creation
//
// Example:
//
//	manager, err := New(
//	    WithAddress("localhost:6379"),
//	    WithPassword("secret"),
//	    WithPrefix("myapp"),
//	    WithMaxActive(200),
//	)
func New(opts ...Option) (*Manager, error) {
	opt := &option{
		network:     DefaultNetwork,
		maxIdle:     DefaultMaxIdle,
		maxActive:   DefaultMaxActive,
		db:          DefaultDB,
		idleTimeout: DefaultIdleTimeout,
	}

	for _, f := range opts {
		f(opt)
	}

	// Initialize Redis connection pool
	connPool := &redis.Pool{
		MaxIdle:     opt.maxIdle,
		MaxActive:   opt.maxActive,
		IdleTimeout: opt.idleTimeout,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial(opt.network, opt.address,
				redis.DialPassword(opt.password),
				redis.DialDatabase(opt.db),
			)
			if err != nil {
				return nil, err
			}

			return con, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
		Wait: true,
	}

	m := &Manager{ConnPool: connPool, Prefix: opt.prefix}

	err := m.Ping()

	return m, err
}

// prefixKey adds the key prefix to the given key
//
// Parameters:
//   - key: The key to prefix
//
// Returns:
//   - string: The prefixed key
func (m *Manager) prefixKey(key string) string {
	return m.Prefix + key
}

// Ping sends a PING command to the Redis server and returns an error if the connection fails
// Returns:
//   - error: Any error that occurred during the PING command
func (m *Manager) Ping() error {
	_, err := m.Do("PING")
	return err
}

// Do execute a Redis command with the given arguments
//
// Parameters:
//   - commandName: The name of the Redis command to execute
//   - args: The arguments for the Redis command
//
// Returns:
//   - any: The result of the Redis command
//   - error: Any error that occurred during the execution
//
// Example:
//
//	result, err := manager.Do("SET", "mykey", "myvalue")
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) Do(commandName string, args ...any) (any, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()
	return conn.Do(commandName, args...)
}

// Lua executes a Lua script on the Redis server
//
// Parameters:
//   - keyCount: The number of keys used in the script
//   - script: The Lua script to execute
//   - keysAndArgs: A slice containing the keys and arguments for the script
//
// Returns:
//   - any: The result of the Lua script execution
//   - error: Any error that occurred during the script execution
//
// Example:
//
//	script := `return redis.call('SET', KEYS[1], ARGV[1])`
//	result, err := manager.Lua(1, script, []string{"mykey", "myvalue"})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) Lua(keyCount int, script string, keysAndArgs []string) (any, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{}.AddFlat(keysAndArgs)
	lua := redis.NewScript(keyCount, script)

	return lua.Do(conn, args...)
}

// Exists checks if a key exists in Redis
//
// Parameters:
//   - key: The key to check
//
// Returns:
//   - bool: True if the key exists, false otherwise
//   - error: Any error that occurred during the operation
//
// Example:
//
//	exists, err := manager.Exists("mykey")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key exists: %v\n", exists)
func (m *Manager) Exists(key string) (bool, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Bool(conn.Do("EXISTS", m.prefixKey(key)))
}

// Del deletes a key from Redis
//
// Parameters:
//   - key: The key to delete
//
// Returns:
//   - bool: True if the key was deleted, false if the key did not exist
//   - error: Any error that occurred during the operation
//
// Example:
//
//	deleted, err := manager.Del("mykey")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key deleted: %v\n", deleted)
func (m *Manager) Del(key string) (bool, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Bool(conn.Do("DEL", m.prefixKey(key)))
}

// BatchDel deletes all keys matching a given pattern.
//
// It uses the SCAN command to iterate through keys, which is more efficient
// and less blocking than the KEYS command for large datasets.
//
// Parameters:
//   - pattern: The pattern to match keys for deletion. The function will prepend
//     the Manager's prefix and add wildcards to create the full pattern.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//
// The function uses a cursor-based iteration to scan keys in batches,
// deleting matched keys in each iteration. This approach is more
// memory-efficient and less likely to block the Redis server compared
// to fetching and deleting all keys at once.
//
// Note: This operation may still take a considerable amount of time for
// very large datasets. Consider implementing additional control mechanisms
// (e.g., timeouts, background processing) for extremely large operations.
//
// Example:
//
//	err := manager.BatchDel("user:*")
//	if err != nil {
//	    log.Printf("Failed to batch delete: %v", err)
//	}
func (m *Manager) BatchDel(pattern string) error {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Initialize cursor and create the full pattern
	cursor := 0
	fullPattern := "*" + m.Prefix + pattern + "*"

	// Iterate through all keys matching the pattern
	for {
		// Perform SCAN operation
		reply, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", fullPattern, "COUNT", 100))
		if err != nil {
			return fmt.Errorf("SCAN operation failed: %w", err)
		}

		// Extract cursor and keys from the reply
		cursor, _ = redis.Int(reply[0], nil)
		keys, _ := redis.Strings(reply[1], nil)

		// Delete the found keys if any
		if len(keys) > 0 {
			_, err = conn.Do("DEL", redis.Args{}.AddFlat(keys)...)
			if err != nil {
				return fmt.Errorf("DEL operation failed: %w", err)
			}
		}

		// Break the loop if we've scanned all keys
		if cursor == 0 {
			break
		}
	}

	return nil
}

// Ttl returns the remaining time to live of a key in seconds
//
// Parameters:
//   - key: The key to check
//
// Returns:
//   - int: The remaining time to live in seconds, or a negative value if the key does not exist or has no expiry
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ttl, err := manager.Ttl("mykey")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Time to live: %d seconds\n", ttl)
func (m *Manager) Ttl(key string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("TTL", m.prefixKey(key)))
}

// Expire sets an expiration time (in seconds) on a key
//
// Parameters:
//   - key: The key to set the expiration on
//   - ttl: The time to live in seconds
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	err := manager.Expire("mykey", 3600) // Expire in 1 hour
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) Expire(key string, ttl int) error {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	_, err := conn.Do("EXPIRE", m.prefixKey(key), ttl)

	return err
}

// Set stores a key-value pair in Redis
//
// Parameters:
//   - key: The key under which to store the value
//   - data: The value to store (will be JSON-encoded)
//   - ttl: The time to live in seconds (0 for no expiration)
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	err := manager.Set("user:1", User{Name: "John", Age: 30}, 3600)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) Set(key string, data any, ttl int) (err error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	if ttl > 0 {
		_, err = conn.Do("SET", m.prefixKey(key), data, "EX", ttl)
	} else {
		_, err = conn.Do("SET", m.prefixKey(key), data)
	}

	return err
}

// SetString stores a string value in Redis
//
// Parameters:
//   - key: The key under which to store the string
//   - str: The string to store
//   - ttl: The time to live in seconds (0 for no expiration)
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	err := manager.SetString("greeting", "Hello, World!", 3600)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) SetString(key string, str string, ttl int) (err error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	if ttl > 0 {
		_, err = conn.Do("SET", m.prefixKey(key), str, "EX", ttl)
	} else {
		_, err = conn.Do("SET", m.prefixKey(key), str)
	}

	return err
}

// GetString retrieves a string value from Redis
//
// Parameters:
//   - key: The key of the string to retrieve
//
// Returns:
//   - string: The retrieved string value
//   - error: Any error that occurred during the operation
//
// Example:
//
//	str, err := manager.GetString("greeting")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(str)
func (m *Manager) GetString(key string) (string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()
	exist, err := redis.Bool(conn.Do("EXISTS", m.prefixKey(key)))
	if !exist || err != nil {
		return "", err
	}
	return redis.String(conn.Do("GET", m.prefixKey(key)))
}

// Get retrieves a value from Redis and returns it as a byte slice
//
// Parameters:
//   - key: The key of the value to retrieve
//
// Returns:
//   - []byte: The retrieved value as a byte slice
//   - error: Any error that occurred during the operation
//
// Example:
//
//	data, err := manager.Get("user:1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	var user User
//	err = json.Unmarshal(data, &user)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) Get(key string) ([]byte, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	exists, err := redis.Bool(conn.Do("EXISTS", m.prefixKey(key)))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, redis.ErrNil
	}

	return redis.Bytes(conn.Do("GET", m.prefixKey(key)))
}

// SetNX sets a key-value pair if the key does not already exist
//
// Parameters:
//   - key: The key to set
//   - value: The value to set
//   - sec: The expiration time in seconds (0 for no expiration)
//
// Returns:
//   - bool: True if the key was set, false if the key already existed
//   - error: Any error that occurred during the operation
//
// Example:
//
//	set, err := manager.SetNX("mykey", "myvalue", 3600)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key was set: %v\n", set)
func (m *Manager) SetNX(key string, value any, sec int) (bool, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{m.prefixKey(key), value}.Add("NX")
	if sec > 0 {
		args = args.Add("EX", sec)
	}

	reply, err := redis.String(conn.Do("SET", args...))
	if errors.Is(err, redis.ErrNil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return reply == "OK", nil
}

// Incr increments the integer value of a key by one
//
// Parameters:
//   - key: The key of the integer to increment
//
// Returns:
//   - int: The new value after incrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	newValue, err := manager.Incr("visitor_count")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("New visitor count: %d\n", newValue)
func (m *Manager) Incr(key string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("INCR", m.prefixKey(key)))
}

// IncrBy increments the integer value of a key by a specified amount
//
// Parameters:
//   - key: The key of the integer to increment
//   - value: The amount to increment by
//
// Returns:
//   - int: The new value after incrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	newValue, err := manager.IncrBy("score", 10)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("New score: %d\n", newValue)
func (m *Manager) IncrBy(key string, value int) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("INCRBY", m.prefixKey(key), value))
}

// Decr decrements the integer value of a key by one
//
// Parameters:
//   - key: The key of the integer to decrement
//
// Returns:
//   - int: The new value after decrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	newValue, err := manager.Decr("stock_count")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Updated stock count: %d\n", newValue)
func (m *Manager) Decr(key string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("DECR", m.prefixKey(key)))
}

// DecrBy decrements the integer value of a key by a specified amount
//
// Parameters:
//   - key: The key of the integer to decrement
//   - value: The amount to decrement by
//
// Returns:
//   - int: The new value after decrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	newValue, err := manager.DecrBy("points", 5)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Updated points: %d\n", newValue)
func (m *Manager) DecrBy(key string, value int) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("DECRBY", m.prefixKey(key), value))
}

// HSet sets a field in a Redis hash
//
// Parameters:
//   - key: The key of the hash
//   - field: The field to set within the hash
//   - value: The value to set for the field
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	err := manager.HSet("user:1", "name", "John Doe")
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) HSet(key, field string, value any) error {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	_, err := conn.Do("HSET", m.prefixKey(key), field, value)
	return err
}

// HGet retrieves the value of a field in a Redis hash
//
// Parameters:
//   - key: The key of the hash
//   - field: The field to retrieve from the hash
//
// Returns:
//   - string: The value of the field
//   - error: Any error that occurred during the operation
//
// Example:
//
//	value, err := manager.HGet("user:1", "name")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("User name: %s\n", value)
func (m *Manager) HGet(key, field string) (string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.String(conn.Do("HGET", m.prefixKey(key), field))
}

// HGetAll retrieves all fields and values in a Redis hash
//
// Parameters:
//   - key: The key of the hash
//
// Returns:
//   - map[string]string: A map of all fields and their values in the hash
//   - error: Any error that occurred during the operation
//
// Example:
//
//	data, err := manager.HGetAll("user:1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for field, value := range data {
//	    fmt.Printf("%s: %s\n", field, value)
//	}
func (m *Manager) HGetAll(key string) (map[string]string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.StringMap(conn.Do("HGETALL", m.prefixKey(key)))
}

// HDel deletes one or more fields from a Redis hash
//
// Parameters:
//   - key: The key of the hash
//   - fields: One or more fields to delete from the hash
//
// Returns:
//   - int: The number of fields that were removed from the hash
//   - error: Any error that occurred during the operation
//
// Example:
//
//	removed, err := manager.HDel("user:1", "age", "address")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of fields removed: %d\n", removed)
func (m *Manager) HDel(key string, fields ...any) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(fields)
	return redis.Int(conn.Do("HDEL", args...))
}

// ZAdd adds one or more members to a sorted set, or updates the score of existing members
//
// Parameters:
//   - key: The key of the sorted set
//   - members: One or more ZSetMember structs containing score and member data
//
// Returns:
//   - int: The number of elements added to the sorted set (not including elements already existing for which the score was updated)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	added, err := manager.ZAdd("leaderboard",
//	    ZSetMember{Score: 100, Member: "player1"},
//	    ZSetMember{Score: 200, Member: "player2"})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of new members added: %d\n", added)
func (m *Manager) ZAdd(key string, members ...ZSetMember) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{}.Add(m.Prefix + key)
	for _, member := range members {
		args = args.Add(member.Score).Add(member.Member)
	}

	return redis.Int(conn.Do("ZADD", args...))
}

// ZRem removes one or more members from a sorted set
//
// Parameters:
//   - key: The key of the sorted set
//   - members: One or more members to remove from the sorted set
//
// Returns:
//   - int: The number of members removed from the sorted set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	removed, err := manager.ZRem("leaderboard", "player1", "player2")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members removed: %d\n", removed)
func (m *Manager) ZRem(key string, members ...any) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(members)
	return redis.Int(conn.Do("ZREM", args...))
}

// ZRange returns a range of members in a sorted set, by index
//
// Parameters:
//   - key: The key of the sorted set
//   - start: The starting index
//   - stop: The ending index
//
// Returns:
//   - []string: A slice of members in the specified range
//   - error: Any error that occurred during the operation
//
// Example:
//
//	members, err := manager.ZRange("leaderboard", 0, 9)  // Get top 10
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %s\n", i+1, member)
//	}
func (m *Manager) ZRange(key string, start, stop int) ([]string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Strings(conn.Do("ZRANGE", m.prefixKey(key), start, stop))
}

// ZRangeWithScores returns a range of members with their scores in a sorted set, by index
//
// Parameters:
//   - key: The key of the sorted set
//   - start: The starting index
//   - stop: The ending index
//
// Returns:
//   - []ZSetMember: A slice of ZSetMember structs containing members and their scores in the specified range
//   - error: Any error that occurred during the operation
//
// Example:
//
//	members, err := manager.ZRangeWithScores("leaderboard", 0, 9)  // Get top 10 with scores
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %v (Score: %.2f)\n", i+1, member.Member, member.Score)
//	}
func (m *Manager) ZRangeWithScores(key string, start, stop int) ([]ZSetMember, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	values, err := redis.Values(conn.Do("ZRANGE", m.prefixKey(key), start, stop, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	var members []ZSetMember
	for i := 0; i < len(values); i += 2 {
		score, err := strconv.ParseFloat(string(values[i+1].([]byte)), 64)
		if err != nil {
			return nil, err
		}
		members = append(members, ZSetMember{
			Score:  score,
			Member: string(values[i].([]byte)),
		})
	}

	return members, nil
}

// ZRevRange returns a range of members in a sorted set, by index, with scores ordered from high to low
//
// Parameters:
//   - key: The key of the sorted set
//   - start: The starting index
//   - stop: The ending index
//
// Returns:
//   - []string: A slice of members in the specified range, ordered from high to low score
//   - error: Any error that occurred during the operation
//
// Example:
//
//	members, err := manager.ZRevRange("leaderboard", 0, 9)  // Get top 10 in reverse order
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %s\n", i+1, member)
//	}
func (m *Manager) ZRevRange(key string, start, stop int) ([]string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Strings(conn.Do("ZREVRANGE", m.prefixKey(key), start, stop))
}

// ZRevRangeWithScores returns a range of members with their scores in a sorted set, by index, with scores ordered from high to low
//
// Parameters:
//   - key: The key of the sorted set
//   - start: The starting index
//   - stop: The ending index
//
// Returns:
//   - []ZSetMember: A slice of ZSetMember structs containing members and their scores in the specified range, ordered from high to low score
//   - error: Any error that occurred during the operation
//
// Example:
//
//	members, err := manager.ZRevRangeWithScores("leaderboard", 0, 9)  // Get top 10 with scores in reverse order
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %v (Score: %.2f)\n", i+1, member.Member, member.Score)
//	}
func (m *Manager) ZRevRangeWithScores(key string, start, stop int) ([]ZSetMember, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	values, err := redis.Values(conn.Do("ZREVRANGE", m.prefixKey(key), start, stop, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	var members []ZSetMember
	for i := 0; i < len(values); i += 2 {
		score, err := strconv.ParseFloat(string(values[i+1].([]byte)), 64)
		if err != nil {
			return nil, err
		}
		members = append(members, ZSetMember{
			Score:  score,
			Member: string(values[i].([]byte)),
		})
	}

	return members, nil
}

// ZCard returns the number of members in a sorted set
//
// Parameters:
//   - key: The key of the sorted set
//
// Returns:
//   - int: The number of members in the sorted set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	count, err := manager.ZCard("leaderboard")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members in leaderboard: %d\n", count)
func (m *Manager) ZCard(key string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("ZCARD", m.prefixKey(key)))
}

// ZScore returns the score of a member in a sorted set
//
// Parameters:
//   - key: The key of the sorted set
//   - member: The member whose score to retrieve
//
// Returns:
//   - float64: The score of the member
//   - error: Any error that occurred during the operation
//
// Example:
//
//	score, err := manager.ZScore("leaderboard", "player1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Score of player1: %.2f\n", score)
func (m *Manager) ZScore(key string, member string) (float64, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Float64(conn.Do("ZSCORE", m.prefixKey(key), member))
}

// ZRank returns the rank of a member in a sorted set, with scores ordered from low to high
//
// Parameters:
//   - key: The key of the sorted set
//   - member: The member whose rank to retrieve
//
// Returns:
//   - int: The rank of the member (0-based)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	rank, err := manager.ZRank("leaderboard", "player1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Rank of player1: %d\n", rank)
func (m *Manager) ZRank(key string, member string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("ZRANK", m.prefixKey(key), member))
}

// ZRevRank returns the rank of a member in a sorted set, with scores ordered from high to low
//
// Parameters:
//   - key: The key of the sorted set
//   - member: The member whose rank to retrieve
//
// Returns:
//   - int: The rank of the member (0-based)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	rank, err := manager.ZRevRank("leaderboard", "player1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Reverse rank of player1: %d\n", rank)
func (m *Manager) ZRevRank(key string, member string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("ZREVRANK", m.prefixKey(key), member))
}

// SAdd adds one or more members to a set
//
// Parameters:
//   - key: The key of the set
//   - members: One or more members to add to the set
//
// Returns:
//   - int: The number of members that were added to the set (not including members already present)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	added, err := manager.SAdd("myset", "member1", "member2", "member3")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of new members added: %d\n", added)
func (m *Manager) SAdd(key string, members ...any) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(members)
	return redis.Int(conn.Do("SADD", args...))
}

// SRem removes one or more members from a set
//
// Parameters:
//   - key: The key of the set
//   - members: One or more members to remove from the set
//
// Returns:
//   - int: The number of members that were removed from the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	removed, err := manager.SRem("myset", "member1", "member2")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members removed: %d\n", removed)
func (m *Manager) SRem(key string, members ...any) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(members)
	return redis.Int(conn.Do("SREM", args...))
}

// SMembers returns all members of a set
//
// Parameters:
//   - key: The key of the set
//
// Returns:
//   - []string: A slice containing all members of the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	members, err := manager.SMembers("myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Set members:", members)
func (m *Manager) SMembers(key string) ([]string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Strings(conn.Do("SMEMBERS", m.prefixKey(key)))
}

// SCard returns the number of members in a set
//
// Parameters:
//   - key: The key of the set
//
// Returns:
//   - int: The number of members in the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	count, err := manager.SCard("myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members in set: %d\n", count)
func (m *Manager) SCard(key string) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("SCARD", m.prefixKey(key)))
}

// SIsMember checks if a value is a member of a set
//
// Parameters:
//   - key: The key of the set
//   - member: The member to check for
//
// Returns:
//   - bool: true if the member exists in the set, false otherwise
//   - error: Any error that occurred during the operation
//
// Example:
//
//	isMember, err := manager.SIsMember("myset", "member1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if isMember {
//	    fmt.Println("member1 is in the set")
//	} else {
//	    fmt.Println("member1 is not in the set")
//	}
func (m *Manager) SIsMember(key string, member any) (bool, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Bool(conn.Do("SISMEMBER", m.prefixKey(key), member))
}

// SPop removes and returns a random member from a set
//
// Parameters:
//   - key: The key of the set
//
// Returns:
//   - string: The removed member
//   - error: Any error that occurred during the operation
//
// Example:
//
//	member, err := manager.SPop("myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Removed member: %s\n", member)
func (m *Manager) SPop(key string) (string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.String(conn.Do("SPOP", m.prefixKey(key)))
}

// SRandMember returns a random member from a set
//
// Parameters:
//   - key: The key of the set
//
// Returns:
//   - string: A random member from the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	member, err := manager.SRandMember("myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Random member: %s\n", member)
func (m *Manager) SRandMember(key string) (string, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.String(conn.Do("SRANDMEMBER", m.prefixKey(key)))
}

// Publish publishes a message to a channel
//
// Parameters:
//   - channel: The channel to publish to
//   - message: The message to publish
//
// Returns:
//   - int: The number of clients that received the message
//   - error: Any error that occurred during the operation
//
// Example:
//
//	receivers, err := manager.Publish("mychannel", "Hello, subscribers!")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Message sent to %d subscribers\n", receivers)
func (m *Manager) Publish(channel string, message any) (int, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	return redis.Int(conn.Do("PUBLISH", m.Prefix+channel, message))
}

// Subscribe subscribes to one or more channels
//
// Parameters:
//   - channels: One or more channels to subscribe to
//
// Returns:
//   - *redis.PubSubConn: A new publish/subscribe connection
//   - error: Any error that occurred during the operation
//
// Example:
//
//	psc, err := manager.Subscribe("channel1", "channel2")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer psc.Close()
//
//	for {
//	    switch v := psc.Receive().(type) {
//	    case redis.Message:
//	        fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
//	    case redis.Subscription:
//	        fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
//	    case error:
//	        return v
//	    }
//	}
func (m *Manager) Subscribe(channels ...any) (*redis.PubSubConn, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	psc := &redis.PubSubConn{Conn: conn}
	prefixedChannels := make([]any, len(channels))
	for i, ch := range channels {
		prefixedChannels[i] = m.Prefix + ch.(string)
	}

	err := psc.Subscribe(prefixedChannels...)
	if err != nil {
		return nil, err
	}

	return psc, nil
}

// PSubscribe subscribes to one or more channels using patterns
//
// Parameters:
//   - patterns: One or more patterns to subscribe to
//
// Returns:
//   - *redis.PubSubConn: A new publish/subscribe connection
//   - error: Any error that occurred during the operation
//
// Example:
//
//	psc, err := manager.PSubscribe("channel*", "user.*")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer psc.Close()
//
//	for {
//	    switch v := psc.Receive().(type) {
//	    case redis.PMessage:
//	        fmt.Printf("%s: message: %s (pattern: %s)\n", v.Channel, v.Data, v.Pattern)
//	    case redis.Subscription:
//	        fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
//	    case error:
//	        return v
//	    }
//	}
func (m *Manager) PSubscribe(patterns ...any) (*redis.PubSubConn, error) {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	psc := &redis.PubSubConn{Conn: conn}
	prefixedPatterns := make([]any, len(patterns))
	for i, p := range patterns {
		prefixedPatterns[i] = m.Prefix + p.(string)
	}

	err := psc.PSubscribe(prefixedPatterns...)
	if err != nil {
		return nil, err
	}

	return psc, nil
}

// SetJSON serializes the given value to JSON and stores it in Redis under the specified key.
//
// Parameters:
//   - key: The key under which to store the JSON data in Redis
//   - value: The value to be serialized to JSON and stored
//   - expiration: The expiration time for the key in seconds. Use 0 for no expiration.
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	type User struct {
//	    Name  string
//	    Email string
//	}
//	user := User{Name: "John Doe", Email: "john@example.com"}
//	err := manager.SetJSON("user:1", user, 3600) // Expire in 1 hour
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) SetJSON(key string, value any, expiration int) error {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if expiration > 0 {
		_, err = conn.Do("SET", m.prefixKey(key), jsonData, "EX", expiration)
	} else {
		_, err = conn.Do("SET", m.Prefix+key, jsonData)
	}

	return err
}

// GetJSON retrieves JSON data from Redis for the specified key and unmarshal it into the provided value.
//
// Parameters:
//   - key: The key from which to retrieve the JSON data in Redis
//   - value: A pointer to the value where the unmarshalled JSON data will be stored
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	var user struct {
//	    Name  string
//	    Email string
//	}
//	err := manager.GetJSON("user:1", &user)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	fmt.Printf("Retrieved user: %+v\n", user)
func (m *Manager) GetJSON(key string, value any) error {
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	reply, err := conn.Do("GET", m.Prefix+key)
	if err != nil {
		return err
	}

	if reply == nil {
		return redis.ErrNil
	}

	jsonData, err := redis.Bytes(reply, err)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonData, value)
	if err != nil {
		return err
	}

	return nil
}

// LPopJSON removes and returns the first element from the list stored at key, unmarshalling it into the provided value.
//
// Parameters:
//   - key: The Redis key of the list.
//   - value: A pointer to the variable where the unmarshalled JSON data will be stored.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//     Returns ErrNil if the list is empty.
//
// Example:
//
//	var person Person
//	err := manager.LPopJSON("people_list", &person)
//	if err != nil {
//	    if err == redis.ErrNil {
//	        fmt.Println("List is empty")
//	    } else {
//	        log.Printf("Failed to pop from list: %v", err)
//	    }
//	    return
//	}
//	fmt.Printf("Popped person: %+v\n", person)
func (m *Manager) LPopJSON(key string, value any) error {
	// Ensure value is a pointer
	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return fmt.Errorf("value must be a pointer")
	}

	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Execute LPOP command and retrieve the result as bytes
	data, err := redis.Bytes(conn.Do("LPOP", m.prefixKey(key)))
	if err != nil {
		return err
	}

	// Unmarshal the JSON data into the provided value
	return json.Unmarshal(data, value)
}

// RPopJSON removes and returns the last element from the list stored at key, unmarshalling it into the provided value.
//
// Parameters:
//   - key: The Redis key of the list.
//   - value: A pointer to the variable where the unmarshalled JSON data will be stored.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//     Returns ErrNil if the list is empty.
//
// Example:
//
//	var task Task
//	err := manager.RPopJSON("task_queue", &task)
//	if err != nil {
//	    if err == redis.ErrNil {
//	        fmt.Println("Queue is empty")
//	    } else {
//	        log.Printf("Failed to pop from list: %v", err)
//	    }
//	    return
//	}
//	fmt.Printf("Popped task: %+v\n", task)
func (m *Manager) RPopJSON(key string, value any) error {
	// Ensure value is a pointer
	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return fmt.Errorf("value must be a pointer")
	}

	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Execute RPOP command and retrieve the result as bytes
	data, err := redis.Bytes(conn.Do("RPOP", m.prefixKey(key)))
	if err != nil {
		return err
	}

	// Unmarshal the JSON data into the provided value
	return json.Unmarshal(data, value)
}

// LPushJSON adds one or more elements to the beginning of the list stored at key, marshalled from the provided values.
//
// Parameters:
//   - key: The Redis key of the list.
//   - values: One or more values to be marshalled to JSON and pushed to the list.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//
// Example:
//
//	person1 := Person{Name: "Alice", Age: 30}
//	person2 := Person{Name: "Bob", Age: 25}
//	err := manager.LPushJSON("people_list", person1, person2)
//	if err != nil {
//	    log.Printf("Failed to push to list: %v", err)
//	    return
//	}
func (m *Manager) LPushJSON(key string, values ...any) error {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Prepare arguments for LPUSH
	args := redis.Args{}.Add(m.prefixKey(key))
	for _, value := range values {
		jsonData, err := json.Marshal(value)
		if err != nil {
			return err
		}
		args = args.Add(jsonData)
	}

	// Execute LPUSH command with all JSON data at once
	_, err := conn.Do("LPUSH", args...)
	return err
}

// RPushJSON adds one or more elements to the end of the list stored at key, marshalled from the provided values.
//
// Parameters:
//   - key: The Redis key of the list.
//   - values: One or more values to be marshalled to JSON and pushed to the list.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//
// Example:
//
//	task1 := Task{ID: 1, Description: "Do something"}
//	task2 := Task{ID: 2, Description: "Do something else"}
//	err := manager.RPushJSON("task_queue", task1, task2)
//	if err != nil {
//	    log.Printf("Failed to push to list: %v", err)
//	    return
//	}
func (m *Manager) RPushJSON(key string, values ...any) error {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Prepare arguments for RPUSH
	args := redis.Args{}.Add(m.prefixKey(key))
	for _, value := range values {
		jsonData, err := json.Marshal(value)
		if err != nil {
			return err
		}
		args = args.Add(jsonData)
	}

	// Execute RPUSH command with all JSON data at once
	_, err := conn.Do("RPUSH", args...)
	return err
}

// LPop removes and returns the first element from the list stored at key as a string.
//
// Parameters:
//   - key: The Redis key of the list.
//
// Returns:
//   - []byte: The popped element as a []byte.
//   - error: An error if the operation fails, or nil if successful.
//     Returns ErrNil if the list is empty.
//
// Example:
//
//	element, err := manager.LPop("my_list")
//	if err != nil {
//	    if err == redis.ErrNil {
//	        fmt.Println("List is empty")
//	    } else {
//	        log.Printf("Failed to pop from list: %v", err)
//	    }
//	    return
//	}
//	fmt.Printf("Popped element: %s\n", element)
func (m *Manager) LPop(key string) ([]byte, error) {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Execute LPOP command and return the result as a string
	return redis.Bytes(conn.Do("LPOP", m.prefixKey(key)))
}

// RPop removes and returns the last element from the list stored at key as a string.
//
// Parameters:
//   - key: The Redis key of the list.
//
// Returns:
//   - []byte: The popped element as a []byte.
//   - error: An error if the operation fails, or nil if successful.
//     Returns ErrNil if the list is empty.
//
// Example:
//
//	element, err := manager.RPop("my_list")
//	if err != nil {
//	    if err == redis.ErrNil {
//	        fmt.Println("List is empty")
//	    } else {
//	        log.Printf("Failed to pop from list: %v", err)
//	    }
//	    return
//	}
//	fmt.Printf("Popped element: %s\n", element)
func (m *Manager) RPop(key string) ([]byte, error) {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Execute RPOP command and return the result as a string
	return redis.Bytes(conn.Do("RPOP", m.prefixKey(key)))
}

// LPush adds one or more elements to the beginning of the list stored at key.
//
// Parameters:
//   - key: The Redis key of the list.
//   - values: One or more values to be pushed to the list.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//
// Example:
//
//	err := manager.LPush("my_list", "item1", "item2", "item3")
//	if err != nil {
//	    log.Printf("Failed to push to list: %v", err)
//	    return
//	}
func (m *Manager) LPush(key string, values ...any) error {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Prepare arguments for LPUSH
	args := redis.Args{}.Add(m.prefixKey(key)).AddFlat(values)

	// Execute LPUSH command with all values at once
	_, err := conn.Do("LPUSH", args...)
	return err
}

// RPush adds one or more elements to the end of the list stored at key.
//
// Parameters:
//   - key: The Redis key of the list.
//   - values: One or more values to be pushed to the list.
//
// Returns:
//   - error: An error if the operation fails, or nil if successful.
//
// Example:
//
//	err := manager.RPush("my_list", "item1", "item2", "item3")
//	if err != nil {
//	    log.Printf("Failed to push to list: %v", err)
//	    return
//	}
func (m *Manager) RPush(key string, values ...any) error {
	// Get a connection from the pool
	conn := m.ConnPool.Get()
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	// Prepare arguments for RPUSH
	args := redis.Args{}.Add(m.prefixKey(key)).AddFlat(values)

	// Execute RPUSH command with all values at once
	_, err := conn.Do("RPUSH", args...)
	return err
}

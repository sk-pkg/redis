// Copyright 2024 Seakee.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

// Package redis provides a Redis client implementation with connection pooling.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
)

// DoWithContext execute a Redis command with the given arguments and context
//
// Parameters:
//   - ctx: The context for the Redis command
//   - commandName: The name of the Redis command to execute
//   - args: The arguments for the Redis command
//
// Returns:
//   - any: The result of the Redis command
//   - error: Any error that occurred during the execution
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	result, err := manager.DoWithContext(ctx, "SET", "mykey", "myvalue")
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) DoWithContext(ctx context.Context, commandName string, args ...any) (any, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return redis.DoContext(conn, ctx, commandName, args...)
}

// LuaWithContext executes a Lua script on the Redis server
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	script := `return redis.call('SET', KEYS[1], ARGV[1])`
//	result, err := manager.LuaWithContext(ctx, 1, script, []string{"mykey", "myvalue"})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) LuaWithContext(ctx context.Context, keyCount int, script string, keysAndArgs []string) (any, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	args := redis.Args{}.AddFlat(keysAndArgs)
	lua := redis.NewScript(keyCount, script)

	return lua.Do(conn, args...)
}

// ExistsWithContext checks if a key exists in Redis
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key to check
//
// Returns:
//   - bool: True if the key exists, false otherwise
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	exists, err := manager.ExistsWithContext(ctx, "mykey")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key exists: %v\n", exists)
func (m *Manager) ExistsWithContext(ctx context.Context, key string) (bool, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	return redis.Bool(conn.Do("EXISTS", m.prefixKey(key)))
}

// DelWithContext deletes a key from Redis
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key to delete
//
// Returns:
//   - bool: True if the key was deleted, false if the key did not exist
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	deleted, err := manager.DelWithContext(ctx, "mykey")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key deleted: %v\n", deleted)
func (m *Manager) DelWithContext(ctx context.Context, key string) (bool, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	return redis.Bool(conn.Do("DEL", m.prefixKey(key)))
}

// BatchDelWithContext deletes all keys matching a given pattern.
//
// It uses the SCAN command to iterate through keys, which is more efficient
// and less blocking than the KEYS command for large datasets.
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	err := manager.BatchDelWithContext(ctx, "user:*")
//	if err != nil {
//	    log.Printf("Failed to batch delete: %v", err)
//	}
func (m *Manager) BatchDelWithContext(ctx context.Context, pattern string) error {
	// Get a connection from the pool
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

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

// TtlWithContext returns the remaining time to live of a key in seconds
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key to check
//
// Returns:
//   - int: The remaining time to live in seconds, or a negative value if the key does not exist or has no expiry
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	ttl, err := manager.TtlWithContext(ctx, "mykey")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Time to live: %d seconds\n", ttl)
func (m *Manager) TtlWithContext(ctx context.Context, key string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("TTL", m.prefixKey(key)))
}

// ExpireWithContext sets an expiration time (in seconds) on a key
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key to set the expiration on
//   - ttl: The time to live in seconds
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	err := manager.ExpireWithContext(ctx, "mykey", 3600) // Expire in 1 hour
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) ExpireWithContext(ctx context.Context, key string, ttl int) error {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("EXPIRE", m.prefixKey(key), ttl)

	return err
}

// SetWithContext stores a key-value pair in Redis
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key under which to store the value
//   - data: The value to store (will be JSON-encoded)
//   - ttl: The time to live in seconds (0 for no expiration)
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	err := manager.SetWithContext(ctx, "user:1", User{Name: "John", Age: 30}, 3600)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) SetWithContext(ctx context.Context, key string, data any, ttl int) error {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if ttl > 0 {
		_, err = conn.Do("SET", m.prefixKey(key), data, "EX", ttl)
	} else {
		_, err = conn.Do("SET", m.prefixKey(key), data)
	}

	return err
}

// SetStringWithContext stores a string value in Redis
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key under which to store the string
//   - str: The string to store
//   - ttl: The time to live in seconds (0 for no expiration)
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	err := manager.SetStringWithContext(ctx, "greeting", "Hello, World!", 3600)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) SetStringWithContext(ctx context.Context, key string, str string, ttl int) error {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if ttl > 0 {
		_, err = conn.Do("SET", m.prefixKey(key), str, "EX", ttl)
	} else {
		_, err = conn.Do("SET", m.prefixKey(key), str)
	}

	return err
}

// GetStringWithContext retrieves a string value from Redis
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the string to retrieve
//
// Returns:
//   - string: The retrieved string value
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	str, err := manager.GetStringWithContext(ctx, "greeting")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(str)
func (m *Manager) GetStringWithContext(ctx context.Context, key string) (string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	exist, err := redis.Bool(conn.Do("EXISTS", m.prefixKey(key)))
	if !exist || err != nil {
		return "", err
	}
	return redis.String(conn.Do("GET", m.prefixKey(key)))
}

// GetWithContext retrieves a value from Redis and returns it as a byte slice
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the value to retrieve
//
// Returns:
//   - []byte: The retrieved value as a byte slice
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	data, err := manager.GetWithContext(ctx, "user:1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	var user User
//	err = json.Unmarshal(data, &user)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("EXISTS", m.prefixKey(key)))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, redis.ErrNil
	}

	return redis.Bytes(conn.Do("GET", m.prefixKey(key)))
}

// SetNXWithContext sets a key-value pair if the key does not already exist
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	set, err := manager.SetNXWithContext(ctx, "mykey", "myvalue", 3600)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key was set: %v\n", set)
func (m *Manager) SetNXWithContext(ctx context.Context, key string, value any, sec int) (bool, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

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

// IncrWithContext increments the integer value of a key by one
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the integer to increment
//
// Returns:
//   - int: The new value after incrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	newValue, err := manager.IncrWithContext(ctx, "visitor_count")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("New visitor count: %d\n", newValue)
func (m *Manager) IncrWithContext(ctx context.Context, key string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("INCR", m.prefixKey(key)))
}

// IncrByWithContext increments the integer value of a key by a specified amount
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the integer to increment
//   - value: The amount to increment by
//
// Returns:
//   - int: The new value after incrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	newValue, err := manager.IncrByWithContext(ctx, "score", 10)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("New score: %d\n", newValue)
func (m *Manager) IncrByWithContext(ctx context.Context, key string, value int) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("INCRBY", m.prefixKey(key), value))
}

// DecrWithContext decrements the integer value of a key by one
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the integer to decrement
//
// Returns:
//   - int: The new value after decrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	newValue, err := manager.DecrWithContext(ctx, "stock_count")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Updated stock count: %d\n", newValue)
func (m *Manager) DecrWithContext(ctx context.Context, key string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("DECR", m.prefixKey(key)))
}

// DecrByWithContext decrements the integer value of a key by a specified amount
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the integer to decrement
//   - value: The amount to decrement by
//
// Returns:
//   - int: The new value after decrementing
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	newValue, err := manager.DecrByWithContext(ctx, "points", 5)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Updated points: %d\n", newValue)
func (m *Manager) DecrByWithContext(ctx context.Context, key string, value int) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("DECRBY", m.prefixKey(key), value))
}

// HSetWithContext sets a field in a Redis hash
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the hash
//   - field: The field to set within the hash
//   - value: The value to set for the field
//
// Returns:
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	err := manager.HSetWithContext(ctx, "user:1", "name", "John Doe")
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) HSetWithContext(ctx context.Context, key, field string, value any) error {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("HSET", m.prefixKey(key), field, value)
	return err
}

// HGetWithContext retrieves the value of a field in a Redis hash
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the hash
//   - field: The field to retrieve from the hash
//
// Returns:
//   - string: The value of the field
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	value, err := manager.HGetWithContext(ctx, "user:1", "name")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("User name: %s\n", value)
func (m *Manager) HGetWithContext(ctx context.Context, key, field string) (string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return redis.String(conn.Do("HGET", m.prefixKey(key), field))
}

// HGetAllWithContext retrieves all fields and values in a Redis hash
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the hash
//
// Returns:
//   - map[string]string: A map of all fields and their values in the hash
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	data, err := manager.HGetAllWithContext(ctx, "user:1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for field, value := range data {
//	    fmt.Printf("%s: %s\n", field, value)
//	}
func (m *Manager) HGetAllWithContext(ctx context.Context, key string) (map[string]string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return redis.StringMap(conn.Do("HGETALL", m.prefixKey(key)))
}

// HDelWithContext deletes one or more fields from a Redis hash
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the hash
//   - fields: One or more fields to delete from the hash
//
// Returns:
//   - int: The number of fields that were removed from the hash
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	removed, err := manager.HDelWithContext(ctx, "user:1", "age", "address")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of fields removed: %d\n", removed)
func (m *Manager) HDelWithContext(ctx context.Context, key string, fields ...any) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(fields)
	return redis.Int(conn.Do("HDEL", args...))
}

// ZAddWithContext adds one or more members to a sorted set, or updates the score of existing members
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the sorted set
//   - members: One or more ZSetMember structs containing score and member data
//
// Returns:
//   - int: The number of elements added to the sorted set (not including elements already existing for which the score was updated)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	added, err := manager.ZAddWithContext(ctx, "leaderboard",
//	    ZSetMember{Score: 100, Member: "player1"},
//	    ZSetMember{Score: 200, Member: "player2"})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of new members added: %d\n", added)
func (m *Manager) ZAddWithContext(ctx context.Context, key string, members ...ZSetMember) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	args := redis.Args{}.Add(m.Prefix + key)
	for _, member := range members {
		args = args.Add(member.Score).Add(member.Member)
	}

	return redis.Int(conn.Do("ZADD", args...))
}

// ZRemWithContext removes one or more members from a sorted set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the sorted set
//   - members: One or more members to remove from the sorted set
//
// Returns:
//   - int: The number of members removed from the sorted set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	removed, err := manager.ZRemWithContext(ctx, "leaderboard", "player1", "player2")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members removed: %d\n", removed)
func (m *Manager) ZRemWithContext(ctx context.Context, key string, members ...any) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(members)
	return redis.Int(conn.Do("ZREM", args...))
}

// ZRangeWithContext returns a range of members in a sorted set, by index
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	members, err := manager.ZRangeWithContext(ctx, "leaderboard", 0, 9)  // Get top 10
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %s\n", i+1, member)
//	}
func (m *Manager) ZRangeWithContext(ctx context.Context, key string, start, stop int64) ([]string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return redis.Strings(conn.Do("ZRANGE", m.prefixKey(key), start, stop))
}

// ZRangeWithScoresWithContext returns a range of members with their scores in a sorted set, by index
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	members, err := manager.ZRangeWithScoresWithContext(ctx, "leaderboard", 0, 9)  // Get top 10 with scores
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %v (Score: %.2f)\n", i+1, member.Member, member.Score)
//	}
func (m *Manager) ZRangeWithScoresWithContext(ctx context.Context, key string, start, stop int64) ([]ZSetMember, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

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

// ZRevRangeWithContext returns a range of members in a sorted set, by index, with scores ordered from high to low
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	members, err := manager.ZRevRangeWithContext(ctx, "leaderboard", 0, 9)  // Get top 10 in reverse order
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %s\n", i+1, member)
//	}
func (m *Manager) ZRevRangeWithContext(ctx context.Context, key string, start, stop int64) ([]string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return redis.Strings(conn.Do("ZREVRANGE", m.prefixKey(key), start, stop))
}

// ZRevRangeWithScoresWithContext returns a range of members with their scores in a sorted set, by index, with scores ordered from high to low
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	members, err := manager.ZRevRangeWithScoresWithContext(ctx, "leaderboard", 0, 9)  // Get top 10 with scores in reverse order
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for i, member := range members {
//	    fmt.Printf("%d. %v (Score: %.2f)\n", i+1, member.Member, member.Score)
//	}
func (m *Manager) ZRevRangeWithScoresWithContext(ctx context.Context, key string, start, stop int64) ([]ZSetMember, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

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

// ZCardWithContext returns the number of members in a sorted set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the sorted set
//
// Returns:
//   - int: The number of members in the sorted set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	count, err := manager.ZCardWithContext(ctx, "leaderboard")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members in leaderboard: %d\n", count)
func (m *Manager) ZCardWithContext(ctx context.Context, key string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("ZCARD", m.prefixKey(key)))
}

// ZScoreWithContext returns the score of a member in a sorted set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the sorted set
//   - member: The member whose score to retrieve
//
// Returns:
//   - float64: The score of the member
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	score, err := manager.ZScoreWithContext(ctx, "leaderboard", "player1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Score of player1: %.2f\n", score)
func (m *Manager) ZScoreWithContext(ctx context.Context, key string, member string) (float64, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Float64(conn.Do("ZSCORE", m.prefixKey(key), member))
}

// ZRankWithContext returns the rank of a member in a sorted set, with scores ordered from low to high
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the sorted set
//   - member: The member whose rank to retrieve
//
// Returns:
//   - int: The rank of the member (0-based)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	rank, err := manager.ZRankWithContext(ctx, "leaderboard", "player1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Rank of player1: %d\n", rank)
func (m *Manager) ZRankWithContext(ctx context.Context, key string, member string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("ZRANK", m.prefixKey(key), member))
}

// ZRevRankWithContext returns the rank of a member in a sorted set, with scores ordered from high to low
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the sorted set
//   - member: The member whose rank to retrieve
//
// Returns:
//   - int: The rank of the member (0-based)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	rank, err := manager.ZRevRankWithContext(ctx, "leaderboard", "player1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Reverse rank of player1: %d\n", rank)
func (m *Manager) ZRevRankWithContext(ctx context.Context, key string, member string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("ZREVRANK", m.prefixKey(key), member))
}

// SAddWithContext adds one or more members to a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//   - members: One or more members to add to the set
//
// Returns:
//   - int: The number of members that were added to the set (not including members already present)
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	added, err := manager.SAddWithContext(ctx, "myset", "member1", "member2", "member3")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of new members added: %d\n", added)
func (m *Manager) SAddWithContext(ctx context.Context, key string, members ...any) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(members)
	return redis.Int(conn.Do("SADD", args...))
}

// SRemWithContext removes one or more members from a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//   - members: One or more members to remove from the set
//
// Returns:
//   - int: The number of members that were removed from the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	removed, err := manager.SRemWithContext(ctx, "myset", "member1", "member2")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members removed: %d\n", removed)
func (m *Manager) SRemWithContext(ctx context.Context, key string, members ...any) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	args := redis.Args{}.Add(m.Prefix + key).AddFlat(members)
	return redis.Int(conn.Do("SREM", args...))
}

// SMembersWithContext returns all members of a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//
// Returns:
//   - []string: A slice containing all members of the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	members, err := manager.SMembersWithContext(ctx, "myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Set members:", members)
func (m *Manager) SMembersWithContext(ctx context.Context, key string) ([]string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return redis.Strings(conn.Do("SMEMBERS", m.prefixKey(key)))
}

// SCardWithContext returns the number of members in a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//
// Returns:
//   - int: The number of members in the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	count, err := manager.SCardWithContext(ctx, "myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Number of members in set: %d\n", count)
func (m *Manager) SCardWithContext(ctx context.Context, key string) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("SCARD", m.prefixKey(key)))
}

// SIsMemberWithContext checks if a value is a member of a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//   - member: The member to check for
//
// Returns:
//   - bool: true if the member exists in the set, false otherwise
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	isMember, err := manager.SIsMemberWithContext(ctx, "myset", "member1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if isMember {
//	    fmt.Println("member1 is in the set")
//	} else {
//	    fmt.Println("member1 is not in the set")
//	}
func (m *Manager) SIsMemberWithContext(ctx context.Context, key string, member any) (bool, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	return redis.Bool(conn.Do("SISMEMBER", m.prefixKey(key), member))
}

// SPopWithContext removes and returns a random member from a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//
// Returns:
//   - string: The removed member
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	member, err := manager.SPopWithContext(ctx, "myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Removed member: %s\n", member)
func (m *Manager) SPopWithContext(ctx context.Context, key string) (string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return redis.String(conn.Do("SPOP", m.prefixKey(key)))
}

// SRandMemberWithContext returns a random member from a set
//
// Parameters:
//   - ctx: The context for the Redis command
//   - key: The key of the set
//
// Returns:
//   - string: A random member from the set
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	member, err := manager.SRandMemberWithContext(ctx, "myset")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Random member: %s\n", member)
func (m *Manager) SRandMemberWithContext(ctx context.Context, key string) (string, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return redis.String(conn.Do("SRANDMEMBER", m.prefixKey(key)))
}

// PublishWithContext publishes a message to a channel
//
// Parameters:
//   - ctx: The context for the Redis command
//   - channel: The channel to publish to
//   - message: The message to publish
//
// Returns:
//   - int: The number of clients that received the message
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	receivers, err := manager.PublishWithContext(ctx, "mychannel", "Hello, subscribers!")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Message sent to %d subscribers\n", receivers)
func (m *Manager) PublishWithContext(ctx context.Context, channel string, message any) (int, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return redis.Int(conn.Do("PUBLISH", m.Prefix+channel, message))
}

// SubscribeWithContext subscribes to one or more channels
//
// Parameters:
//   - ctx: The context for the Redis command
//   - channels: One or more channels to subscribe to
//
// Returns:
//   - *redis.PubSubConn: A new publish/subscribe connection
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	psc, err := manager.SubscribeWithContext(ctx, ctx, "channel1", "channel2")
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
func (m *Manager) SubscribeWithContext(ctx context.Context, channels ...any) (*redis.PubSubConn, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}

	psc := &redis.PubSubConn{Conn: conn}
	prefixedChannels := make([]any, len(channels))
	for i, ch := range channels {
		prefixedChannels[i] = m.Prefix + ch.(string)
	}

	err = psc.Subscribe(prefixedChannels...)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return psc, nil
}

// PSubscribeWithContext subscribes to one or more channels using patterns
//
// Parameters:
//   - ctx: The context for the Redis command
//   - patterns: One or more patterns to subscribe to
//
// Returns:
//   - *redis.PubSubConn: A new publish/subscribe connection
//   - error: Any error that occurred during the operation
//
// Example:
//
//	ctx := context.Background()
//	psc, err := manager.PSubscribeWithContext(ctx, "channel*", "user.*")
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
func (m *Manager) PSubscribeWithContext(ctx context.Context, patterns ...any) (*redis.PubSubConn, error) {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}

	psc := &redis.PubSubConn{Conn: conn}
	prefixedPatterns := make([]any, len(patterns))
	for i, p := range patterns {
		prefixedPatterns[i] = m.Prefix + p.(string)
	}

	err = psc.PSubscribe(prefixedPatterns...)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return psc, nil
}

// SetJSONWithContext serializes the given value to JSON and stores it in Redis under the specified key.
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	user := User{Name: "John Doe", Email: "john@example.com"}
//	err := manager.SetJSONWithContext(ctx, "user:1", user, 3600) // Expire in 1 hour
//	if err != nil {
//	    log.Fatal(err)
//	}
func (m *Manager) SetJSONWithContext(ctx context.Context, key string, value any, expiration int) error {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if expiration > 0 {
		_, err = conn.Do("SETEX", m.Prefix+key, expiration, jsonData)
	} else {
		_, err = conn.Do("SET", m.Prefix+key, jsonData)
	}

	return err
}

// GetJSONWithContext retrieves JSON data from Redis for the specified key and unmarshal it into the provided value.
//
// Parameters:
//   - ctx: The context for the Redis command
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
//	ctx := context.Background()
//	err := manager.GetJSONWithContext(ctx, "user:1", &user)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	fmt.Printf("Retrieved user: %+v\n", user)
func (m *Manager) GetJSONWithContext(ctx context.Context, key string, value any) error {
	conn, err := m.ConnPool.GetContext(ctx)
	if err != nil {
		return err
	}

	defer conn.Close()

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

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newManager() (*Manager, error) {
	return New(
		WithAddress("10.10.10.3:6379"),
		WithPassword(""),
		WithPrefix("testprefix"),
		WithMaxActive(100),
		WithDB(0),
		WithMaxIdle(30),
		WithIdleTimeout(time.Minute),
	)
}

func TestNew(t *testing.T) {
	testCases := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{
			name: "Custom options",
			opts: []Option{
				WithAddress("10.10.10.3:6379"),
				WithPassword(""),
				WithPrefix("testprefix"),
				WithMaxActive(200),
				WithDB(1),
				WithMaxIdle(50),
				WithIdleTimeout(time.Minute),
			},
		},
		{
			name:    "Invalid address",
			opts:    []Option{WithAddress("invalid:address")},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager, err := New(tc.opts...)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, manager)
				if len(tc.opts) > 0 {
					assert.Equal(t, "testprefix:", manager.Prefix)
				}
			}
		})
	}
}

func TestPing(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	err = manager.Ping()
	assert.NoError(t, err)
}

func TestSetAndGet(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name        string
		key         string
		value       string
		expiration  int
		expectedErr bool
	}{
		{"Set and Get without expiration", "key1", "value1", 0, false},
		{"Set and Get with expiration", "key2", "value2", 60, false},
		{"Get non-existent key", "nonexistent", "", 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.expectedErr {
				err = manager.Set(tc.key, tc.value, tc.expiration)
				assert.NoError(t, err)

				value, err := manager.Get(tc.key)
				assert.NoError(t, err)
				assert.Equal(t, []byte(tc.value), value)
			} else {
				_, err := manager.Get(tc.key)
				assert.Error(t, err)
			}
		})
	}
}

func TestExists(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name     string
		key      string
		setValue bool
		expected bool
	}{
		{"Existing key", "existskey", true, true},
		{"Non-existent key", "nonexistentkey", false, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setValue {
				err := manager.Set(tc.key, "value", 0)
				require.NoError(t, err)
			}

			exists, err := manager.Exists(tc.key)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, exists)
		})
	}
}

func TestDel(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name     string
		key      string
		setValue bool
		expected bool
	}{
		{"Delete existing key", "delkey", true, true},
		{"Delete non-existent key", "nonexistentkey", false, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setValue {
				err := manager.Set(tc.key, "value", 0)
				require.NoError(t, err)
			}

			deleted, err := manager.Del(tc.key)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, deleted)

			exists, err := manager.Exists(tc.key)
			assert.NoError(t, err)
			assert.False(t, exists)
		})
	}
}

func TestBatchDel(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	// Set up test data
	testKeys := []string{"batchdel1", "batchdel2", "otherbatchdel"}
	for _, key := range testKeys {
		err := manager.Set(key, "value", 0)
		require.NoError(t, err)
	}

	testCases := []struct {
		name          string
		pattern       string
		expectedCount int
	}{
		{"Delete with prefix", "batchdel*", 2},
		{"Delete all", "*", 3},
		{"Delete non-existent pattern", "nonexistent*", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := manager.BatchDel(tc.pattern)
			assert.NoError(t, err)

			// Verify deletion
			for _, key := range testKeys {
				exists, err := manager.Exists(key)
				assert.NoError(t, err)
				if matchPattern(key, tc.pattern) {
					assert.False(t, exists)
				} else {
					assert.True(t, exists)
				}
			}
		})

		// Reset data for next test case
		for _, key := range testKeys {
			err := manager.Set(key, "value", 0)
			require.NoError(t, err)
		}
	}
}

func TestTtl(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name        string
		key         string
		setValue    bool
		expiration  int
		expectedTTL int
	}{
		{"Key with expiration", "ttlkey", true, 60, 60},
		{"Key without expiration", "noexpkey", true, 0, -1},
		{"Non-existent key", "nonexistentkey", false, 0, -2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setValue {
				err := manager.Set(tc.key, "value", int(tc.expiration))
				require.NoError(t, err)
			}

			ttl, err := manager.Ttl(tc.key)
			assert.NoError(t, err)
			if tc.expectedTTL >= 0 {
				assert.True(t, ttl >= 0 && ttl <= tc.expectedTTL)
			} else {
				assert.Equal(t, tc.expectedTTL, ttl)
			}
		})
	}
}

func TestExpire(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name       string
		key        string
		setValue   bool
		expiration int
		expected   bool
	}{
		{"Set expiration on existing key", "expirekey", true, 30, true},
		{"Set expiration on non-existent key", "nonexistentkey", false, 30, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setValue {
				err = manager.Set(tc.key, "value", 0)
				require.NoError(t, err)
			}

			err = manager.Expire(tc.key, tc.expiration)
			assert.NoError(t, err)

			ttl, err := manager.Ttl(tc.key)
			assert.NoError(t, err)
			if tc.expected {
				assert.True(t, ttl > 0 && ttl <= tc.expiration)
			} else {
				assert.Equal(t, -2, ttl)
			}
		})
	}
}

func TestLua(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Set and Get using Lua", func(t *testing.T) {
		script := `
			redis.call('SET', KEYS[1], ARGV[1])
			return redis.call('GET', KEYS[1])
		`
		result, err := manager.Lua(1, script, []string{"luakey", "luavalue"})
		assert.NoError(t, err)
		resultBytes, ok := result.([]byte)
		assert.True(t, ok, "Expected result to be []byte")
		assert.Equal(t, "luavalue", string(resultBytes))
	})

	t.Run("Increment using Lua", func(t *testing.T) {
		script := `
			redis.call('SET', KEYS[1], 0)
			return redis.call('INCR', KEYS[1])
		`
		result, err := manager.Lua(1, script, []string{"counterkey"})
		assert.NoError(t, err)
		resultInt, ok := result.(int64)
		assert.True(t, ok, "Expected result to be int64")
		assert.Equal(t, int64(1), resultInt)
	})

	t.Run("Multiple keys operation using Lua", func(t *testing.T) {
		script := `
			redis.call('SET', KEYS[1], ARGV[1])
			redis.call('SET', KEYS[2], ARGV[2])
			return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}
		`
		result, err := manager.Lua(2, script, []string{"key1", "key2", "value1", "value2"})
		assert.NoError(t, err)
		resultSlice, ok := result.([]any)
		assert.True(t, ok, "Expected result to be []any")
		assert.Equal(t, 2, len(resultSlice))
		assert.Equal(t, "value1", string(resultSlice[0].([]byte)))
		assert.Equal(t, "value2", string(resultSlice[1].([]byte)))
	})

	t.Run("Error handling in Lua", func(t *testing.T) {
		script := `return redis.call('UNKNOWN_COMMAND')`
		_, err := manager.Lua(0, script, []string{})
		assert.Error(t, err)
	})
}

func TestDoWithContext(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	t.Run("Set key", func(t *testing.T) {
		reply, err := manager.DoWithContext(ctx, "SET", "testkey", "testvalue")
		assert.NoError(t, err)
		assert.Equal(t, "OK", reply)
	})

	t.Run("Get key", func(t *testing.T) {
		reply, err := manager.DoWithContext(ctx, "GET", "testkey")
		assert.NoError(t, err)
		replyBytes, ok := reply.([]byte)
		assert.True(t, ok, "Expected reply to be []byte")
		assert.Equal(t, "testvalue", string(replyBytes))
	})

	t.Run("Invalid command", func(t *testing.T) {
		_, err := manager.DoWithContext(ctx, "INVALID", "key")
		assert.Error(t, err)
	})

	t.Run("Invalid command", func(t *testing.T) {
		_, err := manager.DoWithContext(ctx, "SET")
		assert.Error(t, err)
	})

	t.Run("Context cancellation", func(t *testing.T) {
		ctxWithCancel, cancel := context.WithCancel(ctx)
		cancel() // Cancel the context immediately
		_, err := manager.DoWithContext(ctxWithCancel, "GET", "testkey")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func matchPattern(s, pattern string) bool {
	if pattern == "*" {
		return true
	}
	if pattern[len(pattern)-1] == '*' {
		return len(s) >= len(pattern)-1 && s[:len(pattern)-1] == pattern[:len(pattern)-1]
	}
	return s == pattern
}

func TestSetAndGetJSON(t *testing.T) {
	manager, err := newManager()
	if err != nil {
		t.Fatal(err)
	}

	type TestStruct struct {
		Name  string
		Value int
	}

	t.Run("Set and Get JSON", func(t *testing.T) {
		testData := TestStruct{
			Name:  "Test",
			Value: 123,
		}

		err := manager.SetJSON("testkey", testData, 0)
		assert.NoError(t, err)

		var retrievedData TestStruct
		exists, err := manager.GetJSON("testkey", &retrievedData)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, testData, retrievedData)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		var retrievedData TestStruct
		exists, err := manager.GetJSON("nonexistentkey", &retrievedData)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Set with expiration", func(t *testing.T) {
		testData := TestStruct{
			Name:  "Expiring",
			Value: 456,
		}

		err := manager.SetJSON("expiringkey", testData, 1) // 1 second expiration
		assert.NoError(t, err)

		// Verify it exists immediately
		var retrievedData TestStruct
		exists, err := manager.GetJSON("expiringkey", &retrievedData)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, testData, retrievedData)

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Verify it no longer exists
		exists, err = manager.GetJSON("expiringkey", &retrievedData)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Set invalid JSON", func(t *testing.T) {
		invalidData := make(chan int) // channels can't be marshaled to JSON
		err := manager.SetJSON("invalidkey", invalidData, 0)
		assert.Error(t, err)
	})

	t.Run("Get invalid JSON", func(t *testing.T) {
		// Manually set invalid JSON data
		conn := manager.ConnPool.Get()
		_, err := conn.Do("SET", manager.Prefix+"invalidjsonkey", "{invalid json}")
		assert.NoError(t, err)
		conn.Close()

		var retrievedData TestStruct
		exists, err := manager.GetJSON("invalidjsonkey", &retrievedData)
		assert.Error(t, err)
		assert.False(t, exists)
	})
}

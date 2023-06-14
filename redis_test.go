package redis

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	Addr     = "127.0.0.1:6379"
	Prefix   = "redisTest"
	Password = ""
	DB       = 9
)

var cache *Manager

func TestMain(m *testing.M) {
	cache = New(WithAddress(Addr), WithPassword(Password), WithPrefix(Prefix), WithDB(DB))
	os.Exit(m.Run())
}

func TestRedis(t *testing.T) {
	err := cache.Set("key", "value", 10)
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("Test Set() success \n")

	exists, err := cache.Exists("key")
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("Test Exists() success output:%v \n", exists)

	value, err := cache.Get("key")
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("Test Get() success output:%v \n", string(value))

	ttl, err := cache.Ttl("key")
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("Test Ttl() success output:%v \n", ttl)

	del, err := cache.Del("key")
	if err != nil {
		t.Error(err)
	}

	exists, err = cache.Exists("key")
	if del && !exists {
		fmt.Printf("Test Del() success \n")
	}

	err = cache.SetString("key", "value", 10)
	if err != nil {
		t.Error(err)
	} else {
		fmt.Println("Test SetString() success")
	}

	valueStr, err := cache.GetString("key")
	if err != nil {
		t.Error(err)
	} else {
		fmt.Printf("Test GetString() success output:%s \n", valueStr)
	}

	ret, err := cache.Do("SET", "key", "value", "EX", 10)
	if err != nil {
		t.Error(err)
	} else {
		fmt.Printf("Test Do()-SET success output:%v \n", ret)
	}

	ret, err = cache.Do("TTL", "key")
	if err != nil {
		t.Error(err)
	} else {
		fmt.Printf("Test Do()-TTL success output:%v \n", ret)
	}

	ret, err = cache.Do("GET", "key")
	if err != nil {
		t.Error(err)
	} else {
		fmt.Printf("Test Do()-GET success output:%v \n", ret)
	}
}

func TestManager_SCard(t *testing.T) {
	key := "test:scard"
	result, err := cache.SAdd(key, map[string]int{"a": 1, "b": 2})
	assert.Nil(t, err)
	t.Log(result)
	result2, err2 := cache.SCard("test:scard")
	assert.Nil(t, err2)
	t.Log(result2)
	result3, err3 := cache.SMembers(key)
	assert.Nil(t, err3)
	t.Log(result3)
}

func TestManager_SUnion(t *testing.T) {
	key1 := "sunion:1"
	cache.SAdd(key1, "a")
	cache.SAdd(key1, "b")
	key2 := "sunion:2"
	cache.SAdd(key2, "b")
	result, err := cache.SUnion([]string{key1, key2})
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_SInter(t *testing.T) {
	key1 := "sunion:1"
	cache.SAdd(key1, "a")
	cache.SAdd(key1, "b")
	key2 := "sunion:2"
	cache.SAdd(key2, "b")
	result, err := cache.SInter([]string{key1, key2})
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_SRem(t *testing.T) {
	key := "sunion:1"
	result, err := cache.SRem(key, "a", "b")
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_SIsMember(t *testing.T) {
	key := "sunion:1"
	result, err := cache.SIsMember(key, "a")
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_SPop(t *testing.T) {
	key := "sunion:1"
	result, err := cache.SPop(key)
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_SetNX(t *testing.T) {
	key := "setnx:1"
	result, err := cache.SetNX(key, "test", 0)
	assert.Nil(t, err)
	t.Log(result)

	key = "setnx:2"
	result, err = cache.SetNX(key, "test", 10)
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_ZRangeByScoreWithScores(t *testing.T) {
	key := "zrangebyscore"
	cache.Zadd(key, "1", "one")
	cache.Zadd(key, "2", "two")
	result, err := cache.ZRangeByScoreWithScores(key, "-inf", "+inf", 0, 1)
	assert.Nil(t, err)
	if len(result) > 0 {
		for k, v := range result {
			fmt.Printf("k: %s, v: %s\n", k, v)
		}
	}
}

func TestManager_ZRangeByScore(t *testing.T) {
	key := "zrangebyscore"

	result, err := cache.ZRangeByScore(key, 0, -1)
	assert.Nil(t, err)

	for _, item := range result {
		fmt.Println("score", item.Score, "member", item.Member)
	}
}

func TestManager_ZRevRangeByScore(t *testing.T) {
	key := "zrangebyscore"
	result, err := cache.ZRevRangeByScoreWithScores(key, "+inf", "-inf", 0, 1)
	assert.Nil(t, err)
	if len(result) > 0 {
		for k, v := range result {
			fmt.Printf("k: %s, v: %s\n", k, v)
		}
	}
}

func TestManager_HGet(t *testing.T) {
	key := "hget"
	cache.Hset(key, "a", "1")
	result, err := cache.HGet(key, "a")
	assert.Nil(t, err)
	t.Log(result)
}

func TestManager_HExists(t *testing.T) {
	key := "hget"
	flag, err := cache.HExists(key, "a")
	assert.Nil(t, err)
	assert.Equal(t, true, flag)

	flag, err = cache.HExists(key, "b")
	assert.Nil(t, err)
	assert.Equal(t, false, flag)
}

func TestManager_HIncrBy(t *testing.T) {
	key := "hget"
	result, err := cache.HIncrBy(key, "a", 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, result)
}

func TestManager_HIncrByFloat(t *testing.T) {
	key := "hget"
	result, err := cache.HIncrByFloat(key, "a", 3.1415926)
	assert.Nil(t, err)
	assert.Equal(t, 4+3.1415926, result)
}

func TestManager_HDel(t *testing.T) {
	key := "hget"
	result, err := cache.HDel(key, "a")
	assert.Nil(t, err)
	assert.Equal(t, 1, result)
}

func TestManager_HMSet(t *testing.T) {
	key := "hmset"
	flag, err := cache.HMSet(key, "count", 0, "content", "123")
	assert.Nil(t, err)
	assert.Equal(t, "OK", flag)
	result, err := cache.HgetAll(key)
	assert.Nil(t, err)
	if len(result) > 0 {
		for k, v := range result {
			fmt.Printf("%s:%s\n", k, v)
		}
	}
}

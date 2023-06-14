# Redis

Redis是在redigo基础上封装的快速使用包。

## 安装

```shell
go get "github.com/sk-pkg/redis"
```

## 快速开始
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
	redisConn := redis.New(redis.WithAddress(address), redis.WithPrefix(prefix))

	err := redisConn.Set("key", "value", 10)
	if err != nil {
		log.Fatal(err)
	}

	value, err := redisConn.Get("key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("value:%v \n", string(value))
}
```
## 可配置项
1.Address连接地址，此项为必填
```go
redis.WithAddress("localhost:6379")
```
2.Password连接授权密码，此项可选
```go
redis.WithPassword("auth")
```
3.Prefix所有键值前缀，此项可选
```go
redis.WithPrefix("prefix")
```
4.MaxIdle连接池最大空闲连接数，默认值为30，此项可选
```go
redis.WithMaxIdle(10)
```
5.MaxActive连接池所能分配的最大的连接数目，默认值为100，此项可选
```go
redis.WithMaxActive(50)
```
6.DB连接redis的数据库，默认为0，此项可选
```go
redis.WithDB(1)
```
7.IdleTimeout空闲连接超时时间，超过超时时间的空闲连接会被关闭，默认为30秒，此项可选
```go
redis.WithIdleTimeout(30 * time.Second)
```
## 可用方法
1.Set 保存一组指定的<key,value>，ttl<=0时则不过期时间
```go
err := redis.Set("key", "value", 10)
```
2.Exists 返回一个指定的key是否存在
```go
exists, err := redis.Exists("key")
```
3.Get 返回一个指定key的缓存内容
```go
value, err := redis.Get("key")
```
4.Del 返回删除一个指定key的结果
```go
del, err := redis.Del("key")
```
5.BatchDel 批量删除指定key相关键的所有内容，使用`*key*`查出相关key，然后循环删除
```go
err := redis.BatchDel("key")
```
6.Ttl 返回一个指定key缓存的剩余时间，单位秒
```go
ttl, err := redis.Ttl("key")
```
7.Expire 设置指定key的过期时间
```go
err := redis.Expire("key", 10)
```
8.Lua 执行lua脚本
```go
err := redis.Lua(keyCount, script, []string{"key1", "key2", "args1", "args2"})
```
9.Zscore 获取有序集合成员分数
```go
err := redis.Zscore("key", "member")
```
10.Hset 哈希表 key 中的字段 field 值设为 value
```go
err := redis.Hset("key", "field", "value")
```
11.HgetAll 获取哈希表 key 的所有 field 和 value
```go
result, err := redis.HgetAll("key")
```
12.Zadd 添加数据到有序集合
```go
result, err := redis.Zadd("key", 1, "1")
```
13.Zrange 分值从小到大获取有序集合数据
```go
result, err := redis.Zrange("key", 0, 1)
```
14.Zrevrange 分值从大到小获取有序集合数据
```go
result, err := redis.Zrevrange("key", 0, 1)
```
15.Zcard 获取有序集合成员数
```go
result, err := redis.Zcard("key")
```
16.Zincrby 增加有序集合指定成员分数
```go
result, err := redis.Zincrby("key", 1, "test")
```
17.Zrem 移除有序集中的一个或多个成员，不存在的成员将被忽略.
```go
result, err := redis.Zrem("key", "member")
```
18.Zrank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。
```go
result, err := redis.Zrank("key", "member")
```
19.Hlen 获取哈希表key数量
```go
result, err := redis.Hlen("key")
```
20.SetString 使用set设置简单字符串
```go
result, err := redis.SetString("key", "value")
```
21.GetString 使用get获取字符串
```go
result, err := redis.GetString("key")
```
22.Do 使用redigo的原生Do方法
```go
result, err := redis.Do("SET", "Key", "Value", "EX", 60)
```

## 使用redigo原生方法
```go
redisConn := redis.New(redis.WithAddress(address), redis.WithPrefix(prefix))

conn := redisConn.ConnPool.Get()
defer conn.Close()

reply, err = conn.Do("EXPIRE", "key", 100)
```
package main

import (
	"fmt"
	"github.com/sk-pkg/redis"
	"log"
)

const (
	address = "127.0.0.1:6379"
	prefix  = "redisTest"
)

func main() {
	redisConn, err := redis.New(redis.WithAddress(address), redis.WithPrefix(prefix))
	if err != nil {
		log.Fatal(err)
	}

	err = redisConn.Set("key", "value", 10)
	if err != nil {
		log.Fatal(err)
	}

	value, err := redisConn.Get("key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("value:%v \n", string(value))
}

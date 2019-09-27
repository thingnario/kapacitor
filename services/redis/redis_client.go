package redis

import (
	"encoding/json"
	"sync"

	"github.com/go-redis/redis"
)

func WriteToRedis(key string, value map[string]interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	client := GetRedisInstance()
	return client.Set(key, b, 0).Err()
}

func ReadFromRedis(key string) (map[string]interface{}, error) {
	client := GetRedisInstance()
	val, err := client.Get(key).Result()
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	err = json.Unmarshal([]byte(val), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

var redisClientInstance *redis.Client
var once sync.Once

func GetRedisInstance() *redis.Client {
	once.Do(func() {
		redisClientInstance = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	})
	return redisClientInstance
}

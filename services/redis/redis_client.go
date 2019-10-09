package redis

import (
	"encoding/json"
	"os"
	"reflect"
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

func ReadAllFromRedis(key_wildcard string) (map[string]map[string]reflect.Value, error) {
	client := GetRedisInstance()
	results := make(map[string]map[string]reflect.Value)
	keys, err := client.Keys(key_wildcard).Result()
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		keylen := len(key_wildcard) - 1
		subkey := key[keylen:]
		response, err := ReadFromRedis(key)
		if err != nil {
			return nil, err
		}
		results[subkey] = make(map[string]reflect.Value)
		for k, v := range response {
			results[subkey][k] = reflect.ValueOf(v)
		}
	}
	return results, nil
}

var redisClientInstance *redis.Client
var once sync.Once

func GetRedisInstance() *redis.Client {
	once.Do(func() {
		addr, exists := os.LookupEnv("REDIS_ADDR")
		if !exists {
			addr = "localhost:6379"
		}
		redisClientInstance = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	})
	return redisClientInstance
}

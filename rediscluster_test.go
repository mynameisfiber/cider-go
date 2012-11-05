package rediscluster

import (
	"testing"
	"github.com/garyburd/redigo/redis"
    "fmt"
)


func TestRedisCluster(t *testing.T) {
    N := 10
    rc := new(RedisCluster)
    
    success := true
    success = success && rc.AddShard(&RedisShard{Id:1, Host: "localhost", Port: 6379, Db: 1})
    success = success && rc.AddShard(&RedisShard{Id:1, Host: "localhost", Port: 6379, Db: 2})
    success = success && rc.AddShard(&RedisShard{Id:1, Host: "localhost", Port: 6379, Db: 3})
    success = success && rc.AddShard(&RedisShard{Id:1, Host: "localhost", Port: 6379, Db: 4})
    if !success {
        t.Fatalf("Could not add shards")
    }

    start := rc.Start()
    if start != CLUSTER_READY {
        t.Fatalf("Could not connect to servers")
    }

    for i := 0; i < N; i++ {
        _, err := rc.Do("SET", fmt.Sprintf("TEST_%d", i), i)
        if err != nil {
            t.Fatalf("Could not set value TEST_%d: %s", i, err)
        }
    }

    pipeline := rc.Pipeline()
    for i := 0; i < N; i++ {
        pipeline.Send("GET", fmt.Sprintf("TEST_%d", i))
    }
    result := pipeline.Execute()
    if len(result) != N {
        t.Fatalf("Did not get enough results back: %d, %s", len(result), result)
    }
    for i, v := range(result) {
        value, err := redis.Int(v, nil)
        if err != nil || value != i {
            t.Fatalf("Did not get proper result back for TEST_%d: %d", i, value)
        }
    }

    for i := 0; i < N; i++ {
        _, err := rc.Do("DEL", fmt.Sprintf("TEST_%d", i))
        if err != nil {
            t.Fatalf("Could not delete key TEST_%d: %s", i, err)
        }
    }
}

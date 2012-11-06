package rediscluster

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"testing"
)

func TestRedisCluster(t *testing.T) {
	N := 1000

	group1 := NewRedisShardGroup(1, &RedisShard{Id: 1, Host: "127.0.0.1", Port: 6379, Db: 1}, &RedisShard{Id: 2, Host: "127.0.0.1", Port: 6379, Db: 2})
	group2 := NewRedisShardGroup(2, &RedisShard{Id: 3, Host: "127.0.0.1", Port: 6379, Db: 3}, &RedisShard{Id: 4, Host: "127.0.0.1", Port: 6379, Db: 4})
	group3 := NewRedisShardGroup(3, &RedisShard{Id: 5, Host: "127.0.0.1", Port: 6379, Db: 5}, &RedisShard{Id: 6, Host: "127.0.0.1", Port: 6379, Db: 6})
	group4 := NewRedisShardGroup(4, &RedisShard{Id: 8, Host: "127.0.0.1", Port: 6379, Db: 8}, &RedisShard{Id: 7, Host: "127.0.0.1", Port: 6379, Db: 7})

	rc := NewRedisCluster(group1, group2, group3, group4)

	if rc == nil {
		t.Fatalf("Could not create redis cluster")
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
		err := pipeline.Send("GET", fmt.Sprintf("TEST_%d", i))
		if err != nil {
			t.Fatalf("Could not send to pipeline: %s: %s", fmt.Sprintf("TEST_%d", i), err)
		}
	}
	result := pipeline.Execute()
	if len(result) != N {
		t.Fatalf("Did not get enough results back: %d, %s", len(result), result)
	}
	for i, v := range result {
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

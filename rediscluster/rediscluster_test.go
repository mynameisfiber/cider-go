package rediscluster

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"testing"
    "log"
    "strings"
)

func TestRedisCluster(t *testing.T) {
	N := 2

	group1 := NewRedisShardGroup(1, NewRedisShard(1, "127.0.0.1", 6379, 1), NewRedisShard(2, "127.0.0.1", 6379, 2))
	group2 := NewRedisShardGroup(2, NewRedisShard(3, "127.0.0.1", 6379, 3), NewRedisShard(4, "127.0.0.1", 6379, 4))
	group3 := NewRedisShardGroup(3, NewRedisShard(5, "127.0.0.1", 6379, 5), NewRedisShard(6, "127.0.0.1", 6379, 6))
	group4 := NewRedisShardGroup(4, NewRedisShard(8, "127.0.0.1", 6379, 8), NewRedisShard(7, "127.0.0.1", 6379, 7))

	rc := NewRedisCluster(group1, group2, group3, group4)

	if rc == nil {
		t.Fatalf("Could not create redis cluster")
	}

	if rc.Status != CLUSTER_READY {
		t.Fatalf("Could not connect to servers")
	}

	for i := 0; i < N; i++ {
        log.Printf("Setting element %d", i)
		_, err := rc.Do(MessageFromString(fmt.Sprintf("SET TEST_%d %d", i, i)))
		if err != nil {
			t.Fatalf("Could not set value TEST_%d: %s", i, err)
		}
	}

	pipeline := rc.Pipeline()
	for i := 0; i < N; i++ {
		err := pipeline.Send(MessageFromString(fmt.Sprintf("GET TEST_%d", i)))
		if err != nil {
			t.Fatalf("Could not send to pipeline: %s: %s", fmt.Sprintf("TEST_%d", i), err)
		}
	}
	result := pipeline.Execute()
	for i, v := range result.Message {
        log.Printf("Result for %d: %s", i, strings.Replace(string(append(v[0], v[1]...)), "\r\n", " : ", -1))
		value, err := redis.Int(append(v[0], v[1]...), nil)
		if err != nil || value != i {
            t.Fatalf("Did not get proper result back for TEST_%d: %d: %s", i, value, err)
		}
	}

	for i := 0; i < N; i++ {
		_, err := rc.Do(MessageFromString(fmt.Sprintf("DEL TEST_%d", i)))
		if err != nil {
			t.Fatalf("Could not delete key TEST_%d: %s", i, err)
		}
	}
}

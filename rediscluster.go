package rediscluster

import (
    "log"
    "fmt"
	"hash/crc32"
)

const (
	CLUSTER_NONINITIALIZED = iota
	CLUSTER_READY
	CLUSTER_DAMAGED
	CLUSTER_DOWN
)

type RedisCluster struct {
	Shards      []*RedisShard
	NumShards   int
	Status      int
	initialized bool
}

func (rc *RedisCluster) Pipeline() *RedisClusterPipeline {
	return NewRedisClusterPipeline(rc)
}

func (rc *RedisCluster) AddShard(shard *RedisShard) bool {
	if rc.initialized {
		return false
	}
	rc.Shards = append(rc.Shards, shard)
	rc.NumShards += 1
	rc.Status = rc.GetStatus()
	return true
}

func (rc *RedisCluster) Start() int {
    rc.initialized = true
    rc.Status = rc.GetStatus()

    if rc.Status == CLUSTER_READY {
        log.Println("Initialized cluster")
    }
    return rc.Status
}

func (rc *RedisCluster) GetStatus() int {
	if !rc.initialized {
		return CLUSTER_NONINITIALIZED
	}
	allUp, allDown, someDown := true, true, false
	for _, shard := range rc.Shards {
		if shard.Status != REDIS_CONNECTED {
			shard.Connect()
		}
		allUp = allUp && (shard.Status == REDIS_CONNECTED)
		someDown = someDown || (shard.Status != REDIS_CONNECTED)
		allDown = allDown && (shard.Status != REDIS_CONNECTED)
	}
	if allUp && !someDown {
		return CLUSTER_READY
	} else if allUp && someDown {
		return CLUSTER_DAMAGED
	} else if allDown {
		return CLUSTER_DOWN
	}
	return -1
}

func (rc *RedisCluster) Partition(key string) (*RedisShard, uint32) {
	idx := crc32.ChecksumIEEE([]byte(key)) % uint32(rc.NumShards)
	return rc.Shards[idx], idx
}

func (rc *RedisCluster) Do(cmd string, args ...interface{}) (interface{}, error) {
    if !rc.initialized {
        return nil, fmt.Errorf("RedisCluster not initialized")
    }
	db, _ := rc.Partition(args[0].(string))
	response, err := db.rdb.Do(cmd, args...)
	return response, err
}

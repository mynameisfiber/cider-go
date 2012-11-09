package rediscluster

import (
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
	ShardGroups []*RedisShardGroup
	NumShards   uint32
	Status      int
	initialized bool
}

func NewRedisCluster(redisShardGroups ...*RedisShardGroup) *RedisCluster {
	rc := RedisCluster{}
	for idx := range redisShardGroups {
		if !rc.AddShardGroup(redisShardGroups[idx]) {
			return nil
		}
	}
	rc.Start()
	return &rc
}

func (rc *RedisCluster) Pipeline() *RedisClusterPipeline {
	return NewRedisClusterPipeline(rc)
}

func (rc *RedisCluster) AddShardGroup(shard *RedisShardGroup) bool {
	if rc.initialized {
		return false
	}
	rc.ShardGroups = append(rc.ShardGroups, shard)
	rc.NumShards += 1
	return true
}

func (rc *RedisCluster) Start() int {
	rc.initialized = true
	rc.Status = rc.GetStatus()
	return rc.Status
}

func (rc *RedisCluster) Stop() int {
	rc.initialized = false
	rc.Status = rc.GetStatus()
	return rc.Status
}

func (rc *RedisCluster) GetStatus() int {
	if !rc.initialized {
		return CLUSTER_NONINITIALIZED
	}
	allUp, allDown, someDown := true, true, false
	for _, shardGroup := range rc.ShardGroups {
		if shardGroup.Status != GROUP_CONNECTED {
			shardGroup.GetStatus()
		}
		allUp = allUp && (shardGroup.Status == GROUP_CONNECTED)
		someDown = someDown || (shardGroup.Status != GROUP_CONNECTED)
		allDown = allDown && (shardGroup.Status == GROUP_DISCONNECTED)
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

func (rc *RedisCluster) Partition(key string) (*RedisShardGroup, uint32) {
	idx := crc32.ChecksumIEEE([]byte(key)) % uint32(rc.NumShards)
	return rc.ShardGroups[idx], idx
}

func (rc *RedisCluster) Do(req *RedisMessage) (*RedisMessage, error) {
	if !rc.initialized {
		return nil, fmt.Errorf("RedisCluster not initialized")
	}
	db, _ := rc.Partition(string(req.Key))
	response, err := db.Do(req)
	return response, err
}

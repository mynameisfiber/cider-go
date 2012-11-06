package rediscluster

import (
	"fmt"
	"log"
)

const (
	GROUP_UNINITIALIZED = iota
	GROUP_DISCONNECTED
	GROUP_CONNECTED
	GROUP_DAMAGED
)

var (
	// NOT SUPPORTED:
	//  * MGET
	//  * MSET
	//  * SPOP
	//  * RENAME
	//  * MOVE
	//  * RENAMENX
	//  * SDIFFSTORE
	//  * SELECT
	//  * SINTERSTORE
	//  * SMOVE
	//  * SUNION
	//  * SUNIONSTORE
	//  * ZINTERSTORE
	//  * ZUNIONSTORE
	WRITE_OPERATIONS = map[string]bool{
		"EXPIRE":           true,
		"EXPIREAT":         true,
		"SET":              true,
		"SETEX":            true,
		"SETNX":            true,
		"SETRANGE":         true,
		"GETSET":           true,
		"SREM":             true,
		"INCR":             true,
		"INCRBY":           true,
		"INCRBYFLOAT":      true,
		"LINSERT":          true,
		"LPOP":             true,
		"LPUSH":            true,
		"LREM":             true,
		"LSET":             true,
		"LTRIM":            true,
		"HSET":             true,
		"HSETNX":           true,
		"HDEL":             true,
		"HINCRBY":          true,
		"HINCRBYFLOAT":     true,
		"DEL":              true,
		"PERSIST":          true,
		"PEXPIRE":          true,
		"PEXPIREAT":        true,
		"PSETEX":           true,
		"RESTORE":          true,
		"RPOPLPUSH":        true,
		"RPUSH":            true,
		"RPUSHX":           true,
		"SADD":             true,
		"SAVE":             true,
		"SCRIPT":           true,
		"SHUTDOWN":         true,
		"SYNC":             true,
		"ZADD":             true,
		"ZINCRBY":          true,
		"ZREM":             true,
		"ZREMRANGEBYRANK":  true,
		"ZREMRANGEBYSCORE": true,
	}
)

type RedisShardGroup struct {
	Id        int
	Shards    []*RedisShard
	Status    int
	NumShards uint32

	readShard   uint32
	initialized bool
}

func NewRedisShardGroup(id int, redisShards ...*RedisShard) *RedisShardGroup {
	rsg := RedisShardGroup{Id: id}
	for idx := range redisShards {
		if !rsg.AddShard(redisShards[idx]) {
			return nil
		}
	}
	rsg.Start()
	return &rsg
}

func (rsg *RedisShardGroup) AddShard(shard *RedisShard) bool {
	if rsg.initialized {
		return false
	}
	rsg.Shards = append(rsg.Shards, shard)
	rsg.NumShards += 1
	return true
}

func (rsg *RedisShardGroup) GetStatus() int {
	if !rsg.initialized {
		rsg.Status = GROUP_UNINITIALIZED
		return GROUP_UNINITIALIZED
	}
	allUp, allDown, someDown := true, true, false
	for _, shard := range rsg.Shards {
		if shard.Status != REDIS_CONNECTED {
			shard.Connect()
		}
		allUp = allUp && (shard.Status == REDIS_CONNECTED)
		someDown = someDown || (shard.Status != REDIS_CONNECTED)
		allDown = allDown && (shard.Status != REDIS_CONNECTED)
	}
	if allUp && !someDown {
		rsg.Status = GROUP_CONNECTED
		return GROUP_CONNECTED
	} else if allUp && someDown {
		rsg.Status = GROUP_DAMAGED
		return GROUP_DAMAGED
	} else if allDown {
		rsg.Status = GROUP_DISCONNECTED
		return GROUP_DISCONNECTED
	}
	rsg.Status = -1
	return -1
}

func (rsg *RedisShardGroup) Start() int {
	rsg.initialized = true
	rsg.Status = rsg.GetStatus()
	return rsg.Status
}

func (rsg *RedisShardGroup) Stop() int {
	rsg.initialized = false
	rsg.Status = rsg.GetStatus()
	log.Println("[group %d] Stopping Shard Group", rsg.Id)
	return rsg.Status
}

func (rsg *RedisShardGroup) Do(cmd string, args ...interface{}) (interface{}, error) {
	if !rsg.initialized {
		return nil, fmt.Errorf("RedisShardGroup not initialized")
	}
	if _, is_write := WRITE_OPERATIONS[cmd]; is_write {
		var finalError, err error
		var response interface{}
		for _, shard := range rsg.Shards {
			// TODO: Right now we only capture the last response and the last error... what is a good fix?
			response, err = shard.rdb.Do(cmd, args...)
			if err != nil {
				finalError = err
			}
		}
		return response, finalError
	} else {
		// TODO: deal with shards that are down
		db, _ := rsg.GetNextShard()
		response, err := db.rdb.Do(cmd, args...)
		return response, err
	}
	return nil, fmt.Errorf("Unknown error")
}

func (rsg *RedisShardGroup) GetNextShard() (*RedisShard, uint32) {
	shard := rsg.Shards[rsg.readShard]
	index := rsg.readShard
	rsg.readShard = (rsg.readShard + 1) % rsg.NumShards
	return shard, index
}

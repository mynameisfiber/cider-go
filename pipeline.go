package rediscluster

import (
	"github.com/garyburd/redigo/redis"
)

type RedisClusterPipeline struct {
	cluster     *RedisCluster
	numRequests uint
	numRecieved uint
	ordering    []uint32
	shardsUsed  map[uint32]bool
}

func NewRedisClusterPipeline(cluster *RedisCluster) *RedisClusterPipeline {
	rcp := &RedisClusterPipeline{
		cluster:    cluster,
		shardsUsed: make(map[uint32]bool),
	}
	return rcp
}

func (rcp *RedisClusterPipeline) Send(cmd string, args ...interface{}) error {
	client, id := rcp.cluster.Partition(args[0].(string))
	if _, ok := rcp.shardsUsed[id]; !ok {
		client.rdb.Send("MULTI")
	}
	err := client.rdb.Send(cmd, args...)
	if err != nil {
		return err
	}
	rcp.ordering = append(rcp.ordering, id)
	rcp.shardsUsed[id] = true
	rcp.numRequests += 1
	return nil
}

func (rcp *RedisClusterPipeline) Execute() []interface{} {
	data := make(map[uint32][]interface{})
	indexes := make(map[uint32]int)
	var err error
	for id, _ := range rcp.shardsUsed {
		data[id], err = redis.MultiBulk(rcp.cluster.Shards[id].rdb.Do("EXEC"))
		if err != nil {
			data[id] = nil
		} else {
			indexes[id] = 0
		}
	}

	results := make([]interface{}, rcp.numRequests)
	for index, shardId := range rcp.ordering[rcp.numRecieved:] {
		dataIndex, ok := indexes[shardId]
		if data[shardId] != nil && ok {
			results[index] = data[shardId][dataIndex]
			indexes[shardId] += 1
		}
	}
	rcp.numRecieved = rcp.numRequests
	return results
}

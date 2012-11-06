package rediscluster

import (
	"github.com/garyburd/redigo/redis"
)

type RedisClusterPipeline struct {
	cluster     *RedisCluster
	numRequests uint
	numRecieved uint
	ordering    [][2]uint32
	shardGroupsUsed  map[[2]uint32]bool
}

func NewRedisClusterPipeline(cluster *RedisCluster) *RedisClusterPipeline {
	rcp := RedisClusterPipeline{
		cluster:    cluster,
		shardGroupsUsed: make(map[[2]uint32]bool),
	}
	return &rcp
}

func (rcp *RedisClusterPipeline) Send(cmd string, args ...interface{}) error {
	group, groupId := rcp.cluster.Partition(args[0].(string))
    shard, shardId := group.GetNextShard()
    dbId := [2]uint32{groupId, shardId}
	if _, ok := rcp.shardGroupsUsed[dbId]; !ok {
		shard.rdb.Send("MULTI")
	}
	err := shard.rdb.Send(cmd, args...)
	if err != nil {
		return err
	}
	rcp.ordering = append(rcp.ordering, [2]uint32{groupId, shardId})
	rcp.shardGroupsUsed[dbId] = true
	rcp.numRequests += 1
	return nil
}

func (rcp *RedisClusterPipeline) Execute() []interface{} {
	data := make(map[[2]uint32][]interface{})
	indexes := make(map[[2]uint32]int)
	var err error
	for dbId, _ := range rcp.shardGroupsUsed {
        groupId, shardId := dbId[0], dbId[1]
	    data[dbId], err = redis.MultiBulk(rcp.cluster.ShardGroups[groupId].Shards[shardId].rdb.Do("EXEC"))
	    if err != nil {
	    	data[dbId] = nil
	    } else {
	    	indexes[dbId] = 0
	    }
	}

	results := make([]interface{}, rcp.numRequests)
	for index, dbId := range rcp.ordering[rcp.numRecieved:] {
		dataIndex, ok := indexes[dbId]
		if data[dbId] != nil && ok {
			results[index] = data[dbId][dataIndex]
			indexes[dbId] += 1
		}
	}
	rcp.numRecieved = rcp.numRequests
	return results
}

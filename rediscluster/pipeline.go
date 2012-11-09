package rediscluster

import (
)

var (
    MULTI = &RedisMessage{
        Message: []byte("*1\r\n$5\r\nMULTI\r\n"), 
        Command: []byte("MULTI"),
    }
    EXEC = &RedisMessage{
        Message: []byte("*1\r\n$4\r\nEXEC\r\n"),
        Command: []byte("EXEC"),
    }
)

type RedisClusterPipeline struct {
	cluster         *RedisCluster
	numRequests     uint
	numRecieved     uint
	ordering        [][2]uint32
	shardGroupsUsed map[[2]uint32]bool
}

func NewRedisClusterPipeline(cluster *RedisCluster) *RedisClusterPipeline {
	rcp := RedisClusterPipeline{
		cluster:         cluster,
		shardGroupsUsed: make(map[[2]uint32]bool),
	}
	return &rcp
}

func (rcp *RedisClusterPipeline) Send(message *RedisMessage) error {
	group, groupId := rcp.cluster.Partition(string(message.Key))
	shard, shardId := group.GetNextShard()
	dbId := [2]uint32{groupId, shardId}
	if _, ok := rcp.shardGroupsUsed[dbId]; !ok {
		shard.Do(MULTI)
	}
	err := shard.Send(message)
	if err != nil {
		return err
	}
	rcp.ordering = append(rcp.ordering, [2]uint32{groupId, shardId})
	rcp.shardGroupsUsed[dbId] = true
	rcp.numRequests += 1
	return nil
}

func (rcp *RedisClusterPipeline) Execute() []*RedisMessage {
	data := make(map[[2]uint32][]*RedisMessage)
	indexes := make(map[[2]uint32]int)
	var err error
	for dbId, _ := range rcp.shardGroupsUsed {
		groupId, shardId := dbId[0], dbId[1]
        rcp.cluster.ShardGroups[groupId].Shards[shardId].Do(EXEC)
		data[dbId], err = rcp.cluster.ShardGroups[groupId].Shards[shardId].Conn.ReadMessages()
		if err != nil {
			data[dbId] = nil
		} else {
			indexes[dbId] = 0
		}
	}

	results := make([]*RedisMessage, rcp.numRequests)
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

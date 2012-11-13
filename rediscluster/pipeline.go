package rediscluster

import (
	"fmt"
	"log"
)

var (
	MULTI = MessageFromString("MULTI")
	EXEC  = MessageFromString("EXEC")
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

func (rcp *RedisClusterPipeline) Active() bool {
	return (rcp.numRequests-rcp.numRecieved != 0)
}

func (rcp *RedisClusterPipeline) Send(message *RedisMessage) (*RedisMessage, error) {
	group, groupId := rcp.cluster.Partition(message.Key())
	shard, shardId := group.GetNextShard()
	dbId := [2]uint32{groupId, shardId}
	if _, ok := rcp.shardGroupsUsed[dbId]; !ok {
		shard.Do(MULTI)
	}
	msg, err := shard.Do(message)
	if err != nil || msg.String() != "+QUEUED\r\n" {
		return msg, err
	}
	rcp.ordering = append(rcp.ordering, [2]uint32{groupId, shardId})
	rcp.shardGroupsUsed[dbId] = true
	rcp.numRequests += 1
	return msg, nil
}

func (rcp *RedisClusterPipeline) Execute() *RedisMessage {
	data := make(map[[2]uint32][][2][]byte)
	indexes := make(map[[2]uint32]int)
	for dbId, _ := range rcp.shardGroupsUsed {
		groupId, shardId := dbId[0], dbId[1]
		msg, err := rcp.cluster.ShardGroups[groupId].Shards[shardId].Do(EXEC)
		if err != nil {
			log.Println("Could not get pipeline result: %s", err)
			data[dbId] = nil
		} else {
			indexes[dbId] = 0
			if len(msg.Message) == 1 {
				// special case for inline response
				data[dbId] = msg.Message
			} else {
				data[dbId] = msg.Message[1:]
			}
		}
	}

	results := RedisMessage{}
	results.Message = make([][2][]byte, rcp.numRequests+1)
	results.Message[0][0] = []byte(fmt.Sprintf("*%d\r\n", rcp.numRequests))
	for index, dbId := range rcp.ordering[rcp.numRecieved:] {
		dataIndex, ok := indexes[dbId]
		if data[dbId] != nil && ok {
			results.Message[index+1] = data[dbId][dataIndex]
			indexes[dbId] += 1
		}
	}
	rcp.numRecieved = rcp.numRequests
	return &results
}

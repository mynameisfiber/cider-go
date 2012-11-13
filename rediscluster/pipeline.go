package rediscluster

import (
	"fmt"
	"log"
)

var (
	MULTI  = MessageFromString("MULTI")
	EXEC   = MessageFromString("EXEC")
	QUEUED = MessageFromString("+QUEUED\r\n")
)

type RedisClusterPipeline struct {
	cluster            *RedisCluster
	numRequests        uint
	numRecieved        uint
	ordering           []*OrderItem
	shardGroupsUsed    map[[2]int]bool
	shardGroupsToClear map[[2]int]bool
}

type OrderItem struct {
	DbId [2]int
	Used bool
}

func NewRedisClusterPipeline(cluster *RedisCluster) *RedisClusterPipeline {
	rcp := RedisClusterPipeline{
		cluster:            cluster,
		shardGroupsUsed:    make(map[[2]int]bool),
		shardGroupsToClear: make(map[[2]int]bool),
	}
	return &rcp
}

func (rcp *RedisClusterPipeline) Active() bool {
	return (rcp.numRequests-rcp.numRecieved != 0)
}

func (rcp *RedisClusterPipeline) sendShards(message *RedisMessage, groupId int, shards ...*RedisShard) error {
	for _, shard := range shards {
		dbId := [2]int{groupId, shard.Id}
		if _, ok := rcp.shardGroupsUsed[dbId]; !ok {
			shard.Do(MULTI)
		}
		msg, err := shard.Do(message)
		if err != nil || msg.String() != "+QUEUED\r\n" {
			return err
		}
	}
	return nil
}

func (rcp *RedisClusterPipeline) Send(message *RedisMessage) (*RedisMessage, error) {
	group, groupId := rcp.cluster.Partition(message.Key())
	shard, shardId := group.GetNextShard()
	dbId := [2]int{groupId, 0}

	var err error
	if _, is_write := WRITE_OPERATIONS[message.Command()]; is_write {
		err = rcp.sendShards(message, groupId, group.Shards...)
		for i, s := range group.Shards {
			dbId[1] = i
			used := (s.Id == shard.Id)
			rcp.ordering = append(rcp.ordering, &OrderItem{DbId: dbId, Used: used})
			rcp.shardGroupsUsed[dbId] = true
		}
	} else {
		err = rcp.sendShards(message, groupId, shard)
		dbId[1] = shardId
		rcp.ordering = append(rcp.ordering, &OrderItem{DbId: dbId, Used: true})
		rcp.shardGroupsUsed[dbId] = true
	}

	rcp.numRequests += 1
	return QUEUED, err
}

func (rcp *RedisClusterPipeline) Execute() *RedisMessage {
	data := make(map[[2]int][][2][]byte)
	indexes := make(map[[2]int]int)
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
	index := 0
	for _, orderItem := range rcp.ordering[rcp.numRecieved:] {
		if orderItem.Used {
			dbId := orderItem.DbId
			dataIndex, ok := indexes[dbId]
			if data[dbId] != nil && ok {
				results.Message[index+1] = data[dbId][dataIndex]
				indexes[dbId] += 1
			}
			index += 1
		}
	}
	rcp.numRecieved = rcp.numRequests
	return &results
}

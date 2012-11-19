package rediscluster

import (
	"container/list"
	"fmt"
)

var (
	MULTI  = MessageFromString("MULTI")
	EXEC   = MessageFromString("EXEC")
	QUEUED = MessageFromString("+QUEUED\r\n")
)

type RedisClusterPipeline struct {
	Messages        *list.List
	cluster         *RedisCluster
	numRequests     uint
	numRecieved     uint
	ordering        []*OrderItem
	shardGroupsUsed map[[2]int]bool
}

type OrderItem struct {
	DbId [2]int
	Used bool
}

func NewRedisClusterPipeline(cluster *RedisCluster) *RedisClusterPipeline {
	rcp := RedisClusterPipeline{
		cluster:         cluster,
		Messages:        list.New(),
		shardGroupsUsed: make(map[[2]int]bool),
	}
	return &rcp
}

func (rcp *RedisClusterPipeline) Active() bool {
	return (rcp.numRequests-rcp.numRecieved != 0)
}

func (rcp *RedisClusterPipeline) Send(message *RedisMessage) (*RedisMessage, error) {
	rcp.Messages.PushBack(message)
	return QUEUED, nil
}

func (rcp *RedisClusterPipeline) sendShards(shard *RedisShard, message *RedisMessage, dbId [2]int) error {
	_, isMulti := rcp.shardGroupsUsed[dbId]
	if !isMulti {
		shard.Lock()
		shard.Do(MULTI)
	}
	msg, err := shard.Do(message)
	if err != nil || msg.String() != "+QUEUED\r\n" {
		return err
	}
	return nil
}

func (rcp *RedisClusterPipeline) sendOne(message *RedisMessage) error {
	group, groupId := rcp.cluster.Partition(message.Key())
	shard, shardId := group.GetNextShard()
	dbId := [2]int{groupId, 0}

	var err error
	if _, is_write := WRITE_OPERATIONS[message.Command()]; is_write {
		for i, s := range group.Shards {
			dbId[1] = i
			used := (s.Id == shard.Id)
			err = rcp.sendShards(s, message, dbId)

			rcp.ordering = append(rcp.ordering, &OrderItem{DbId: dbId, Used: used})
			rcp.shardGroupsUsed[dbId] = true
			if err != nil {
				break
			}
		}
	} else {
		dbId[1] = shardId
		err = rcp.sendShards(shard, message, dbId)

		rcp.ordering = append(rcp.ordering, &OrderItem{DbId: dbId, Used: true})
		rcp.shardGroupsUsed[dbId] = true
	}

	rcp.numRequests += 1
	return err
}

func (rcp *RedisClusterPipeline) Execute() (*RedisMessage, error) {
	for msg := rcp.Messages.Front(); msg != nil; msg = msg.Next() {
		err := rcp.sendOne(msg.Value.(*RedisMessage))
		if err != nil {
			resp := rcp.executeAll()
			return resp, err
		}
	}
	return rcp.executeAll(), nil
}

func (rcp *RedisClusterPipeline) executeAll() *RedisMessage {
	data := make(map[[2]int][][2][]byte)
	indexes := make(map[[2]int]int)
	for dbId, _ := range rcp.shardGroupsUsed {
		groupId, shardId := dbId[0], dbId[1]
		msg, err := rcp.cluster.ShardGroups[groupId].Shards[shardId].Do(EXEC)
		rcp.cluster.ShardGroups[groupId].Shards[shardId].Unlock()
		if err != nil {
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

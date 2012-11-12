package rediscluster

import (
	"fmt"
	"log"
)

const (
	REDIS_DISCONNECTED = iota
	REDIS_CONNECTED
	REDIS_READONLY
	REDIS_WRITEONLY
)

type RedisShard struct {
	Id        int
	Status    uint32
	LastError error

	Conn *RedisConnection
}

func NewRedisShard(id int, host string, port, db int) *RedisShard {
	var err error
	rs := RedisShard{Id: id}

	rs.Conn, err = NewRedisConnection(host, port, db)
	if err != nil {
		return nil
	}
	return &rs
}

func (rs *RedisShard) Close() {
	rs.Conn.Close()
}

func (rs *RedisShard) GetStatus() uint32 {
	_, err := rs.Conn.WriteBytes([]byte("PING\r\n"))
	if err != nil {
		rs.Status = REDIS_DISCONNECTED
	}

	reply, err := rs.Conn.ReadMessage()
	if err != nil || reply.String() != "+PONG\r\n" {
		rs.Status = REDIS_DISCONNECTED
	} else {
		if rs.Status != REDIS_READONLY && rs.Status != REDIS_WRITEONLY {
			rs.Status = REDIS_CONNECTED
		}
	}
	return rs.Status
}

func (rs *RedisShard) SetMode(mode uint32) error {
	if mode != REDIS_READONLY && mode != REDIS_WRITEONLY {
		return fmt.Errorf("Invalid shard mode: %d", mode)
	}
	rs.Status = mode
	return nil
}

func (rs *RedisShard) Do(req *RedisMessage) (*RedisMessage, error) {
	err := rs.canIssue(req.Command())
	if err != nil {
		return nil, err
	}
	log.Printf("[shard %d] Do'ing on command: %s - %s", rs.Id, req.Command(), req.Key())
	_, err = rs.Conn.WriteMessage(req)
	if err != nil {
		return nil, err
	}
	return rs.Conn.ReadMessage()
}

func (rs *RedisShard) Send(req *RedisMessage) error {
	err := rs.canIssue(req.Command())
	if err != nil {
		return err
	}
	_, err = rs.Conn.WriteMessage(req)
	return err
}

func (rs *RedisShard) canIssue(cmd string) error {
	if rs.Status == REDIS_DISCONNECTED {
		return fmt.Errorf("[shard %d] Shard not connected", rs.Id)
	}
	_, is_write := WRITE_OPERATIONS[cmd]
	if rs.Status == REDIS_READONLY && is_write {
		return fmt.Errorf("[shard %d] Shard in read only mode and write operation '%s' issued", rs.Id, cmd)
	} else if rs.Status == REDIS_WRITEONLY && !is_write {
		return fmt.Errorf("[shard %d] Shard in write only mode and read operation '%s' issued", rs.Id, cmd)
	}
	return nil
}

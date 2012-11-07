package rediscluster

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
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
	Host      string
	Port      int
	Db        int
	Status    uint32
	LastError error

	rdb redis.Conn
}

func (rs *RedisShard) Connect() uint32 {
	var err error
	rs.rdb, err = redis.Dial("tcp", fmt.Sprintf("%s:%d", rs.Host, rs.Port))
	if err == nil {
		status, err := redis.String(rs.rdb.Do("SELECT", rs.Db))
		if status != "OK" || err != nil {
			log.Printf("[shard %d] Could not change to DB %d", rs.Id, rs.Db)
			rs.LastError = err
		} else {
			log.Printf("[shard %d] Connected on %s:%d:%d", rs.Id, rs.Host, rs.Port, rs.Db)
		}
	} else {
		log.Printf("[shard %d] Could not connect: %s", rs.Id, err)
		rs.LastError = err
	}
	return rs.GetStatus()
}

func (rs *RedisShard) Close() {
	rs.rdb.Close()
}

func (rs *RedisShard) GetStatus() uint32 {
	reply, err := redis.String(rs.rdb.Do("PING"))
	if err != nil || reply != "PONG" {
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

func (rs *RedisShard) Do(cmd string, args ...interface{}) (interface{}, error) {
	err := rs.canIssue(cmd)
	if err != nil {
		return nil, err
	}
	return rs.rdb.Do(cmd, args...)
}

func (rs *RedisShard) Send(cmd string, args ...interface{}) error {
	err := rs.canIssue(cmd)
	if err != nil {
		return err
	}
	return rs.rdb.Send(cmd, args...)
}

func (rs *RedisShard) canIssue(cmd string) error {
	if rs.Status == REDIS_DISCONNECTED {
		return fmt.Errorf("[shard %d] Shard not connected")
	}
	_, is_write := WRITE_OPERATIONS[cmd]
	if rs.Status == REDIS_READONLY && is_write {
		return fmt.Errorf("[shard %d] Shard in read only mode and write operation '%s' issued", cmd)
	} else if rs.Status == REDIS_WRITEONLY && !is_write {
		return fmt.Errorf("[shard %d] Shard in write only mode and read operation '%s' issued", cmd)
	}
	return nil
}

package rediscluster

import (
	"fmt"
	"log"
	"net"
)

type RedisConnection struct {
	Host string
	Port int
	Db   int

	Conn net.Conn
	*RedisProtocol
}

func NewRedisConnection(host string, port, db int) (*RedisConnection, error) {
	rc := RedisConnection{Host: host, Port: port, Db: db}
	err := rc.Connect()
	return &rc, err
}

func (rc *RedisConnection) Connect() error {
	var err error

	rc.Conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", rc.Host, rc.Port))
	if err != nil {
		return err
	}

	rc.RedisProtocol = NewRedisProtocol(rc.Conn)

	if err = rc.SelectDb(); err != nil {
		log.Printf("Could not change to DB %d: %s", rc.Db, err)
		return err
	} else {
		log.Printf("Connected on %s:%d:%d", rc.Host, rc.Port, rc.Db)
	}

	return nil
}

func (rc *RedisConnection) SelectDb() error {
	message := MessageFromString(fmt.Sprintf("SELECT %d", rc.Db))
	_, err := rc.WriteMessage(message)
	if err != nil {
		return err
	}

	response, err := rc.ReadMessage()
	if err != nil {
		return err
	}

	if response.String() != "+OK\r\n" {
		return fmt.Errorf("Could not switch databases: %s", response.String())
	}
	return nil
}

func (rc *RedisConnection) Close() {
	rc.Conn.Close()
}

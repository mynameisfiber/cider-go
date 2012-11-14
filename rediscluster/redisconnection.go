package rediscluster

import (
	"fmt"
	"net"
)

type RedisConnection struct {
	Host string
	Port int

	Conn net.Conn
	*RedisProtocol
}

func NewRedisConnection(host string, port int) (*RedisConnection, error) {
	rc := RedisConnection{Host: host, Port: port}
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

	return nil
}

func (rc *RedisConnection) Close() {
	rc.Conn.Close()
}

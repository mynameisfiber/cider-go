package main

import (
	"./rediscluster"
	"bufio"
	"log"
	"net"
)

var redisCluster *rediscluster.RedisCluster
var (
	Clients = uint64(0)
)

type RedisClient struct {
	Conn *net.Conn

	*rediscluster.RedisProtocol
}

func NewRedisClient(conn net.Conn) *RedisClient {
	client := RedisClient{
		Conn: &conn,
		bw:   bufio.NewWriter(conn),
		br:   bufio.NewReader(conn),
	}
	return &client
}

func (rc *RedisClient) Handle() error {
	var err error
	for request, err := rediscluster.ReadRequest(rc.br); err == nil; {
		redisCluster.DoRequest(request)
	}
	return err
}

func main() {
	netAddr := ":6666"
	ln, err := net.Listen("tcp", netAddr)
	if err != nil {
		log.Fatalf("Could not bind to address %s", netAddr)
	}

	group1 := rediscluster.NewRedisShardGroup(1, &rediscluster.RedisShard{Id: 1, Host: "127.0.0.1", Port: 6379, Db: 1}, &rediscluster.RedisShard{Id: 2, Host: "127.0.0.1", Port: 6379, Db: 2})
	group2 := rediscluster.NewRedisShardGroup(2, &rediscluster.RedisShard{Id: 3, Host: "127.0.0.1", Port: 6379, Db: 3}, &rediscluster.RedisShard{Id: 4, Host: "127.0.0.1", Port: 6379, Db: 4})
	group3 := rediscluster.NewRedisShardGroup(3, &rediscluster.RedisShard{Id: 5, Host: "127.0.0.1", Port: 6379, Db: 5}, &rediscluster.RedisShard{Id: 6, Host: "127.0.0.1", Port: 6379, Db: 6})
	group4 := rediscluster.NewRedisShardGroup(4, &rediscluster.RedisShard{Id: 8, Host: "127.0.0.1", Port: 6379, Db: 8}, &rediscluster.RedisShard{Id: 7, Host: "127.0.0.1", Port: 6379, Db: 7})
	redisCluster = rediscluster.NewRedisCluster(group1, group2, group3, group4)
	log.Printf("Started redis clister")

	log.Printf("Listening to connections on %s", netAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		Clients += 1
		log.Printf("Got connection from: %s. %d clients connected", conn.RemoteAddr(), Clients)
		client := NewRedisClient(conn)
		go func() {
			client.Handle()
			Clients -= 1
		}()
	}
}

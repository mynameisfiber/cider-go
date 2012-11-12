package main

import (
	"log"
	"net"
    "./rediscluster"
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
        RedisProtocol: rediscluster.NewRedisProtocol(conn),
	}
	return &client
}

func (rc *RedisClient) Handle() error {
	var err error
    isPipeline := false
    var pipeline *rediscluster.RedisClusterPipeline
	for request, err := rc.ReadMessage(); err == nil; {
        switch request.Command() {
            case "MULTI":
                isPipeline = true
                pipeline = rediscluster.NewRedisClusterPipeline(redisCluster)
                break
            case "EXEC":
                isPipeline = false
                response := pipeline.Execute()
                rc.WriteMessage(response)
                continue
        }

        var response *rediscluster.RedisMessage
        if isPipeline {
            response, err = pipeline.Send(request)
        } else {
            response, err = redisCluster.Do(request)
        }
        rc.WriteMessage(response)
	}
	return err
}

func main() {
	netAddr := ":6666"
	ln, err := net.Listen("tcp", netAddr)
	if err != nil {
		log.Fatalf("Could not bind to address %s", netAddr)
	}

	group1 := rediscluster.NewRedisShardGroup(1, rediscluster.NewRedisShard(1, "127.0.0.1", 6379, 1), rediscluster.NewRedisShard(2, "127.0.0.1", 6379, 2))
	group2 := rediscluster.NewRedisShardGroup(2, rediscluster.NewRedisShard(3, "127.0.0.1", 6379, 3), rediscluster.NewRedisShard(4, "127.0.0.1", 6379, 4))
	group3 := rediscluster.NewRedisShardGroup(3, rediscluster.NewRedisShard(5, "127.0.0.1", 6379, 5), rediscluster.NewRedisShard(6, "127.0.0.1", 6379, 6))
	group4 := rediscluster.NewRedisShardGroup(4, rediscluster.NewRedisShard(8, "127.0.0.1", 6379, 8), rediscluster.NewRedisShard(7, "127.0.0.1", 6379, 7))

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

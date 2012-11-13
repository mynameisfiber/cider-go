package main

import (
	"./rediscluster"
	"log"
	"net"
	"sync/atomic"
	"time"

	"fmt"
	"os"
	"runtime/pprof"
	"strings"
)

var redisCluster *rediscluster.RedisCluster
var (
	Clients = uint64(0)
	OK      = rediscluster.MessageFromString("+OK\r\n")
)

type RedisClient struct {
	Conn           net.Conn
	NumRequests    uint64
	ConnectionTime time.Time
	*rediscluster.RedisProtocol
}

func NewRedisClient(conn net.Conn) *RedisClient {
	client := RedisClient{
		Conn:           conn,
		RedisProtocol:  rediscluster.NewRedisProtocol(conn),
		NumRequests:    0,
		ConnectionTime: time.Now(),
	}
	return &client
}

func (rc *RedisClient) Handle() error {
	var err error

	f, err := os.Create(fmt.Sprintf("%s.pprof", rc.Conn.RemoteAddr()))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	isPipeline := false
	var pipeline *rediscluster.RedisClusterPipeline
	for {
		request, err := rc.ReadMessage()
		if err != nil {
			return err
		}
		log.Printf("Got command: %s", strings.Replace(request.String(), "\r\n", " : ", -1))

		atomic.AddUint64(&rc.NumRequests, 1)
		command := request.Command()
		var response *rediscluster.RedisMessage
		if command == "MULTI" {
			isPipeline = true
			pipeline = rediscluster.NewRedisClusterPipeline(redisCluster)
			response = OK
		} else {
			if command == "EXEC" {
				isPipeline = false
				response := pipeline.Execute()
				rc.WriteMessage(response)
			} else {
				if isPipeline {
					response, err = pipeline.Send(request)
					if err != nil {
						log.Printf("Error getting response: %s", err)
						return err
					}
				} else {
					response, err = redisCluster.Do(request)
					if err != nil {
						log.Printf("Error getting response: %s", err)
						return err
					}
				}
			}
		}
		rc.WriteMessage(response)
	}
	return err
}

func main() {
	netAddr := ":6666"
	listener, err := net.Listen("tcp", netAddr)
	if err != nil {
		log.Fatalf("Could not bind to address %s", netAddr)
	}

	group1 := rediscluster.NewRedisShardGroup(1, rediscluster.NewRedisShard(1, "127.0.0.1", 6379, 1), rediscluster.NewRedisShard(2, "127.0.0.1", 6379, 2))
	group2 := rediscluster.NewRedisShardGroup(2, rediscluster.NewRedisShard(3, "127.0.0.1", 6379, 3), rediscluster.NewRedisShard(4, "127.0.0.1", 6379, 4))
	group3 := rediscluster.NewRedisShardGroup(3, rediscluster.NewRedisShard(5, "127.0.0.1", 6379, 5), rediscluster.NewRedisShard(6, "127.0.0.1", 6379, 6))
	group4 := rediscluster.NewRedisShardGroup(4, rediscluster.NewRedisShard(8, "127.0.0.1", 6379, 8), rediscluster.NewRedisShard(7, "127.0.0.1", 6379, 7))

	redisCluster = rediscluster.NewRedisCluster(group1, group2, group3, group4)
	log.Printf("Started redis cluster")

	log.Printf("Listening to connections on %s", netAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			// handle error
			continue
		}
		client := NewRedisClient(conn)
		go func(client *RedisClient) {
			Clients += 1
			log.Printf("Got connection from: %s. %d clients connected", client.Conn.RemoteAddr(), Clients)
			client.Handle()
			Clients -= 1
			log.Printf("Client disconnected: %s (after %d requests). %d clients connected", client.Conn.RemoteAddr(), client.NumRequests, Clients)
		}(client)
	}
}

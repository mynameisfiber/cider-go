package main

import (
	"./rediscluster"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	//"os"
	//"runtime/pprof"
)

const (
	VERSION = "0.9.2"
)

var NetAddr string
var RedisServers RedisServerList
var redisCluster *rediscluster.RedisCluster

var (
	Clients = uint64(0)
	OK      = rediscluster.MessageFromString("+OK\r\n")
	ERROR   = rediscluster.MessageFromString("-ERROR\r\n")
)

type RedisServerList []*rediscluster.RedisShardGroup

func (rsl *RedisServerList) Set(s string) error {
	for _, group := range strings.Split(s, ";") {
		log.Printf("Creating shard group")
		shardGroup := rediscluster.RedisShardGroup{}
		for _, shard := range strings.Split(group, ",") {
			parts := strings.Split(shard, ":")
			if len(parts) != 2 {
				return fmt.Errorf("Invalid shard format, must be in host:port form: %s", shard)
			}

			id := rand.Intn(8999999) + 1000000
			host := parts[0]
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				return fmt.Errorf("Could not parse port for shard: %s", shard)
			}

			s := rediscluster.NewRedisShard(id, host, port)
			if s == nil {
				return fmt.Errorf("Could not create shard (probably a connection problem): %s", shard)
			}

			log.Printf("[%d] --- Added shard: %s", id, shard)
			shardGroup.AddShard(s)
		}
		shardGroup.Start()
		*rsl = append(*rsl, &shardGroup)
	}
	return nil
}

func (rsp *RedisServerList) String() string {
	return "RedisServerList"
}

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

	//f, err := os.Create(fmt.Sprintf("%s.pprof", rc.Conn.RemoteAddr()))
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()

	isPipeline := false
	var pipeline *rediscluster.RedisClusterPipeline
	for {
		request, err := rc.ReadMessage()
		if err != nil {
			return err
		}
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
				response, err = pipeline.Execute()
				if err != nil {
					log.Printf("Error executing pipeline: %s", err)
					rc.WriteMessage(ERROR)
					continue
				}
			} else {
				if isPipeline {
					response, err = pipeline.Send(request)
					if err != nil {
						log.Printf("Error sending on pipeline: %s", err)
						rc.WriteMessage(ERROR)
						continue
					}
				} else {
					response, err = redisCluster.Do(request)
					if err != nil {
						log.Printf("Error from cluster: %s", err)
						rc.WriteMessage(ERROR)
						continue
					}
				}
			}
		}
		rc.WriteMessage(response)
	}
	return err
}

func main() {
	flag.StringVar(&NetAddr, "net-address", ":6543", "Net address that the redis proxy will listen on")
	flag.Var(&RedisServers, "redis-group", "List of redis shards that form one redundant redis shard-group in the form host:port (may be given multiple times to specify multiple shard-groups)")
	version := flag.Bool("version", false, "Output version number")
	flag.Parse()

	if *version {
		fmt.Printf("redisproxy: Version %s", VERSION)
		return
	}

	if len(RedisServers) == 0 {
		fmt.Println("Missing argument: redis-cluster")
		flag.Usage()
		return
	}

	listener, err := net.Listen("tcp", NetAddr)
	if err != nil {
		log.Fatalf("Could not bind to address %s", NetAddr)
	}

	redisCluster = rediscluster.NewRedisCluster(RedisServers...)
	log.Printf("Started redis cluster")

	log.Printf("Listening to connections on %s", NetAddr)
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

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

var (
	EOL     = []byte("\r\n")
	Clients = uint64(0)
)

type RedisRequest struct {
	Request *bytes.Buffer
	Command []byte
	Key     []byte
}

type RedisClient struct {
	Conn *net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

func NewRedisClient(conn net.Conn) *RedisClient {
	client := RedisClient{
		Conn: &conn,
		bw:   bufio.NewWriter(conn),
		br:   bufio.NewReader(conn),
	}
	return &client
}

func (rc *RedisClient) readLine() ([]byte, error) {
	p, err := rc.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, fmt.Errorf("long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, fmt.Errorf("bad response line terminator")
	}
	return p[:i], nil
}

func (rc *RedisClient) readRequest() (*RedisRequest, error) {
	request := new(RedisRequest)
	request.Request = new(bytes.Buffer)

	n := 1 // n gets changed with the first multi-bulk request
	for i := 0; i < n; i += 1 {
		line, err := rc.readLine()
		request.Request.Write(line)
		request.Request.Write(EOL)
		switch line[0] {
		case '+':
		case '-':
		case ':':
			break
		case '$':
			n, err := strconv.Atoi(string(line[1:]))
			if err != nil || n < 0 {
				return nil, err
			}
			bulk := make([]byte, n)
			_, err = io.ReadFull(rc.br, bulk)

			request.Request.Write(bulk)
			request.Request.Write(EOL)
			if err != nil {
				return nil, err
			}

			if i == 1 {
				request.Command = bulk
			} else if i == 2 {
				request.Key = bulk
			}

			// The following clears out the /r/n on this argument line
			line, err := rc.readLine()
			if err != nil {
				return nil, err
			}
			if len(line) != 0 {
				return nil, fmt.Errorf("Bad bulk format")
			}
			break
		case '*':
			n, err = strconv.Atoi(string(line[1:]))
			if err != nil || n < 0 {
				return nil, err
			}
			break
		default:
			return nil, fmt.Errorf("Unpexected response line")
		}
	}
	return request, nil
}

func (rc *RedisClient) Handle() error {
	var err error
	for request, err := rc.readRequest(); err == nil; {
		log.Printf("Got command '%s' with key '%s'", string(request.Command), string(request.Key))
	}
	return err
}

func main() {
	netAddr := ":6666"
	ln, err := net.Listen("tcp", netAddr)
	if err != nil {
		log.Fatalf("Could not bind to address %s", netAddr)
	}
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

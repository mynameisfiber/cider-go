package rediscluster

import (
    "fmt"
    "bytes"
    "strings"
)

var (
	EOL     = []byte("\r\n")
)

type RedisMessage struct {
    Message []byte
	Command []byte
	Key     []byte
}

func MessageFromString(input string) *RedisMessage {
    message := RedisMessage{}
    parts := strings.Fields(input)
    msgbuf := new(bytes.Buffer)

    msgbuf.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
    for _, comp := range strings.Split(input, " ") {
        msgbuf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(comp), comp))
    }

    message.Message = msgbuf.Bytes()
    message.Command = []byte(parts[0])
    message.Key = []byte(parts[1])

    return &message
}

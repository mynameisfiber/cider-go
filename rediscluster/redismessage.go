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
	Message *bytes.Buffer
	Command []byte
	Key     []byte
}

func MessageFromString(input string) *RedisMessage {
    msg := new(RedisMessage)
    parts := strings.Split(input, " ")

    msg.Message.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
    for _, comp := range strings.Split(input, " ") {
        msg.Message.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(comp), comp))
    }

    msg.Command = []byte(parts[0])
    msg.Key = []byte(parts[1])

    return msg
}

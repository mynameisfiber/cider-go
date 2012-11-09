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
    Parts [][]byte
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
    message.Parts = make([][]byte, len(parts))
    for i, part := range parts {
        message.Parts[i] = []byte(part)
    }

    return &message
}

package rediscluster

import (
    "fmt"
    "bytes"
    "bufio"
    "strconv"
    "io"
    "net"
    "log"
)

type RedisConnection struct {
	Host      string
	Port      int
	Db        int
	Conn  net.Conn

	br   *bufio.Reader
	bw   *bufio.Writer

    pending int
}


func NewRedisConnection(host string, port, db int) (*RedisConnection, error) {
    rc := RedisConnection{Host: host, Port: port, Db: db}
    err := rc.Connect()
    return &rc, err
}

func (rc *RedisConnection) WriteMulti() (int64, error) {
    log.Printf("Writing multi")
    n, err := rc.bw.Write([]byte("*1\r\n$5\r\nMULTI\r\n"))
    if err != nil {
        return 0, err
    }
    return int64(n), nil
}

func (rc *RedisConnection) WriteBytes(message []byte) (int64, error) {
    log.Printf("Writing message: %s", message)
    n, err := rc.bw.Write(message)
    if err != nil {
        return 0, err
    }
    rc.pending += 1
    return int64(n), nil
}

func (rc *RedisConnection) Connect() error {
	var err error

	rc.Conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", rc.Host, rc.Port))
	if err != nil {
        return err
    }
    rc.br = bufio.NewReader(rc.Conn)
    rc.bw = bufio.NewWriter(rc.Conn)

    if err = rc.SelectDb(); err != nil {
		log.Printf("Could not change to DB %d", rc.Db)
        return err
	} else {
		log.Printf("Connected on %s:%d:%d", rc.Host, rc.Port, rc.Db)
	}

	return nil
}

func (rc *RedisConnection) SelectDb() error {
    message := MessageFromString(fmt.Sprintf("SELECT %d", rc.Db))
    _, err := rc.WriteBytes(message.Message)
    if err != nil {
        return err
    }

    response, err := rc.ReadMessage()
    if err != nil {
        return err
    }

    if string(response.Message) != "+OK\r\n" {
        return fmt.Errorf("Could not switch databases")
    }
    return nil
}

func (rc *RedisConnection) readLine() ([]byte, error) {
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

func (rc *RedisConnection) ReadMessages() ([]*RedisMessage, error) {
    log.Printf("Doing %d reads", rc.pending+1)
    messages := make([]*RedisMessage, 1)//rc.pending+1)
    i := 0
    var err error
    for rc.pending >= 0 && len(messages) > i {
        messages[i], err = rc.ReadMessage()
        if err != nil {
            return nil, err
        }
        i += 1
    }
    return messages, nil
}

func (rc *RedisConnection) ReadMessage() (*RedisMessage, error) {
    rc.bw.Flush()

	message := new(RedisMessage)

    parts := make([][2]int, 1)
    var start, end int
    msgbuf := new(bytes.Buffer)
	n := 0 // n gets changed with the first multi-bulk request
	for i := 0; i <= n; i += 1 {
        end = start
		line, err := rc.readLine()
        log.Printf("n = %d, line = %s, pending = %d", n, line, rc.pending)
        if err != nil {
            return nil, err
        }

        start = msgbuf.Len()
		msgbuf.Write(line)
        end = msgbuf.Len()
		msgbuf.Write(EOL)
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
            log.Printf("(also just read '%s')", bulk)

            start = msgbuf.Len()
			msgbuf.Write(bulk)
            end = msgbuf.Len()
			msgbuf.Write(EOL)
			if err != nil {
				return nil, err
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
            newN, err := strconv.Atoi(string(line[1:]))
            n += newN
			if err != nil || n < 0 {
				return nil, err
			}
			break
		default:
			return nil, fmt.Errorf("Unpexected response line")
		}
        if start != end {
            parts = append(parts, [2]int{start, end})
        }
	}

    rc.pending -= 1
    log.Printf("Done with line!")
    message.Message = msgbuf.Bytes()
    message.Parts = make([][]byte, len(parts))
    for i, part := range parts {
        message.Parts[i] = message.Message[part[0]:part[1]] 
    }
	return message, nil
}

func (rc *RedisConnection) Close() {
    rc.Conn.Close()
}

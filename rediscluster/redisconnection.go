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

func (rc *RedisConnection) WriteMessage(message *RedisMessage) (int64, error) {
    n, err := io.Copy(rc.bw, message.Message)
    if err != nil {
        return 0, err
    }
    rc.pending += 1
    return n, nil
}

func (rc *RedisConnection) WriteBytes(message []byte) (int64, error) {
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
    dbString := strconv.Itoa(rc.Db)
    p := []byte(fmt.Sprintf("$2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", len(dbString), dbString))
    message := &RedisMessage{Message: bytes.NewBuffer(p), Command: []byte("SELECT")}

    _, err := rc.WriteMessage(message)
    if err != nil {
        return err
    }

    log.Printf("Wrote message... waiting for reply")
    response, err := rc.ReadMessage()
    log.Printf("Got response: %s", response)
    if err != nil {
        return err
    }

    if response.Message.String() != "+OK\r\n" {
        return fmt.Errorf("Could not switch databases")
    }
    return nil
}

func (rc *RedisConnection) readLine() ([]byte, error) {
    log.Println("starting read")
	p, err := rc.br.ReadSlice('\n')
    log.Printf("Found: %s", p)
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
    messages := make([]*RedisMessage, rc.pending)
    i := 0
    var err error
    for rc.pending > 0 {
        messages[i], err = rc.ReadMessage()
        if err != nil {
            return nil, err
        }
    }
    return messages, nil
}

func (rc *RedisConnection) ReadMessage() (*RedisMessage, error) {
	message := new(RedisMessage)
	message.Message = new(bytes.Buffer)

	n := 1 // n gets changed with the first multi-bulk request
	for i := 0; i <= n; i += 1 {
        log.Printf("Reading message mofo")
		line, err := rc.readLine()
        log.Printf("n = %d, line = %s", n, line)
        if err != nil {
            return nil, err
        }

		message.Message.Write(line)
		message.Message.Write(EOL)
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

			message.Message.Write(bulk)
			message.Message.Write(EOL)
			if err != nil {
				return nil, err
			}

			if i == 1 {
				message.Command = bulk
			} else if i == 2 {
				message.Key = bulk
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

    rc.pending -= 1
	return message, nil
}

func (rc *RedisConnection) Close() {
    rc.Conn.Close()
}

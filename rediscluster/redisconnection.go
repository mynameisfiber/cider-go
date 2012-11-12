package rediscluster

import (
    "bytes"
    "fmt"
    "bufio"
    "strconv"
    "io"
    "net"
    "log"
    "strings"
)

type RedisConnection struct {
	Host      string
	Port      int
	Db        int
	Conn  net.Conn

	br   *bufio.Reader
	bw   *bufio.Writer
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
    log.Printf("Writing message: %s", strings.Replace(string(message), "\r\n", " : ", -1))
    n, err := rc.bw.Write(message)
    if err != nil {
        return 0, err
    }
    return int64(n), nil
}

func (rc *RedisConnection) WriteMessage(message *RedisMessage) (int64, error) {
    return rc.WriteBytes(message.Bytes())
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
        log.Printf("Could not change to DB %d: %s", rc.Db, err)
        return err
	} else {
		log.Printf("Connected on %s:%d:%d", rc.Host, rc.Port, rc.Db)
	}

	return nil
}

func (rc *RedisConnection) SelectDb() error {
    message := MessageFromString(fmt.Sprintf("SELECT %d", rc.Db))
    _, err := rc.WriteMessage(message)
    if err != nil {
        return err
    }

    response, err := rc.ReadMessage()
    if err != nil {
        return err
    }

    if response.String() != "+OK\r\n" {
        return fmt.Errorf("Could not switch databases: %s", response.String())
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
    return p, nil
}

func (rc *RedisConnection) ReadMessage() (*RedisMessage, error) {
    rc.bw.Flush()

    curPart := [2]bytes.Buffer{}
	message := NewRedisMessage()
    inNestedMultiBlock := 0
	n := 0 // n gets changed with the first multi-bulk request
    var err error
    var line []byte
	for i := 0; i <= n; i += 1 {
		line, err = rc.readLine()
        log.Printf("Read line: %s", line)
        if err != nil {
            return nil, err
        }

		switch line[0] {
        case '+', '-', ':':
            _, err = curPart[1].Write(line)
            if err != nil {
                return nil, err
            }
			break
		case '$':
            m, err := strconv.Atoi(string(line[1:len(line)-2]))
			if err != nil || m < 0 {
				return nil, err
			}
            if inNestedMultiBlock == 0 {
                _, err = curPart[0].Write(line)
                if err != nil {
                    return nil, err
                }
            } else {
                _, err = curPart[1].Write(line)
                if err != nil {
                    return nil, err
                }
            }

            if m != -1 {
			    bulk := make([]byte, m)
			    _, err = io.ReadFull(rc.br, bulk)
                log.Printf("Read line: %s", bulk)
                if err != nil {
                    return nil, err
                }

                _, err = curPart[1].Write(bulk)
                if err != nil {
                    return nil, err
                }
                _, err = curPart[1].Write(EOL)
                if err != nil {
                    return nil, err
                }

			    // The following clears out the /r/n on this argument line
			    line, err = rc.readLine()
			    if err != nil {
			    	return nil, err
			    }
			    if len(line) != 2 {
			    	return nil, fmt.Errorf("Bad bulk format")
			    }
            }
			break
		case '*':
            newN, err := strconv.Atoi(string(line[1:len(line)-2]))
            if n != 0 {
                inNestedMultiBlock = newN
            }
            n += newN
			if err != nil || n < 0 {
				return nil, err
			}
            _, err = curPart[0].Write(line)
            if err != nil {
                return nil, err
            }
			break
		default:
			return nil, fmt.Errorf("Unpexected response line")
		}

        if inNestedMultiBlock > 0 {
            inNestedMultiBlock -= 1
        } else {
            a := make([]byte, len(curPart[0]))
            b := make([]byte, len(curPart[1]))
            curPart[0].Read(a)
            curPart[1].Read(b)
            clear
            cd /servcd /ser 
            sudo svc -d /ser    qu  

            message.Message = append(message.Message, [2][]byte{curPart[0].Bytes(), curPart[1].Bytes()})
    
            
            
            vim ser 
            
            :cc
            jjjjjjjjjjjkkkkkkkkkkkkkkkkkkjjjjjjuuut checkout HEAD~1 -- ser  
            
            :e:cc
            /queuereader_
            n/queuereader_Recomm
            /recommendation
            jjjjjjjjjjjjjjjjjjjjjjjjlllllllllllllR3urPart[0].Reset()
            curPart[1].Reset()
        }
	}
    log.Printf("Read response: %s", strings.Replace(message.String(), "\r\n", " : ", -1))
	return message, nil
}

func (rc *RedisConnection) Close() {
    rc.Conn.Close()
}

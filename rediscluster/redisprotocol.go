package rediscluster

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

type RedisProtocol struct {
	br *bufio.Reader
	bw *bufio.Writer
}

type ReadWriter interface {
	io.ReadWriter
}

func NewRedisProtocol(connection ReadWriter) *RedisProtocol {
	return &RedisProtocol{
		br: bufio.NewReader(connection),
		bw: bufio.NewWriter(connection),
	}
}

func (rp *RedisProtocol) WriteMulti() (int64, error) {
	n, err := rp.bw.Write([]byte("*1\r\n$5\r\nMULTI\r\n"))
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func (rp *RedisProtocol) WriteBytes(message []byte) (int64, error) {
	n, err := rp.bw.Write(message)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func (rp *RedisProtocol) WriteMessage(message *RedisMessage) (int64, error) {
	return rp.WriteBytes(message.Bytes())
}

func (rp *RedisProtocol) readLine() ([]byte, error) {
	p, err := rp.br.ReadSlice('\n')
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

func (rp *RedisProtocol) ReadMessage() (*RedisMessage, error) {
	rp.bw.Flush()

	curPart := [2]bytes.Buffer{}
	message := NewRedisMessage()
	inNestedMultiBlock := 0
	n := 0 // n gets changed with the first multi-bulk request
	var err error
	var line []byte
	for i := 0; i <= n; i += 1 {
		line, err = rp.readLine()
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
			m, err := strconv.Atoi(string(line[1 : len(line)-2]))
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
				_, err = io.ReadFull(rp.br, bulk)
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
				line, err = rp.readLine()
				if err != nil {
					return nil, err
				}
				if len(line) != 2 {
					return nil, fmt.Errorf("Bad bulk format")
				}
			}
			break
		case '*':
			newN, err := strconv.Atoi(string(line[1 : len(line)-2]))
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
			a := make([]byte, curPart[0].Len())
			b := make([]byte, curPart[1].Len())
			curPart[0].Read(a)
			curPart[1].Read(b)
			message.Message = append(message.Message, [2][]byte{a, b})
			curPart[0].Reset()
			curPart[1].Reset()
		}
	}
	return message, nil
}

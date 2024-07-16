package main

import (
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
	"github.com/rbongIO/bitcask-go/redis"
	"log"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

var addr = ":6380"

func main() {
	var mu sync.RWMutex
	var ps redcon.PubSub
	rds, err := redis.NewDataStructureType(bitcask.WithDirPath("bitcask-go-redis"))
	go log.Printf("started server at %s", addr)

	err = redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "publish":
				// Publish to all pub/sub subscribers and return the number of
				// messages that were sent.
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				count := ps.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
				conn.WriteInt(count)
			case "subscribe", "psubscribe":
				// Subscribe to a pub/sub channel. The `Psubscribe` and
				// `Subscribe` operations will detach the connection from the
				// event handler and manage all network I/O for this connection
				// in the background.
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				command := strings.ToLower(string(cmd.Args[0]))
				for i := 1; i < len(cmd.Args); i++ {
					if command == "psubscribe" {
						ps.Psubscribe(conn, string(cmd.Args[i]))
					} else {
						ps.Subscribe(conn, string(cmd.Args[i]))
					}
				}
			case "detach":
				hconn := conn.Detach()
				log.Printf("connection has been detached")
				go func() {
					defer hconn.Close()
					hconn.WriteString("OK")
					hconn.Flush()
				}()
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				//mu.Lock()
				rds.Set(cmd.Args[1], 0, cmd.Args[2])
				//muUn.Lock()
				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.RLock()
				val, err := rds.Get(cmd.Args[1])
				mu.RUnlock()
				if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
					conn.WriteError(err.Error())
				}
				if errors.Is(err, bitcask.ErrKeyNotFound) {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				//mu.Lock()
				err := rds.Delete(cmd.Args[1])
				//muUn.Lock()
				if err != nil {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			case "config":
				// This simple (blank) response is only here to allow for the
				// redis-benchmark command to work with this example.
				conn.WriteArray(2)
				conn.WriteBulk(cmd.Args[2])
				conn.WriteBulkString("")
			}
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

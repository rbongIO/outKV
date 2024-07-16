package main

import (
	bitcask "github.com/rbongIO/bitcask-go"
	"github.com/rbongIO/bitcask-go/redis"
	"github.com/tidwall/redcon"
	"strings"
)

var rds *redis.DataStructureType

func init() {
	var err error
	rds, err = redis.NewDataStructureType(bitcask.WithDirPath("bitcask-go-redis"))
	if err != nil {
		panic(err)
	}
}

func main() {

	if err := redcon.ListenAndServe(":6380", handlerCMD, handlerConn, handlerClose); err != nil {
		panic(err)
	}
}
func handlerConn(conn redcon.Conn) bool {
	return true
}
func handlerClose(conn redcon.Conn, err error) {}
func handlerCMD(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "set":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for 'set' command")
			return
		}
		err := rds.Set(cmd.Args[1], 0, cmd.Args[2])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
	case "get":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for 'get' command")
			return
		}
		val, err := rds.Get(cmd.Args[1])
		if err != nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(val)
	}
}

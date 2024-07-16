package main

import (
	"errors"
	bitcask "github.com/rbongIO/bitcask-go"
	"github.com/rbongIO/bitcask-go/redis"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return errors.New("ERR wrong number of arguments for '" + cmd + "' command")
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":    set,
	"get":    get,
	"ping":   nil,
	"quit":   nil,
	"config": nil,
}

type BitcaskClient struct {
	db     *redis.DataStructureType
	server *BitcaskServer
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteString("ERR unknown command '" + command + "'")
		return
	}
	cli, _ := conn.Context().(*BitcaskClient)
	cli.db.Lock()
	defer cli.db.UnLock()
	switch command {
	case "ping":
		conn.WriteString("PONG")
		return
	case "quit":
		conn.WriteString("OK")
		conn.Close()
		return
	case "config":
		conn.WriteArray(2)
		conn.WriteBulk(cmd.Args[2])
		conn.WriteBulkString("")
		return
	default:
		res, err := cmdFunc(cli, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, bitcask.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}
	// set key value
	key, val := args[0], args[1]
	if err := cli.db.Set(key, 0, val); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}
func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	// get key
	val, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return val, nil
}

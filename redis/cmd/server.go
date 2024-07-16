package main

import (
	bitcask "github.com/rbongIO/bitcask-go"
	"github.com/rbongIO/bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const (
	addr = "127.0.0.1:6390"
)

type BitcaskServer struct {
	dbs    map[int]*redis.DataStructureType
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	redisStr, err := redis.NewDataStructureType(bitcask.WithDirPath("bitcask-go-redis"))
	if err != nil {
		panic(err)
	}

	//初始化一个新的 BitcaskServer
	bs := &BitcaskServer{
		dbs: make(map[int]*redis.DataStructureType),
	}
	bs.dbs[0] = redisStr
	// 创建一个新的 redis 服务
	bs.server = redcon.NewServer(addr, execClientCommand, bs.accept, bs.close)
	bs.listen()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.db = svr.dbs[0]
	cli.server = svr
	conn.SetContext(cli)
	return true
}
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

func (bs *BitcaskServer) listen() {
	log.Println("Starting Bitcask server on", addr)
	if err := bs.server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

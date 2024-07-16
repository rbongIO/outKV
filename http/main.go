package main

import (
	"context"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	bitcask "github.com/rbongIO/bitcask-go"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	var err error
	dir, _ := os.MkdirTemp("", "bitcask-go")
	db, err = bitcask.Open(bitcask.WithDirPath(dir), bitcask.WithMaxDataFileSize(64*1024*1024), bitcask.WithSyncWrite(true), bitcask.WithBytePerSync(1024*1024))
	if err != nil {
		panic(err)
	}
}

func getHandler(c context.Context, ctx *app.RequestContext) {
	key := ctx.Query("key")
	val, err := db.Get([]byte(key))
	if err != nil {
		ctx.String(500, err.Error())
		return
	}
	ctx.JSON(200, map[string]string{"key": key, "value": string(val)})
}

func putHandler(c context.Context, ctx *app.RequestContext) {
	var data map[string]string
	err := ctx.BindJSON(&data)
	if err != nil {
		ctx.String(http.StatusBadRequest, err.Error())
		return
	}
	for k, v := range data {
		err := db.Put([]byte(k), []byte(v))
		if err != nil {
			ctx.String(500, err.Error())
			log.Printf("put key %s error: %s", k, err.Error())
			return
		}
	}
	ctx.String(200, `{"status":"ok"}`)
}

func deleteHandler(c context.Context, ctx *app.RequestContext) {
	key := ctx.Query("key")
	err := db.Delete([]byte(key))
	if err != nil {
		ctx.String(500, err.Error())
		return
	}
	ctx.String(200, `{"status":"ok"}`)
}

func ListKeysHandler(c context.Context, ctx *app.RequestContext) {
	var res []string
	keys := db.ListKeys()
	for _, key := range keys {
		res = append(res, string(key))
	}
	ctx.JSON(200, res)
}

func StatsHandler(c context.Context, ctx *app.RequestContext) {
	stats := db.Stat()
	ctx.JSON(200, stats)
}

func main() {
	h := server.Default(server.WithHostPorts(":8080"))
	h.GET("/get", getHandler)
	h.POST("/put", putHandler)
	h.GET("/delete", deleteHandler)
	h.GET("/list_keys", ListKeysHandler)
	h.GET("/stats", StatsHandler)
	h.Spin()
}

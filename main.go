package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"vwap/configs"
	"vwap/sockets"
	"vwap/storages"
)

const (
	printFormat = "%s: %.3f | "
)

var (
	cFile = flag.String("c", "./config.json", "config file path. default is ./config.json")
)

func init() {
	flag.Parse()
}

func main() {
	var storage storages.Storage
	var receiver sockets.SocketClient

	c, err := configs.ReadConfig(*cFile)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	storageCtx, storageCancel := context.WithCancel(ctx)
	socketCtx, socketCancel := context.WithCancel(ctx)

	dataPointsChan := make(chan *sockets.FullMessage)
	vwapCHan := make(chan *storages.Vwap)

	storage = storages.NewRamStorage(storageCtx, dataPointsChan, c, vwapCHan)
	receiver = sockets.NewCoinBaseReciever(socketCtx, dataPointsChan, c)

	go storage.Listen()

	go func() {
		err := receiver.Connect()
		if err != nil {
			log.Printf("[Main]Error processing message %s", err)
			storageCancel()
			socketCancel()
			cancel()
			os.Exit(1)
		}
	}()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

out:
	for {
		select {
		case v := <-vwapCHan:
			b := bytes.Buffer{}
			v.Products.Range(func(key, value interface{}) bool {
				b.WriteString(fmt.Sprintf(printFormat, key, value))
				return true
			})
			fmt.Println(b.String())
		case s := <-sigc:
			log.Printf("[Main]Caught signal %s. Shutting down", s)
			storageCancel()
			socketCancel()
			cancel()
			break out
		default:
			continue
		}
	}
}

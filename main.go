package main

import (
	"awesomeProject/configs"
	"awesomeProject/sockets"
	"awesomeProject/storages"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	c, err := configs.ReadConfig(*cFile)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dataPointsChan := make(chan *sockets.MatchMessage)
	vwapCHan := make(chan *storages.Vwap)
	reciever := sockets.NewCoinBaseReciever(ctx, dataPointsChan, c)
	_ = storages.NewRamStorage(ctx, dataPointsChan, c, vwapCHan)

	go func() {
		err := reciever.Connect()
		if err != nil {
			log.Printf("[Main]Error processing message %s", err)
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
			cancel()
			break out
		default:
			continue
		}
	}
}

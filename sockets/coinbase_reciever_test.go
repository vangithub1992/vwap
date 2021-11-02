package sockets

import (
	"flag"
	"github.com/gorilla/websocket"
	"log"
	"testing"
	"unsafe"
	"vwap/configs"
)

var (
	subscribeStr = `{
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        "BTC-USD",
		"ETH-BTC"
    ],
    "channels": [
        "matches",
        "heartbeat"
    ]
}`
	unsubscribeStr = `{
    "type": "unsubscribe",
    "product_ids": [
        "ETH-USD",
        "BTC-USD",
		"ETH-BTC"
    ],
    "channels": [
        "matches",
        "heartbeat"
    ]
}`
)

func TestCoinBaseReciever_Connect(t *testing.T) {
	fp := flag.String("c", "../config.json", "config filepath")
	flag.Parse()
	c, err := configs.ReadConfig(*fp)
	if err != nil {
		log.Printf("[Test] Error during read config: %s", err)
		t.Fail()
	}

	conn, _, err := websocket.DefaultDialer.Dial(c.Conn.SocketUrl, nil)
	if err != nil {
		log.Printf("[Test]Error connection: %s", err)
		t.Fail()
	}

	if err := conn.WriteMessage(websocket.BinaryMessage, *(*[]byte)(unsafe.Pointer(&subscribeStr))); err != nil {
		log.Printf("[Test]Error sending message: %s", err)
		t.Fail()
	}

	for i := 0; i < 10; i++ {
		msgType, mBody, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[Test]Error during read message: %s", err)
			t.Fail()
		}
		log.Printf("[TEST]Message type %d, body: %s", msgType, mBody)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, *(*[]byte)(unsafe.Pointer(&unsubscribeStr))); err != nil {
		log.Printf("[Test]Error sending message: %s", err)
		t.Fail()
	}
}

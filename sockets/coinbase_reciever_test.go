package sockets

import (
	"github.com/gorilla/websocket"
	"log"
	"testing"
	"unsafe"
)

func TestCoinBaseReciever_Connect(t *testing.T) {
	str := `{
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
	conn, _, err := websocket.DefaultDialer.Dial("wss://ws-feed.exchange.coinbase.com", nil)
	if err != nil {
		log.Printf("[Test]Error connection: %s", err)
		t.Fail()
	}

	if err := conn.WriteMessage(websocket.BinaryMessage, *(*[]byte)(unsafe.Pointer(&str))); err != nil {
		log.Printf("[Test]Error sending message")
	}

	for i := 0; i < 100; i++ {
		msgType, mBody, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[Test]Error during read message: %s", err)
			t.Fail()
		}
		log.Printf("[TEST]Message type %d, body: %s", msgType, mBody)
	}
}

package sockets

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
	"time"
	"vwap/configs"
)

const (
	//request messages types
	subscribe   = "subscribe"
	unsubscribe = "unsubscribe"

	//response messages we have interest in
	match = "match"
)

const zero = 0

//error formats
const (
	jsonMarshallErr  = "[WS]Error marshalling json: %s"
	wsConnErr        = "[WS]Error connection: %s"
	wsSendErr        = "[WS]Error sending message: %s"
	wsReadErr        = "[WS]Error during read message: %s"
	wsUnmarshallErr  = "[WS]Error unmarshalling message: %s"
	wsEmptyBodyErr   = "[WS]Empty body"
	wsCloseConnErr   = "[WS]Error closing connection: %s"
	wsUnsubscribeErr = "[WS]Error unsubscribe: %s"
)

// requestMessage is a message to be sent into socket connection
type requestMessage struct {
	// Type is actual message type (e.g. `subscribe`/`unsubscribe`)
	Type string `json:"type"`
	// ProductIds is a list of Trading pairs we looking for
	ProductIds []string `json:"product_ids"`
	// Channels is a list of channels where we'll read information from coinbase
	Channels []string `json:"channels"`
}

// FullMessage represents merged data from match and heartbeat channels
// example Match message {"type":"match","trade_id":174742474,"maker_order_id":"924c16b5-4163-43f7-98cd-cc77795cbea3","taker_order_id":"1e12f2d5-a2e5-4262-a578-c2f5390288d6","side":"buy","size":"0.02100634","price":"4450.09","product_id":"ETH-USD","sequence":22201266131,"time":"2021-11-02T11:30:50.808413Z"}
// example HeartBeat message {"type":"heartbeat","last_trade_id":23080426,"product_id":"ETH-BTC","sequence":4613181181,"time":"2021-11-02T11:30:50.710150Z"}
type FullMessage struct {
	//Size represent quantity of trading match
	Size string `json:"size,omitempty"`
	// Price represent sum of buying
	Price string `json:"price,omitempty"`
	//all fields below presents in both heartbeat/match messages
	Type      string    `json:"type"`
	ProductId string    `json:"product_id"`
	Sequence  int64     `json:"sequence"`
	Time      time.Time `json:"time"`
}

type CoinBaseReciever struct {
	config       *configs.Config
	messagesChan chan<- *FullMessage
	conn         *websocket.Conn
	ctx          context.Context
}

func NewCoinBaseReciever(cbrCtx context.Context, sentChan chan<- *FullMessage, c *configs.Config) *CoinBaseReciever {
	cbr := &CoinBaseReciever{config: c, ctx: cbrCtx, messagesChan: sentChan}

	return cbr
}

func (cbr *CoinBaseReciever) Connect() (err error) {
	req := cbr.getRequestMessage(subscribe)
	reqJson, err := json.Marshal(req)
	if err != nil {
		log.Printf(jsonMarshallErr, err)
		return err
	}

	cbr.conn, _, err = websocket.DefaultDialer.Dial(cbr.config.Conn.SocketUrl, nil)
	if err != nil {
		log.Printf(wsConnErr, err)
		return err
	}
	//subscribing to channels and trading pairs from config file
	if err := cbr.conn.WriteMessage(websocket.BinaryMessage, reqJson); err != nil {
		log.Printf(wsSendErr, err)
		return err
	}

	defer func(conn *websocket.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf(wsCloseConnErr, err)
		}
	}(cbr.conn)

	for {
		m := &FullMessage{}
		//reading message from socket connection.
		//we don't have interest into message type
		_, mBody, err := cbr.conn.ReadMessage()
		if err != nil {
			log.Printf(wsReadErr, err)
			continue
		}

		if len(mBody) == zero {
			log.Println(wsEmptyBodyErr)
			continue
		}

		if err := json.Unmarshal(mBody, m); err != nil {
			log.Printf(wsUnmarshallErr, err)
			continue
		}

		if m.Type != match {
			continue
		}
		//message passes all conditions and cen be send to storage
		cbr.messagesChan <- m

		select {
		case <-cbr.ctx.Done():
			//catch context done
			m := cbr.getRequestMessage(unsubscribe)
			j, err := json.Marshal(m)
			if err != nil {
				log.Printf("[WS]Error marshalling json message: %s", err)
			}
			//unsubscribing from our trading pairs and channels
			if err := cbr.conn.WriteMessage(websocket.BinaryMessage, j); err != nil {
				log.Printf(wsUnsubscribeErr, err)
				return err
			}
			//closing websocket connection
			if err := cbr.conn.Close(); err != nil {
				log.Printf(wsCloseConnErr, err)
				return err
			}
			return nil
		default:
			continue
		}

	}
}

// getRequestMessage returns default message to subscribe/unsubscribe from channles
func (cbr *CoinBaseReciever) getRequestMessage(mtype string) *requestMessage {
	return &requestMessage{
		Type:       mtype,
		ProductIds: cbr.config.TradingPairs,
		Channels:   cbr.config.Channels,
	}
}

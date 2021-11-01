package sockets

import (
	"awesomeProject/configs"
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
	"time"
)

const (
	//request messages types
	subscribe   = "subscribe"
	unsubscribe = "unsubscribe"

	//response messages types
	heartbeat     = "heartbeat"
	subscriptions = "subscriptions"
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

type requestMessage struct {
	Type       string   `json:"type"`
	ProductIds []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

type MatchMessage struct {
	LastTradeId  int    `json:"last_trade_id,omitempty"`
	TradeId      int    `json:"trade_id,omitempty"`
	MakerOrderId string `json:"maker_order_id,omitempty"`
	TakerOrderId string `json:"taker_order_id,omitempty"`
	Side         string `json:"side,omitempty"`
	Size         string `json:"size,omitempty"`
	Price        string `json:"price,omitempty"`
	//all fields below presents in both heartbeat/match messages
	Type      string    `json:"type"`
	ProductId string    `json:"product_id"`
	Sequence  int64     `json:"sequence"`
	Time      time.Time `json:"time"`
}

type CoinBaseReciever struct {
	config       *configs.Config
	messagesChan chan<- *MatchMessage
	conn         *websocket.Conn
	ctx          context.Context
	cancelF      context.CancelFunc
}

func NewCoinBaseReciever(parentCtx context.Context, sentChan chan<- *MatchMessage, c *configs.Config) *CoinBaseReciever {
	cbrCtx, cancelF := context.WithCancel(parentCtx)
	cbr := &CoinBaseReciever{config: c, ctx: cbrCtx, cancelF: cancelF, messagesChan: sentChan}

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
		m := &MatchMessage{}
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

		if m.Type == heartbeat || m.Type == subscriptions {
			continue
		}

		cbr.messagesChan <- m

		select {
		case <-cbr.ctx.Done():
			m := cbr.getRequestMessage(unsubscribe)
			j, _ := json.Marshal(m)

			if err := cbr.conn.WriteMessage(websocket.BinaryMessage, j); err != nil {
				log.Printf(wsUnsubscribeErr, err)
				return err
			}

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

func (cbr *CoinBaseReciever) getRequestMessage(mtype string) *requestMessage {
	return &requestMessage{
		Type:       mtype,
		ProductIds: cbr.config.TradingPairs,
		Channels:   cbr.config.Channels,
	}
}

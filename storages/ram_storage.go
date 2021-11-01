package storages

import (
	"awesomeProject/configs"
	"awesomeProject/sockets"
	"context"
	"log"
	"strconv"
	"sync"
)

const (
	elementsCount = 200
	zero          = 0
	one           = 1
	bitsSize      = 64
)

const (
	errConvertToArray = "[RamStorage]Error invalid interface"
	errInterfaceType  = "[RamStorage]Error setting type for interface"
)

type RamStorage struct {
	elem         *sync.Map
	messages     <-chan *sockets.MatchMessage
	ctx          context.Context
	cancelF      context.CancelFunc
	config       *configs.Config
	vwap         *Vwap
	vwapMessages chan<- *Vwap
}

func NewRamStorage(parentCtx context.Context, mChan <-chan *sockets.MatchMessage, c *configs.Config, receiveChan chan<- *Vwap) *RamStorage {
	ramCtx, cancelF := context.WithCancel(parentCtx)
	r := &RamStorage{
		elem:     &sync.Map{},
		messages: mChan,
		ctx:      ramCtx,
		cancelF:  cancelF,
		config:   c,
		vwap: &Vwap{
			Products: &sync.Map{},
		},
		vwapMessages: receiveChan,
	}
	for _, pair := range c.TradingPairs {
		r.elem.Store(pair, [elementsCount]*sockets.MatchMessage{})
		r.vwap.Set(pair, float64(zero))
	}
	go r.listen()

	return r
}

func (r *RamStorage) updateStorageCalculateVWap(m *sockets.MatchMessage) {
	needToRefresh := m.ProductId
	for _, v := range r.config.TradingPairs {
		if v == needToRefresh {
			go func() {
				pM, _ := r.elem.Load(needToRefresh)
				productMessages := pM.([elementsCount]*sockets.MatchMessage)
				newVwap := float64(zero)
				quantitySum, priceSum := float64(zero), float64(zero)
				for i := zero; i < elementsCount; i++ {
					if i == elementsCount-one {
						continue
					}
					next := productMessages[i+one]
					q, err := strconv.ParseFloat(m.Size, bitsSize)
					if err != nil {
						continue
					}
					p, err := strconv.ParseFloat(m.Price, bitsSize)
					priceSum += p * q
					quantitySum += q
					productMessages[i] = next
				}
				productMessages[elementsCount-one] = m
				newVwap = priceSum / quantitySum
				r.elem.Store(m.ProductId, productMessages)
				r.vwap.Set(m.ProductId, newVwap)
			}()
		}
	}
	r.vwapMessages <- r.vwap

}

func (r *RamStorage) listen() {
out:
	for {
		select {
		case m := <-r.messages:
			pMInterface, ok := r.elem.Load(m.ProductId)
			if !ok {
				log.Printf(errInterfaceType)
				continue
			}
			_, ok = pMInterface.([elementsCount]*sockets.MatchMessage)
			if !ok {
				log.Printf(errConvertToArray)
				continue
			}
			go func(msg *sockets.MatchMessage) { r.updateStorageCalculateVWap(msg) }(m)
			break
		case <-r.ctx.Done():
			break out
		default:
			continue
		}
	}
}

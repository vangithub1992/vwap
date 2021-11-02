package storages

import (
	"context"
	"log"
	"strconv"
	"sync"
	"vwap/configs"
	"vwap/sockets"
)

const (
	elementsCount = 200
	zero          = 0
	one           = 1
	bitsSize      = 64
)

const (
	errConvertToArray    = "[RamStorage]Error invalid interface"
	errInterfaceType     = "[RamStorage]Error setting type for interface"
	errParsingStrToFloat = "[RamStorage]Error parsing string [%s] to float: %s "
)

// RamStorage need to store last 200 trades for each trading pair
type RamStorage struct {
	// elem is a sync.Map with trading pair as
	//a key and an array of 200 elements for each (e.g. [200]*sockets.FullMessage{})
	//it's safe for Pop first element and Push last without increasing array into memory
	elem *sync.Map
	// messages is a channel to receive new trades from socket
	messages <-chan *sockets.FullMessage
	// vwap is a struct representing actual Volume-Weighted Average Price
	//(https://en.wikipedia.org/wiki/Volume-weighted_average_price) for each trading pair
	vwap *Vwap
	// vwap messages is a channel for sending updated vwap
	vwapMessages chan<- *Vwap
	ctx          context.Context
	config       *configs.Config
}

func NewRamStorage(ramCtx context.Context, mChan <-chan *sockets.FullMessage, c *configs.Config, receiveChan chan<- *Vwap) *RamStorage {
	r := &RamStorage{
		elem:     &sync.Map{},
		messages: mChan,
		ctx:      ramCtx,
		config:   c,
		vwap: &Vwap{
			Products: &sync.Map{},
		},
		vwapMessages: receiveChan,
	}
	for _, pair := range c.TradingPairs {
		r.elem.Store(pair, [elementsCount]*sockets.FullMessage{})
		r.vwap.Set(pair, float64(zero))
	}

	return r
}

func (r *RamStorage) updateStorageCalculateVWap(m *sockets.FullMessage) {
	//looking for our trading pair
	for _, v := range r.config.TradingPairs {
		if v != m.ProductId {
			continue
		}
		//get stored 200 tradings for this pair
		//we need to remove first element from array
		// and add last new element
		// also wee need to recalculate vwap for new messages sequence
		pM, _ := r.elem.Load(m.ProductId)
		productMessages := pM.([elementsCount]*sockets.FullMessage)
		newVwap := float64(zero)
		sizeSum, priceSum := float64(zero), float64(zero)
		//read all messages into bucket except first and calculate
		// price sum and size sum
		for i := zero; i < elementsCount-one; i++ {
			next := productMessages[i+one]
			if next == nil {
				continue
			}
			p, q, err := getPriceAndSize(next)
			if err != nil {
				//we already logged this error in function
				continue
			}
			priceSum += p * q
			sizeSum += q
			productMessages[i] = next
		}
		//now we need to add last new element
		productMessages[elementsCount-one] = m
		np, nq, err := getPriceAndSize(m)
		if err != nil {
			//we already logged this error in function
			return
		}
		//added to vwap new last element
		priceSum += np * nq
		sizeSum += nq
		newVwap = priceSum / sizeSum
		//store updated elements to sync.Map
		r.elem.Store(m.ProductId, productMessages)
		//update vwap map
		r.vwap.Set(m.ProductId, newVwap)
	}
	//send new vwap message to main
	r.vwapMessages <- r.vwap

}

func (r *RamStorage) Listen() {
out:
	for {
		select {
		//received message from socket connection
		case m := <-r.messages:
			pMInterface, ok := r.elem.Load(m.ProductId)
			if !ok {
				log.Printf(errInterfaceType)
				continue
			}
			// trying to convert interface to an array
			_, ok = pMInterface.([elementsCount]*sockets.FullMessage)
			if !ok {
				log.Printf(errConvertToArray)
				continue
			}
			//all ok, let's try to calculate it
			r.updateStorageCalculateVWap(m)
			break
		case <-r.ctx.Done():
			break out
		}
	}
}

//getPriceAndSize returns readable data from websocket messages
//since they store  all data into strings ¯\_(ツ)_/¯
func getPriceAndSize(m *sockets.FullMessage) (p, q float64, err error) {
	q, err = strconv.ParseFloat(m.Size, bitsSize)
	if err != nil {
		log.Printf(errParsingStrToFloat, m.Size, err)
		return 0, 0, err
	}
	p, err = strconv.ParseFloat(m.Price, bitsSize)
	if err != nil {
		log.Printf(errParsingStrToFloat, m.Price, err)
		return 0, 0, err
	}

	return p, q, nil
}

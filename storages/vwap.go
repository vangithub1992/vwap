package storages

import "sync"

func (v *Vwap) Set(key string, value float64) {
	v.Products.Store(key, value)
}

func (v *Vwap) Get(key string) (float64, bool) {
	val, ok := v.Products.Load(key)

	return val.(float64), ok
}

type Vwap struct {
	Products *sync.Map
}

package orderbook

import (
	"errors"

	ob "github.com/joshelb/orderbook"
)

// get Orderbook
func (o Orderbookcollection) GetOrderbook_bySymbol(symbol string) (*ob.OrderBook, error) {
	value, ok := o.Map.Load(symbol)
	if ok {
		return value.(*ob.OrderBook), nil
	} else {
		return nil, errors.New("cant find Symbol")
	}
}

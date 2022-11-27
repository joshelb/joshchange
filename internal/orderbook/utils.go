package orderbook

import (
	"errors"

	ob "github.com/muzykantov/orderbook"
)

func (o Orderbookcollection) GetOrderbook_bySymbol(symbol string) (*ob.OrderBook, error) {
	value, ok := o.Load(symbol)
	if ok {
		return value.(*ob.OrderBook), nil
	} else {
		return nil, errors.New("cant find Symbol")
	}
}

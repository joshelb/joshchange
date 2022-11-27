package orderbook

import (
	"sync"

	ob "github.com/muzykantov/orderbook"
	"github.com/rs/xid"
	"github.com/shopspring/decimal"
	logg "github.com/sirupsen/logrus"
)

type Order struct {
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	Ordertype string  `json:"ordertype"`
	Quantity  float64 `json:"quantity"`
	Price     float64 `json:"price"`
}

type Orderbookcollection struct {
	sync.Map
}

func (o *Orderbookcollection) InitOrderbook(symbol string) {
	o.Store(symbol, ob.NewOrderBook())
	o.Store("hhh", "rage")
	logg.Info(o.Load("btcusd"))
	logg.Info("Initialized Orderbook for Symbol %s", symbol)
}

func (o Orderbookcollection) Marketorder(obj Order) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Info(err)
	}
	if obj.Side == "sell" {
		_, _, _, _, err := orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
	}
	if obj.Side == "buy" {
		_, _, _, _, err := orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
	}
}

func (o Orderbookcollection) Limitorder(obj Order) {
	orderBook, _ := o.GetOrderbook_bySymbol(obj.Symbol)
	if obj.Side == "sell" {
		_, _, _, err := orderBook.ProcessLimitOrder(ob.Sell, xid.New().String(), decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
	}
	if obj.Side == "buy" {
		_, _, _, err := orderBook.ProcessLimitOrder(ob.Buy, xid.New().String(), decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
	}
}

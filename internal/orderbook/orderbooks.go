package orderbook

import (
	"sync"
	"context"

	ob "github.com/joshelb/orderbook"
	"github.com/rs/xid"
	"github.com/go-redis/redis/v8"
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
	Map sync.Map
	RedisClient *redis.Client
}

func (o *Orderbookcollection) InitOrderbook(symbol string) {
	o.Map.Store(symbol, ob.NewOrderBook())
	o.Map.Store("hhh", "rage")
	logg.Info(o.Map.Load("btcusd"))
	logg.Info("Initialized Orderbook for Symbol %s", symbol)
}

func (o Orderbookcollection) Marketorder(obj Order) {
	var ctx = context.Background()
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Error(err)
	}
	if obj.Side == "sell" {
		curPrice, errr := orderBook.CalculatePriceAfterExecution(ob.Sell,decimal.NewFromFloat(obj.Quantity))
		logg.Info(errr)
		_, _, _, _, err := orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		err = o.RedisClient.Set(ctx, "curPrice", curPrice.String(), 0).Err()
		if err != nil {
			logg.Error(err)
		}
	}
	if obj.Side == "buy" {
		curPrice, errr := orderBook.CalculatePriceAfterExecution(ob.Buy,decimal.NewFromFloat(obj.Quantity))
		logg.Info(errr)
		_, _, _, _, err := orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		logg.Info(curPrice)
		err = o.RedisClient.Set(ctx, "curPrice", curPrice.String(), 0).Err()
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

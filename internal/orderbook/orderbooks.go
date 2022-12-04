package orderbook

import (
	"fmt"
	"sync"
	"time"

	ob "github.com/joshelb/orderbook"
	"github.com/roistat/go-clickhouse"
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
	Map              sync.Map
	ClickhouseClient *clickhouse.Conn
}

func (o *Orderbookcollection) InitOrderbook(symbol string) {
	o.Map.Store(symbol, ob.NewOrderBook())
	o.Map.Store("hhh", "rage")
	logg.Info(o.Map.Load("btcusd"))
	logg.Info("Initialized Orderbook for Symbol %s", symbol)
}

func (o Orderbookcollection) Marketorder(obj Order) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Error(err)
	}
	if obj.Side == "sell" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		_, _, _, _, err = orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		unix_timestamp := time.Now().Unix()
		Price, _ := curPrice.Float64()
		q := clickhouse.NewQuery(fmt.Sprintf("INSERT INTO tickdata.%s VALUES (%d,%s,%s,'sell')", obj.Symbol, int(unix_timestamp), fmt.Sprintf("%f", obj.Quantity), fmt.Sprintf("%f", Price)))
		err = q.Exec(o.ClickhouseClient)
		if err != nil {
			logg.Error(err)
		}
	}
	if obj.Side == "buy" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		_, _, _, _, err = orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		unix_timestamp := time.Now().Unix()
		Price, _ := curPrice.Float64()
		q := clickhouse.NewQuery(fmt.Sprintf("INSERT INTO tickdata.%s VALUES (%d,%s,%s,'buy')", obj.Symbol, int(unix_timestamp), fmt.Sprintf("%f", obj.Quantity), fmt.Sprintf("%f", Price)))
		err = q.Exec(o.ClickhouseClient)
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

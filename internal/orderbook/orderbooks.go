package orderbook

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	ob "github.com/joshelb/orderbook"
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
	Map         sync.Map
	MySQLClient *sql.DB
}

func (o *Orderbookcollection) InitOrderbook(symbol string) {
	o.Map.Store(symbol, ob.NewOrderBook())
	o.Map.Store("hhh", "rage")
	logg.Info(o.Map.Load("btcusd"))
	logg.Info("Initialized Orderbook for Symbol %s", symbol)
}

func (o Orderbookcollection) Marketorder(obj Order, userid string) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	if obj.Side == "sell" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		done, partial, partialQuantityProcessed, _, err := orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		trd, err := db.Prepare("INSERT INTO tradeHistory(userid,side,quantity,price,timestamp) VALUES(?,?,?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		_, err = trd.Exec(userid, "sell", obj.Quantity, curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			logg.Error(err)
		}
		processOrders(db, done, partial, userid, partialQuantityProcessed)
	}
	if obj.Side == "buy" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		done, partial, partialQuantityProcessed, _, err := orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		trd, err := db.Prepare("INSERT INTO tradeHistory(userid,side,quantity,price,timestamp) VALUES(?,?,?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		_, err = trd.Exec(userid, "buy", obj.Quantity, curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			logg.Error(err)
		}
		processOrders(db, done, partial, userid, partialQuantityProcessed)
	}
}

func (o Orderbookcollection) Limitorder(obj Order, userid string) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	if obj.Side == "sell" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		ID := xid.New().String()
		done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Sell, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		if done == nil && partial == nil {
			insertOrders(db, "buy", obj.Quantity, fmt.Sprintf("%f", obj.Price), userid, ID)
			return
		}
		trd, err := db.Prepare("INSERT INTO tradeHistory(userid,side,quantity,price,timestamp) VALUES(?,'sell',?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		_, err = trd.Exec(userid, obj.Quantity, curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			logg.Error(err)
		}
		processOrders(db, done, partial, userid, partialQuantityProcessed)
		restOrder := orderBook.Order(ID)
		if restOrder != nil {
			quant, _ := (restOrder.Quantity()).Float64()
			insertOrders(db, (restOrder.Side()).String(), quant, (restOrder.Price()).String(), userid, ID)
		}
	}
	if obj.Side == "buy" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		ID := xid.New().String()
		done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Buy, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		if done == nil && partial == nil {
			insertOrders(db, "buy", obj.Quantity, fmt.Sprintf("%f", obj.Price), userid, ID)
			return
		}
		_, rest, _ := orderBook.CalculateMarketPrice(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		logg.Info(rest)
		trd, err := db.Prepare("INSERT INTO tradeHistory(userid,side,quantity,price,timestamp) VALUES(?,'buy',?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		quant, _ := rest.Float64()
		_, err = trd.Exec(userid, quant, curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			logg.Error(err)
		}
		processOrders(db, done, partial, userid, partialQuantityProcessed)
		restOrder := orderBook.Order(ID)
		if restOrder != nil {
			quant, _ := (restOrder.Quantity()).Float64()
			insertOrders(db, (restOrder.Side()).String(), quant, (restOrder.Price()).String(), userid, ID)
		}

	}
}

func insertOrders(db *sql.DB, side string, quantity float64, price string, userid string, orderid string) {
	stmt, err := db.Prepare("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
	if err != nil {
		logg.Error(err)
	}
	_, err = stmt.Exec(orderid, userid, side, quantity, price, string(time.Now().Unix()))
	if err != nil {
		logg.Error(err)
	}

}

func processOrders(db *sql.DB, done []*ob.Order, partial *ob.Order, userid string, partialQuantityProcessed decimal.Decimal) {
	for _, value := range done {
		stmt, err := db.Prepare("DELETE FROM orders WHERE orderid=?")
		if err != nil {
			logg.Error(err)
		}
		_, err = stmt.Exec(value.ID())
		if err != nil {
			logg.Error(err)
		}
		stmt2, err := db.Prepare("INSERT INTO orderHistory(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		quant, _ := (value.Quantity()).Float64()
		_, err = stmt2.Exec(value.ID(), userid, (value.Side()).String(), quant, (value.Price()).String(), string(time.Now().Unix()))
		if err != nil {
			update, err := db.Prepare("UPDATE orderHistory SET quantity = quantity + ? WHERE orderid = ?")
			if err != nil {
				logg.Error(err)
			}
			_, err = update.Exec(quant, value.ID())
			if err != nil {
				logg.Error(err)
			}
		}
	}
	if partial != nil {
		stmt, err := db.Prepare("UPDATE orders SET quantity = ? WHERE orderid = ?")
		if err != nil {
			logg.Error(err)
		}
		_, err = stmt.Exec((partial.Quantity()).String(), partial.ID())
		if err != nil {
			logg.Error(err)
		}
		stmt2, err := db.Prepare("INSERT INTO orderHistory(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		quant, _ := (partialQuantityProcessed).Float64()
		_, err = stmt2.Exec(partial.ID(), userid, (partial.Side()).String(), quant, (partial.Price()).String(), string(time.Now().Unix()))
		if err != nil {
			update, err := db.Prepare("UPDATE orderHistory SET quantity = quantity + ? WHERE orderid = ?")
			if err != nil {
				logg.Error(err)
			}
			_, err = update.Exec(quant, partial.ID())
			if err != nil {
				logg.Error(err)
			}
		}
	}
}

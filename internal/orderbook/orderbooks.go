package orderbook

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
	MySQLClient      *sql.DB
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
		done, partial, _, _, err := orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		for _, value := range done {
			logg.Info(value.ID())
			query := fmt.Sprintf("DELETE FROM orders WHERE orderid='%s'", value.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
		}
		if partial != nil {
			query := fmt.Sprintf("UPDATE orders SET quantity = '%s' WHERE orderid = '%s'", (partial.Quantity()).String(), partial.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
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
		done, partial, _, _, err := orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		for _, value := range done {
			logg.Info(value.ID())
			query := fmt.Sprintf("DELETE FROM orders WHERE orderid='%s'", value.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
		}
		if partial != nil {
			query := fmt.Sprintf("UPDATE orders SET quantity = '%s' WHERE orderid = '%s'", (partial.Quantity()).String(), partial.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
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

func (o Orderbookcollection) Limitorder(obj Order, userid string) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	if obj.Side == "sell" {
		ID := xid.New().String()
		done, partial, _, err := orderBook.ProcessLimitOrder(ob.Sell, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		if done == nil && partial == nil {
			query := fmt.Sprintf("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES('%s','%s','sell','%s','%s','%s')", ID, userid, fmt.Sprintf("%f", obj.Quantity), fmt.Sprintf("%f", obj.Price), string(time.Now().Unix()))
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
			return
		}
		for _, value := range done {
			query := fmt.Sprintf("DELETE FROM orders WHERE orderid='%s'", value.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}

		}
		if partial != nil {
			query := fmt.Sprintf("UPDATE orders SET quantity = '%s' WHERE orderid = '%s'", (partial.Quantity()).String(), partial.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
		}
	}
	if obj.Side == "buy" {
		ID := xid.New().String()
		done, partial, _, err := orderBook.ProcessLimitOrder(ob.Buy, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		if done == nil && partial == nil {
			query := fmt.Sprintf("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES('%s','%s','buy','%s','%s','%s')", ID, userid, fmt.Sprintf("%f", obj.Quantity), fmt.Sprintf("%f", obj.Price), string(time.Now().Unix()))
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
			return
		}
		for _, value := range done {
			query := fmt.Sprintf("DELETE FROM orders WHERE orderid='%s'", value.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
		}
		if partial != nil {
			query := fmt.Sprintf("UPDATE orders SET quantity = '%s' WHERE orderid = '%s'", (partial.Quantity()).String(), partial.ID())
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec()
			if err != nil {
				logg.Error(err)
			}
		}

	}
}

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
		for _, value := range done {
			logg.Info(value.ID())
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
		for _, value := range done {
			logg.Info(value.ID())
			stmt, err := db.Prepare("DELETE FROM orders WHERE orderid=?")
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec(value.ID())
			if err != nil {
				logg.Info(err)
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
			stmt, err := db.Prepare("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec(ID, userid, "sell", fmt.Sprintf("%f", obj.Quantity), fmt.Sprintf("%f", obj.Price), string(time.Now().Unix()))
			if err != nil {
				logg.Error(err)
			}
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
		restOrder := orderBook.Order(ID)
		if restOrder != nil {
			stmt, err := db.Prepare("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
			if err != nil {
				logg.Error(err)
			}
			quant, _ := (restOrder.Quantity()).Float64()
			_, err = stmt.Exec(ID, userid, (restOrder.Side()).String(), quant, (restOrder.Price()).String(), string(time.Now().Unix()))
			if err != nil {
				logg.Error(err)
			}
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
		logg.Info(done)
		logg.Info(partial)
		if done == nil && partial == nil {
			stmt, err := db.Prepare("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
			if err != nil {
				logg.Error(err)
			}
			_, err = stmt.Exec(ID, userid, "buy", fmt.Sprintf("%f", obj.Quantity), fmt.Sprintf("%f", obj.Price), string(time.Now().Unix()))
			if err != nil {
				logg.Error(err)
			}
			return
		}
		trd, err := db.Prepare("INSERT INTO tradeHistory(userid,side,quantity,price,timestamp) VALUES(?,'buy',?,?,?)")
		if err != nil {
			logg.Error(err)
		}
		_, err = trd.Exec(userid, obj.Quantity, curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			logg.Error(err)
		}
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
		restOrder := orderBook.Order(ID)
		if restOrder != nil {
			stmt, err := db.Prepare("INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)")
			if err != nil {
				logg.Error(err)
			}
			quant, _ := (restOrder.Quantity()).Float64()
			_, err = stmt.Exec(ID, userid, (restOrder.Side()).String(), quant, (restOrder.Price()).String(), string(time.Now().Unix()))
			if err != nil {
				logg.Error(err)
			}
		}

	}
}

package orderbook

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

type CancelOrder struct {
	Symbol  string `json:"symbol"`
	OrderID string `json:"orderid"`
}

type Orderbookcollection struct {
	Map         sync.Map
	MySQLClient *sql.DB
	mu          *sync.Mutex
}

var mx sync.Mutex

func (o *Orderbookcollection) InitOrderbook(symbol string) {
	obook := ob.NewOrderBook()
	var timestamp int
	var obj []byte
	err := o.MySQLClient.QueryRow("SELECT * FROM orderbookKISM_JOSH ORDER BY timestamp DESC LIMIT 1").Scan(&obj, &timestamp)
	if err != nil {
		logg.Error(err)
		o.Map.Store(symbol, ob.NewOrderBook())
		return
	}
	data := obj
	err = obook.UnmarshalJSON(data)
	if err != nil {
		logg.Error(err)
	}
	logg.Info(obook)

	o.Map.Store(symbol, obook)
	logg.Info("Initialized Orderbook for Symbol %s", symbol)
}

func (o Orderbookcollection) Cancelorder(obj CancelOrder, userid string) error {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	symbols := strings.Split((obj.Symbol), ":")
	symbol1 := symbols[0]
	symbol2 := symbols[1]
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	tx, err := db.Begin()
	if err != nil {
		logg.Error(err)
	}
	mx.Lock()
	order := orderBook.CancelOrder(obj.OrderID)
	mx.Unlock()
	if order == nil {
		return errors.New("Order doesnt exist anymore")
	}
	cancelOrderquery := "DELETE FROM orders WHERE orderid = ?"
	_, err = tx.Exec(cancelOrderquery, obj.OrderID)
	if err != nil {
		tx.Rollback()
		logg.Error(err)
		return errors.New("DB delet error")
	}
	val := (order.Quantity()).Mul(order.Price())
	if (order.Side()).String() == "buy" {
		addAvailableBalancebackquery := fmt.Sprintf("Update %s SET AvailableBalance = AvailableBalance + ?  WHERE userid = ?", ("wallet" + symbol2))
		_, err := tx.Exec(addAvailableBalancebackquery, val, userid)
		if err != nil {
			logg.Error(err)
		}
	}
	if (order.Side()).String() == "sell" {
		addAvailableBalancebackquery := fmt.Sprintf("Update %s SET AvailableBalance = AvailableBalance + ?  WHERE userid = ?", ("wallet" + symbol1))
		_, err := tx.Exec(addAvailableBalancebackquery, order.Quantity(), userid)
		if err != nil {
			logg.Error(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		logg.Error(err)
	}
	return nil

}

func (o Orderbookcollection) Marketorder(obj Order, userid string) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	symbols := strings.Split((obj.Symbol), ":")
	symbol1 := symbols[0]
	symbol2 := symbols[1]
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	tx, err := db.Begin()
	if err != nil {
		logg.Error(err)
	}
	if obj.Side == "sell" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		mx.Lock()
		err = validateOrder(db, decimal.NewFromFloat(obj.Quantity), symbol1, userid)
		if err != nil {
			tx.Rollback()
			mx.Unlock()
			return
		}
		done, partial, partialQuantityProcessed, quantityLeft, err := orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
			return
		}
		mx.Unlock()
		logg.Info(partial)
		logg.Info(done)
		if done == nil && partial == nil {
			logg.Error("doesnt work brah")
			query := fmt.Sprintf("UPDATE %s  SET AvailableBalance = AvailableBalance + ? WHERE userid = ?", ("wallet" + symbol1))
			_, err := db.Exec(query, quantityLeft, userid)
			if err != nil {
				logg.Error(err)
			}
			return
		}
		o.ticktoCandlestick(obj, curPrice)
		tradequery := fmt.Sprintf("INSERT INTO tradeHistory%s_%s(uniqueid,userid,side,quantity,price,timestamp) VALUES(?,?,?,?,?,?)", symbol1, symbol2)
		unique_id := xid.New().String()
		_, err = tx.Exec(tradequery, unique_id, userid, "sell", decimal.NewFromFloat(obj.Quantity).Sub(quantityLeft), curPrice.String(), time.Now().Unix())
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		processOrders(tx, "x", db, done, partial, quantityLeft, userid, partialQuantityProcessed, symbol1, symbol2)
		err = tx.Commit()
		if err != nil {
			logg.Error(err)
		}
	}
	if obj.Side == "buy" {
		curPrice, err := orderBook.CalculatePriceAfterExecution(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		mx.Lock()
		price, _, err := orderBook.CalculateMarketPrice(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		err = validateOrder(db, price, symbol2, userid)
		if err != nil {
			tx.Rollback()
			mx.Unlock()
			return
		}
		done, partial, partialQuantityProcessed, quantityLeft, err := orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
			return
		}
		mx.Unlock()
		if done == nil && partial == nil {
			logg.Error("deosnt work brah")
			return
		}
		o.ticktoCandlestick(obj, curPrice)
		tradequery := fmt.Sprintf("INSERT INTO tradeHistory%s_%s(uniqueid,userid,side,quantity,price,timestamp) VALUES(?,?,?,?,?,?)", symbol1, symbol2)
		unique_id := xid.New().String()
		_, err = tx.Exec(tradequery, unique_id, userid, "buy", decimal.NewFromFloat(obj.Quantity).Sub(quantityLeft), curPrice.String(), time.Now().Unix())
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		processOrders(tx, "x", db, done, partial, decimal.NewFromFloat(0), userid, partialQuantityProcessed, symbol1, symbol2)
		err = tx.Commit()
		if err != nil {
			logg.Error(err)
		}
	}
}

func (o Orderbookcollection) Limitorder(obj Order, userid string) {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	symbols := strings.Split((obj.Symbol), ":")
	symbol1 := symbols[0]
	symbol2 := symbols[1]
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	tx, err := db.Begin()
	if err != nil {
		logg.Error(err)
	}
	if obj.Side == "sell" {
		if err != nil {
			logg.Error(err)
		}
		ID := xid.New().String()
		mx.Lock()
		err = validateOrder(db, decimal.NewFromFloat(obj.Quantity), symbol1, userid)
		if err != nil {
			logg.Error(err)
			tx.Rollback()
			mx.Unlock()
			return
		}
		done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Sell, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}

		mx.Unlock()
		if done == nil && partial == nil {
			insertOrders(tx, db, "sell", decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price), userid, ID)
			err = tx.Commit()
			if err != nil {
				logg.Error(err)
			}
			return
		}
		restOrder := orderBook.Order(ID)
		if restOrder == nil {
			processOrders(tx, ID, db, done, partial, decimal.NewFromFloat(0), userid, partialQuantityProcessed, symbol1, symbol2)
		}
		if restOrder != nil {
			processOrders(tx, ID, db, done, partial, restOrder.Quantity(), userid, partialQuantityProcessed, symbol1, symbol2)
			quant := (restOrder.Quantity())
			price := (restOrder.Price())
			side := (restOrder.Side()).String()
			insertOrders(tx, db, (restOrder.Side()).String(), quant, price, userid, ID)
			if side == "sell" {
				updateAvailableBalance(tx, userid, quant, ("wallet" + symbol1))
			}
			if side == "buy" {
				updateAvailableBalance(tx, userid, quant.Mul(price), ("wallet" + symbol2))
			}
		}
		err = tx.Commit()
		if err != nil {
			logg.Error(err)
		}
	}
	if obj.Side == "buy" {
		if err != nil {
			logg.Error(err)
		}
		ID := xid.New().String()
		mx.Lock()
		price, _, err := orderBook.CalculateMarketPrice(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
			price = decimal.NewFromFloat(obj.Quantity).Mul(decimal.NewFromFloat(obj.Price))
		}
		if price.Cmp(decimal.NewFromFloat(obj.Quantity).Mul(decimal.NewFromFloat(obj.Price))) == 1 {
			price = decimal.NewFromFloat(obj.Quantity).Mul(decimal.NewFromFloat(obj.Price))
		}

		logg.Info(price)
		err = validateOrder(db, price, symbol2, userid)
		if err != nil {
			logg.Info(err)
			tx.Rollback()
			mx.Unlock()
			return
		}
		done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Buy, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		mx.Unlock()
		if done == nil && partial == nil {
			insertOrders(tx, db, "buy", decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price), userid, ID)
			err = tx.Commit()
			if err != nil {
				logg.Error(err)
			}
			return
		}
		restOrder := orderBook.Order(ID)
		if restOrder == nil {
			processOrders(tx, ID, db, done, partial, decimal.NewFromFloat(0), userid, partialQuantityProcessed, symbol1, symbol2)
		}
		if restOrder != nil {
			processOrders(tx, ID, db, done, partial, (restOrder.Quantity()).Mul(restOrder.Price()), userid, partialQuantityProcessed, symbol1, symbol2)
			quant := (restOrder.Quantity())
			price := (restOrder.Price())
			side := (restOrder.Side()).String()
			insertOrders(tx, db, (restOrder.Side()).String(), quant, price, userid, ID)
			if side == "sell" {
				updateAvailableBalance(tx, userid, quant, ("wallet" + symbol1))
			}
			if side == "buy" {
				updateAvailableBalance(tx, userid, quant.Mul(price), ("wallet" + symbol2))
			}
		}
		err = tx.Commit()
		if err != nil {
			logg.Error(err)
		}

	}
}

func insertOrders(tx *sql.Tx, db *sql.DB, side string, quantity decimal.Decimal, price decimal.Decimal, userid string, orderid string) {
	insertquery := "INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)"
	_, err := tx.Exec(insertquery, orderid, userid, side, quantity, price, time.Now().Unix())
	if err != nil {
		tx.Rollback()
		logg.Error(err)
		return
	}
}

func updateAvailableBalance(tx *sql.Tx, userid string, quantity decimal.Decimal, wallet string) {
	updateAvailableBalancequery := fmt.Sprintf("Update %s SET AvailableBalance = AvailableBalance - ?  WHERE userid = ?", wallet)
	_, err := tx.Exec(updateAvailableBalancequery, quantity, userid)
	if err != nil {
		tx.Rollback()
		logg.Error(err)
		return
	}

}

// User1 is the maker
func processWalletTransaction(tx *sql.Tx, side string, db *sql.DB, user1 string, user2 string, price decimal.Decimal, quantity decimal.Decimal, wallet1 string, wallet2 string) {
	query := fmt.Sprintf("Update %s SET Balance = Balance + ?, AvailableBalance = AvailableBalance + ?  WHERE userid = ?", wallet1)
	query2 := fmt.Sprintf("Update %s SET Balance = Balance + ?, AvailableBalance = AvailableBalance + ? WHERE userid = ?", wallet2)
	query3 := fmt.Sprintf("Update %s SET Balance = Balance - ?, AvailableBalance = AvailableBalance - ? WHERE userid = ?", wallet1)
	query4 := fmt.Sprintf("Update %s SET Balance = Balance - ?, AvailableBalance = AvailableBalance - ? WHERE userid = ?", wallet2)
	if side == "sell" {
		_, err := tx.Exec(query, quantity, quantity, user2)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query2, quantity.Mul(price), quantity.Mul(price), user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query3, quantity, 0, user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query4, quantity.Mul(price), 0, user2)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
	}
	if side == "buy" {
		_, err := tx.Exec(query, quantity, quantity, user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query2, quantity.Mul(price), quantity.Mul(price), user2)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query3, quantity, 0, user2)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query4, quantity.Mul(price), 0, user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
	}
}

func processOrders(tx *sql.Tx, orderid string, db *sql.DB, done []*ob.Order, partial *ob.Order, left decimal.Decimal, userid string, partialQuantityProcessed decimal.Decimal, symbol1 string, symbol2 string) {
	var ID string
	side := ""
	buy := decimal.NewFromFloat(0)
	sell := decimal.NewFromFloat(0)
	for _, value := range done {
		buy = buy.Add((value.Quantity()).Mul(value.Price()))
		sell = sell.Add(value.Quantity())
		if orderid == value.ID() {
			logg.Info("gi")
			continue
		}
		side = (value.Side()).String()
		query := "SELECT userid from orders WHERE orderid = ?"
		row, err := db.Query(query, value.ID())
		if err != nil {
			logg.Error(err)
		}
		for row.Next() {
			err = row.Scan(&ID)
			if err != nil {
				logg.Info(err)
			}
		}
		row.Close()
		quan := value.Quantity()
		price := value.Price()
		processWalletTransaction(tx, (value.Side()).String(), db, ID, userid, price, quan, "wallet"+symbol1, "wallet"+symbol2)
		deleteOrdersquery := "DELETE FROM orders WHERE orderid=?"
		_, err = tx.Exec(deleteOrdersquery, value.ID())
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		insertOrderHistoryquery := "INSERT INTO orderHistory(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)"
		quant := (value.Quantity())
		_, err = tx.Exec(insertOrderHistoryquery, value.ID(), ID, (value.Side()).String(), quant, (value.Price()).String(), time.Now().Unix())
		if err != nil {
			updateOrderHistoryquery := "UPDATE orderHistory SET quantity = quantity + ? WHERE orderid = ?"
			_, err = tx.Exec(updateOrderHistoryquery, quant, value.ID())
			if err != nil {
				tx.Rollback()
				logg.Error(err)
				return
			}
		}
	}
	if partial != nil {
		if (partial.Side()).String() != side && side != "" {

			if side == "sell" {
				query := fmt.Sprintf("UPDATE %s  SET AvailableBalance = AvailableBalance + ? WHERE userid = ?", ("wallet" + symbol2))
				_, err := tx.Exec(query, left, userid)
				if err != nil {
					logg.Error(err)
				}
			} else {
				query := fmt.Sprintf("UPDATE %s  SET AvailableBalance = AvailableBalance + ? WHERE userid = ?", ("wallet" + symbol1))
				_, err := tx.Exec(query, left, userid)
				if err != nil {
					logg.Error(err)
				}
			}
			return

		}
	}
	if partial != nil {
		buy = buy.Add(partialQuantityProcessed.Mul(partial.Price()))
		sell = sell.Add(partialQuantityProcessed)
		logg.Info(partial)
		if orderid == partial.ID() {
			return
		}
		query := "SELECT userid from orders WHERE orderid = ?"
		row, err := db.Query(query, partial.ID())
		if err != nil {
			logg.Error(err)
		}
		for row.Next() {
			err = row.Scan(&ID)
			if err != nil {
				logg.Info(err)
			}
		}
		row.Close()
		quan := (partialQuantityProcessed)
		price := (partial.Price())
		processWalletTransaction(tx, (partial.Side()).String(), db, ID, userid, price, quan, "wallet"+symbol1, "wallet"+symbol2)
		updateOrdersquery := "UPDATE orders SET quantity = ? WHERE orderid = ?"
		_, err = tx.Exec(updateOrdersquery, (partial.Quantity()).String(), partial.ID())
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		insertOrderHistoryquery := "INSERT INTO orderHistory(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)"
		quant := (partialQuantityProcessed)
		_, err = tx.Exec(insertOrderHistoryquery, partial.ID(), ID, (partial.Side()).String(), quant, (partial.Price()).String(), time.Now().Unix())
		if err != nil {
			updateOrderHistoryquery := "UPDATE orderHistory SET quantity = quantity + ? WHERE orderid = ?"
			_, err = tx.Exec(updateOrderHistoryquery, quant, partial.ID())
			if err != nil {
				tx.Rollback()
				logg.Error(err)
				return
			}
		}
	}

}

func (o Orderbookcollection) ticktoCandlestick(order Order, curprice decimal.Decimal) {
	db := o.MySQLClient
	timeframe := 1
	time_now := time.Now().Unix()
	var (
		timestamp int64
		open      decimal.Decimal
		high      decimal.Decimal
		low       decimal.Decimal
		close     decimal.Decimal
		quantity  decimal.Decimal
	)
	symbols := strings.Split(order.Symbol, ":")
	query := "SELECT * FROM candlestickData" + symbols[0] + symbols[1] + fmt.Sprintf("%dMin", timeframe) + " ORDER BY timestamp DESC LIMIT 1"
	errNoRow := db.QueryRow(query).Scan(&timestamp, &open, &high, &low, &close, &quantity)
	if errNoRow != nil {
		logg.Error(errNoRow)
	}

	if errNoRow != nil {
		logg.Info("heyo")
		query := fmt.Sprintf("INSERT INTO candlestickData%s%s%s (timestamp, open, high, low, close, quantity) VALUES(?,?,?,?,?,?)", symbols[0], symbols[1], fmt.Sprintf("%dMin", timeframe))
		ts_curr_min := time_now - (time_now % 60)
		_, err := db.Exec(query, ts_curr_min, curprice, curprice, curprice, curprice, order.Quantity)
		if err != nil {
			logg.Error(err)
		}
		return
	}

	if timestamp <= time_now && time_now <= timestamp+int64(timeframe*60) {
		low_new := low
		high_new := high
		if curprice.Cmp(high) == 1 {
			high_new = curprice
		}
		if curprice.Cmp(low) == -1 {
			low_new = curprice
		}
		query := fmt.Sprintf("UPDATE candlestickData%s%s%s SET quantity = quantity + ?,low = ? ,high = ?,close = ? WHERE timestamp = ?", symbols[0], symbols[1], fmt.Sprintf("%dMin", timeframe))
		_, err := db.Exec(query, order.Quantity, low_new, high_new, curprice, timestamp)
		if err != nil {
			logg.Error(err)
		}
	} else if timestamp < time_now {
		query := fmt.Sprintf("INSERT INTO candlestickData%s%s%s (timestamp, open, high, low, close, quantity) VALUES(?,?,?,?,?,?)", symbols[0], symbols[1], fmt.Sprintf("%dMin", timeframe))
		ts_curr_min := time_now - (time_now % 60)
		_, err := db.Exec(query, ts_curr_min, curprice, curprice, curprice, curprice, order.Quantity)
		if err != nil {
			logg.Error(err)
		}
	}

}

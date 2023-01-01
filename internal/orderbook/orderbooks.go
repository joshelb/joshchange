package orderbook

import (
	"database/sql"
	"errors"
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

type CancelOrder struct {
	Symbol  string `json:"symbol"`
	OrderID string `json:"orderid"`
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

func (o Orderbookcollection) Cancelorder(obj CancelOrder, userid string) error {
	orderBook, err := o.GetOrderbook_bySymbol(obj.Symbol)
	if err != nil {
		logg.Error(err)
	}
	db := o.MySQLClient
	tx, err := db.Begin()
	if err != nil {
		logg.Error(err)
	}
	order := orderBook.CancelOrder(obj.OrderID)
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
	quan, _ := (order.Quantity()).Float64()
	price, _ := (order.Price()).Float64()
	if (order.Side()).String() == "buy" {
		addAvailableBalancebackquery := fmt.Sprintf("Update %s SET AvailableBalance = AvailableBalance + ?  WHERE userid = ?", "walletusd")
		_, err := tx.Exec(addAvailableBalancebackquery, quan*price, userid)
		if err != nil {
			logg.Error(err)
		}
	}
	if (order.Side()).String() == "sell" {
		addAvailableBalancebackquery := fmt.Sprintf("Update %s SET AvailableBalance = AvailableBalance + ?  WHERE userid = ?", "walletbtc")
		_, err := tx.Exec(addAvailableBalancebackquery, quan, userid)
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
		done, partial, partialQuantityProcessed, quantityLeft, err := orderBook.ProcessMarketOrder(ob.Sell, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		tradequery := "INSERT INTO tradeHistory(uniqueid,userid,side,quantity,price,timestamp) VALUES(?,?,?,?,?,?)"
		quan, _ := (quantityLeft.Float64())
		unique_id := xid.New().String()
		_, err = tx.Exec(tradequery, unique_id, userid, "sell", (obj.Quantity - quan), curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		processOrders(tx, "x", db, done, partial, userid, partialQuantityProcessed, "btc", "usd")
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
		done, partial, partialQuantityProcessed, quantityLeft, err := orderBook.ProcessMarketOrder(ob.Buy, decimal.NewFromFloat(obj.Quantity))
		if err != nil {
			logg.Error(err)
		}
		tradequery := "INSERT INTO tradeHistory(uniqueid,userid,side,quantity,price,timestamp) VALUES(?,?,?,?,?,?)"
		quan, _ := (quantityLeft.Float64())
		unique_id := xid.New().String()
		_, err = tx.Exec(tradequery, unique_id, userid, "buy", (obj.Quantity - quan), curPrice.String(), string(time.Now().Unix()))
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		processOrders(tx, "x", db, done, partial, userid, partialQuantityProcessed, "btc", "usd")
		err = tx.Commit()
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
	tx, err := db.Begin()
	if err != nil {
		logg.Error(err)
	}
	if obj.Side == "sell" {
		if err != nil {
			logg.Error(err)
		}
		ID := xid.New().String()
		done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Sell, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		if done == nil && partial == nil {
			insertOrders(tx, db, "sell", obj.Quantity, obj.Price, userid, ID)
			err = tx.Commit()
			if err != nil {
				logg.Error(err)
			}
			return
		}
		logg.Info("hi")
		processOrders(tx, ID, db, done, partial, userid, partialQuantityProcessed, "btc", "usd")
		restOrder := orderBook.Order(ID)
		if restOrder != nil {
			quant, _ := (restOrder.Quantity()).Float64()
			price, _ := (restOrder.Price()).Float64()
			insertOrders(tx, db, (restOrder.Side()).String(), quant, price, userid, ID)
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
		done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Buy, ID, decimal.NewFromFloat(obj.Quantity), decimal.NewFromFloat(obj.Price))
		if err != nil {
			logg.Error(err)
		}
		if done == nil && partial == nil {
			insertOrders(tx, db, "buy", obj.Quantity, obj.Price, userid, ID)
			err = tx.Commit()
			if err != nil {
				logg.Error(err)
			}
			return
		}
		processOrders(tx, ID, db, done, partial, userid, partialQuantityProcessed, "btc", "usd")
		restOrder := orderBook.Order(ID)
		if restOrder != nil {
			quant, _ := (restOrder.Quantity()).Float64()
			price, _ := (restOrder.Price()).Float64()
			insertOrders(tx, db, (restOrder.Side()).String(), quant, price, userid, ID)
		}
		err = tx.Commit()
		if err != nil {
			logg.Error(err)
		}

	}
}

func insertOrders(tx *sql.Tx, db *sql.DB, side string, quantity float64, price float64, userid string, orderid string) {
	insertquery := "INSERT INTO orders(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)"
	_, err := tx.Exec(insertquery, orderid, userid, side, quantity, price, string(time.Now().Unix()))
	if err != nil {
		tx.Rollback()
		logg.Error(err)
		return
	}
	if side == "sell" {
		updateAvailableBalance(tx, userid, quantity, "walletbtc")
	}
	if side == "buy" {
		updateAvailableBalance(tx, userid, quantity*price, "walletusd")
	}
}

func updateAvailableBalance(tx *sql.Tx, userid string, quantity float64, wallet string) {
	updateAvailableBalancequery := fmt.Sprintf("Update %s SET AvailableBalance = AvailableBalance - ?  WHERE userid = ?", wallet)
	_, err := tx.Exec(updateAvailableBalancequery, quantity, userid)
	if err != nil {
		tx.Rollback()
		logg.Error(err)
		return
	}

}

// User1 is the maker
func processWalletTransaction(tx *sql.Tx, side string, db *sql.DB, user1 string, user2 string, price float64, quantity float64, wallet1 string, wallet2 string) {
	logg.Info(quantity)
	logg.Info(price)
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
		_, err = tx.Exec(query2, quantity*price, quantity*price, user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query3, quantity, quantity, user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query4, quantity*price, quantity*price, user2)
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
		_, err = tx.Exec(query2, quantity*price, quantity*price, user2)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query3, quantity, quantity, user2)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		_, err = tx.Exec(query4, quantity*price, quantity*price, user1)
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
	}
}

func processOrders(tx *sql.Tx, orderid string, db *sql.DB, done []*ob.Order, partial *ob.Order, userid string, partialQuantityProcessed decimal.Decimal, symbol1 string, symbol2 string) {
	var ID string
	for _, value := range done {
		if orderid == value.ID() {
			return
		}
		logg.Info(value)
		user_id, err := db.Prepare("SELECT userid from orders WHERE orderid = ?")
		if err != nil {
			logg.Error(err)
		}
		defer user_id.Close()
		row, err := user_id.Query(value.ID())
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
		quan, _ := (value.Quantity()).Float64()
		price, _ := (value.Price()).Float64()
		processWalletTransaction(tx, (value.Side()).String(), db, ID, userid, price, quan, "wallet"+symbol1, "wallet"+symbol2)
		deleteOrdersquery := "DELETE FROM orders WHERE orderid=?"
		_, err = tx.Exec(deleteOrdersquery, value.ID())
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		insertOrderHistoryquery := "INSERT INTO orderHistory(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)"
		quant, _ := (value.Quantity()).Float64()
		_, err = tx.Exec(insertOrderHistoryquery, value.ID(), ID, (value.Side()).String(), quant, (value.Price()).String(), string(time.Now().Unix()))
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
		if orderid == partial.ID() {
			return
		}
		user_id, err := db.Prepare("SELECT userid from orders WHERE orderid = ?")
		if err != nil {
			logg.Error(err)
		}
		defer user_id.Close()
		row, err := user_id.Query(partial.ID())
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
		quan, _ := (partial.Quantity()).Float64()
		price, _ := (partial.Price()).Float64()
		processWalletTransaction(tx, (partial.Side()).String(), db, ID, userid, price, quan, "wallet"+symbol1, "wallet"+symbol2)
		updateOrdersquery := "UPDATE orders SET quantity = ? WHERE orderid = ?"
		_, err = tx.Exec(updateOrdersquery, (partial.Quantity()).String(), partial.ID())
		if err != nil {
			tx.Rollback()
			logg.Error(err)
			return
		}
		insertOrderHistoryquery := "INSERT INTO orderHistory(orderid, userid, side, quantity, price, timestamp) VALUES(?,?,?,?,?,?)"
		quant, _ := (partialQuantityProcessed).Float64()
		_, err = tx.Exec(insertOrderHistoryquery, partial.ID(), ID, (partial.Side()).String(), quant, (partial.Price()).String(), string(time.Now().Unix()))
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

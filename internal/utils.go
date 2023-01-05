package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joshelb/joshchange/internal/orderbook"
	logg "github.com/sirupsen/logrus"
)

// Websocket Response from client
type Response struct {
	Stream string
	Data   interface{}
}

type UserData struct {
	ActiveOrders   [][]string
	OrderHistory   [][]string
	TradeHistory   [][]string
	WalletBalances map[string][]float64
}

func (c *Connection) pairDataHandler(mt int, msg WSStream, ch <-chan bool, e Embed) {
	db := e.Collection.MySQLClient
	var ()
	m := make(map[string][]float64)
	for {
		select {
		case <-ch:
			return
		default:
			getAllTradesquery := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_NAME LIKE 'tradeHistory%'"
			names, err := db.Query(getAllTradesquery)
			if err != nil {
				logg.Error(err)
			}
			var name string
			for names.Next() {
				err = names.Scan(&name)
				if err != nil {
					logg.Error(err)
				}
				query := fmt.Sprintf("SELECT * FROM %s WHERE FROM_UNIXTIME(timestamp) >= NOW() - INTERVAL 1 DAY", name)
				query2 := fmt.Sprintf("SELECT * FROM %s WHERE FROM_UNIXTIME(timestamp) >= NOW() - INTERVAL 7 DAY", name)
				resp, err := db.Query(query)
				if err != nil {
					logg.Error(err)
				}
				resp2, err := db.Query(query2)
				if err != nil {
					logg.Error(err)
				}
				var quantity float64
				var placeholder string
				sum := float64(0)
				sum2 := float64(0)
				for resp.Next() {
					err = resp.Scan(&placeholder, &placeholder, &placeholder, &quantity, &placeholder, &placeholder)
					if err != nil {
						logg.Error(err)
					}
					sum += quantity
				}
				for resp2.Next() {
					err = resp2.Scan(&placeholder, &placeholder, &placeholder, &quantity, &placeholder, &placeholder)
					if err != nil {
						logg.Error(err)
					}
					sum2 += quantity
				}
				resp.Close()
				resp2.Close()
				m[name] = []float64{sum, sum2}
			}
			names.Close()
			data := &Response{Stream: "pairData", Data: m}
			res, err := json.Marshal(data)
			if err != nil {
				logg.Error(err)
			}
			err = c.Send(mt, res)
			if err != nil {
				logg.Info("broke")
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

}

// Handling of Trade Data
func (c *Connection) tradesHandler(mt int, msg WSStream, ch <-chan bool, e Embed) {
	db := e.Collection.MySQLClient
	var (
		uniqueid  string
		userid    string
		side      string
		quantity  string
		price     string
		timestamp string
	)
	for {
		select {
		case <-ch:
			return
		default:
			symbols := strings.Split(msg.Symbol, ":")
			query := fmt.Sprintf("SELECT * FROM tradeHistory%s", symbols[0]+symbols[1])
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			resp, err := stmt.Query()
			if err != nil {
				logg.Error(err)
			}
			result := [][]string{}
			for resp.Next() {
				err = resp.Scan(&uniqueid, &userid, &side, &quantity, &price, &timestamp)
				if err != nil {
					logg.Info(err)
				}
				result = append(result, []string{uniqueid, side, quantity, price, timestamp})
			}
			stmt.Close()
			resp.Close()
			data := &Response{Stream: "trades", Data: result}
			res, err := json.Marshal(data)
			if err != nil {
				logg.Error(err)
			}
			err = c.Send(mt, res)
			if err != nil {
				logg.Info("broke")
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Handling of CandlestickData
func candlesticksHandler(conn *websocket.Conn, mt int, msg WSStream, ch <-chan bool, e Embed) {
	for {
		select {
		case <-ch:
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Handling of OrderboookData
func (c *Connection) orderbookHandler(mt int, msg WSStream, ch <-chan bool, e Embed) {
	for {
		select {
		case <-ch:
			return
		default:
			orderBook, err := e.Collection.GetOrderbook_bySymbol(msg.Symbol)
			if err != nil {
				logg.Error(err)
			}
			data := &Response{Stream: "orderbook", Data: orderBook}
			res, err := json.Marshal(data)
			if err != nil {
				logg.Error(err)
			}
			err = c.Send(mt, res)
			if err != nil {
				logg.Info("broke")
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *Connection) userDataHandler(mt int, msg WSStream, ch <-chan bool, e Embed) {
	db := e.Collection.MySQLClient
	var result [][]string
	var result2 [][]string
	var result3 [][]string
	var (
		uniqueid  string
		orderid   string
		userid    string
		side      string
		quantity  string
		price     string
		timestamp string
	)
	symbol := strings.Split(msg.Symbol, ":")
	for {
		select {
		case <-ch:
			return
		default:
			query := fmt.Sprintf("SELECT * FROM orders WHERE userid='%s'", msg.Email)
			query2 := fmt.Sprintf("SELECT * FROM orderHistory WHERE userid='%s'", msg.Email)
			query3 := fmt.Sprintf("SELECT * FROM tradeHistory%s WHERE userid='%s'", symbol[0]+symbol[1], msg.Email)
			getAllWalletsquery := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_NAME LIKE 'wallet%';"
			names, err := db.Query(getAllWalletsquery)
			if err != nil {
				logg.Error(err)
			}
			var name string
			var AvailableBalance float64
			var Balance float64
			m := make(map[string][]float64)
			for names.Next() {
				err = names.Scan(&name)
				if err != nil {
					logg.Error(err)
				}
				query := fmt.Sprintf("SELECT AvailableBalance,Balance FROM %s WHERE userid = ?", name)
				rows, err := db.Query(query, msg.Email)
				if err != nil {
					logg.Error(err)
				}
				for rows.Next() {
					err := rows.Scan(&AvailableBalance, &Balance)
					if err != nil {
						logg.Error(err)
					}
					m[(name[6:])] = []float64{AvailableBalance, Balance}
				}
				rows.Close()
			}
			stmt, err := db.Prepare(query)
			if err != nil {
				logg.Error(err)
			}
			stmt2, err := db.Prepare(query2)
			if err != nil {
				logg.Error(err)
			}
			stmt3, err := db.Prepare(query3)
			if err != nil {
				logg.Error(err)
			}
			rows, err := stmt.Query()
			if err != nil {
				logg.Error(err)
			}
			rows2, err := stmt2.Query()
			if err != nil {
				logg.Error(err)
			}
			rows3, err := stmt3.Query()
			if err != nil {
				logg.Error(err)
			}
			result = [][]string{}
			for rows.Next() {
				err = rows.Scan(&orderid, &userid, &side, &quantity, &price, &timestamp)
				if err != nil {
					logg.Info(err)
				}
				result = append(result, []string{orderid, userid, side, quantity, price, timestamp})

			}
			rows.Close()
			result2 = [][]string{}
			for rows2.Next() {
				err = rows2.Scan(&orderid, &userid, &side, &quantity, &price, &timestamp)
				if err != nil {
					logg.Info(err)
				}
				result2 = append(result2, []string{orderid, userid, side, quantity, price, timestamp})

			}
			rows2.Close()
			result3 = [][]string{}
			for rows3.Next() {
				err = rows3.Scan(&uniqueid, &userid, &side, &quantity, &price, &timestamp)
				if err != nil {
					logg.Info(err)
				}
				result3 = append(result3, []string{uniqueid, userid, side, quantity, price, timestamp})

			}
			rows3.Close()
			userData := &UserData{ActiveOrders: result, OrderHistory: result2, TradeHistory: result3, WalletBalances: m}
			data := &Response{Stream: "userData", Data: userData}
			res, err := json.Marshal(data)
			if err != nil {
				logg.Error(err)
			}
			err = c.Send(mt, res)
			if err != nil {
				logg.Info("broke")
				return
			}
			stmt.Close()
			stmt2.Close()
			stmt3.Close()
			time.Sleep(1000 * time.Millisecond)
		}
	}

}

// Sends message to client
func (c *Connection) Send(mt int, message []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Socket.WriteMessage(mt, message)
}

func isOrderPossible(obj orderbook.Order, db *sql.DB) error {
	symbols := strings.Split((obj.Symbol), ":")
	symbol1 := symbols[0]
	symbol2 := symbols[1]
	quantity := obj.Quantity
	price := obj.Price
	side := obj.Side
	tx, err := db.Begin()
	if err != nil {
		logg.Error(err)
	}
	if side == "sell" {
		query := fmt.Sprintf("UPDATE %s  SET AvailableBalance = CASE     WHEN AvailableBalance < ? THEN AvailableBalance     ELSE AvailableBalance - ?     END", ("wallet" + symbol1))
		results, err := tx.Exec(query, quantity, quantity)
		if err != nil {
			logg.Error(err)
		}
		rowsAffected, err := results.RowsAffected()
		if err != nil {
			logg.Error(err)
		}
		if rowsAffected < 1 {
			tx.Commit()
			return errors.New("order not possible")
		}
	}
	if side == "buy" {
		query := fmt.Sprintf("UPDATE %s  SET AvailableBalance = CASE     WHEN AvailableBalance < ? THEN AvailableBalance     ELSE AvailableBalance - ?     END", ("wallet" + symbol2))
		results, err := tx.Exec(query, quantity*price, quantity*price)
		if err != nil {
			logg.Error(err)
		}
		logg.Info(results)
		rowsAffected, err := results.RowsAffected()
		if err != nil {
			logg.Error(err)
		}
		if rowsAffected < 1 {
			tx.Commit()
			return errors.New("order not possible")
		}
	}
	tx.Commit()
	return nil

}

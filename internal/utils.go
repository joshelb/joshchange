package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	logg "github.com/sirupsen/logrus"
)

// Websocket Response from client
type Response struct {
	Stream string
	Data   interface{}
}

type UserData struct {
	ActiveOrders [][]string
	OrderHistory [][]string
	TradeHistory [][]string
}

// Handling of Trade Data
func (c *Connection) tradesHandler(mt int, msg WSStream, ch <-chan bool, e Embed) {
	for {
		select {
		case <-ch:
			return
		default:
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
	for {
		select {
		case <-ch:
			return
		default:
			query := fmt.Sprintf("SELECT * FROM orders WHERE userid='%s'", msg.Email)
			query2 := fmt.Sprintf("SELECT * FROM orderHistory WHERE userid='%s'", msg.Email)
			query3 := fmt.Sprintf("SELECT * FROM tradeHistory WHERE userid='%s'", msg.Email)

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
			userData := &UserData{ActiveOrders: result, OrderHistory: result2, TradeHistory: result3}
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

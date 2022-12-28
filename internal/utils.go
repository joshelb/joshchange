package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/roistat/go-clickhouse"
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
func (c *Connection) tradesHandler(clickhouseConn *clickhouse.Conn, mt int, msg WSStream, ch <-chan bool, e Embed) {
	for {
		select {
		case <-ch:
			return
		default:
			s := fmt.Sprintf("SELECT TOP (50) * FROM tickdata.%s ORDER BY timestamp DESC", msg.Symbol)
			q := clickhouse.NewQuery(s)
			iter := q.Iter(clickhouseConn)
			var (
				timestamp string
				quantity  string
				price     string
				side      string
			)
			var table [][]string
			for iter.Scan(&timestamp, &quantity, &price, &side) {
				row := []string{timestamp, quantity, price, side}
				table = append(table, row)
			}
			if iter.Error() != nil {
				logg.Error(iter.Error())
			}
			data := &Response{Stream: "trades", Data: table}
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
func candlesticksHandler(clickhouseConn *clickhouse.Conn, conn *websocket.Conn, mt int, msg WSStream, ch <-chan bool, e Embed) {
	for {
		select {
		case <-ch:
			return
		default:
			s := fmt.Sprintf("SELECT * FROM candlesticks.%s%s FINAL", msg.Symbol, msg.Timeframe)
			q := clickhouse.NewQuery(s)
			iter := q.Iter(clickhouseConn)
			var (
				timestamp string
				open      string
				high      string
				low       string
				close     string
				volume    string
			)
			var table [][]string
			for iter.Scan(&timestamp, &open, &high, &low, &close, &volume) {
				row := []string{timestamp, open, high, low, close, volume}
				table = append(table, row)
			}
			if iter.Error() != nil {
				logg.Error(iter.Error())
			}
			res, err := json.Marshal(table)
			if err != nil {
				logg.Error(err)
			}
			err = conn.WriteMessage(mt, res)
			if err != nil {
				logg.Info("broke")
				return
			}
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
				err = rows3.Scan(&userid, &side, &quantity, &price, &timestamp)
				if err != nil {
					logg.Info(err)
				}
				result3 = append(result3, []string{userid, side, quantity, price, timestamp})

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

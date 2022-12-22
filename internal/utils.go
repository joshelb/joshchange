package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/roistat/go-clickhouse"
	logg "github.com/sirupsen/logrus"
)

type Response struct {
	Stream string
	Data   interface{}
}

func createDbIfNotExists(email string){


}




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

func (c *Connection) Send(mt int, message []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Socket.WriteMessage(mt, message)
}

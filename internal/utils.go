package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joshelb/joshchange/internal/orderbook"
	"github.com/shopspring/decimal"
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

type WalletData struct {
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
				query := fmt.Sprintf("SELECT * FROM %s WHERE FROM_UNIXTIME(timestamp) >= NOW() - INTERVAL 1 DAY ", name)
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
				var price float64
				var timestamp float64
				sum := float64(0)
				sum2 := float64(0)
				first := []float64{1000000000000, 0}
				last := []float64{0, 0}
				for resp.Next() {
					err = resp.Scan(&placeholder, &placeholder, &placeholder, &quantity, &price, &timestamp)
					if err != nil {
						logg.Error(err)
					}
					if timestamp < first[0] {
						first = []float64{timestamp, price}
					}
					if timestamp > last[0] {
						last = []float64{timestamp, price}
					}
					sum += quantity
				}
				onedaychange := last[1] - first[1]
				onedaychangeinpercent := last[1]/first[1] - 1
				for resp2.Next() {
					err = resp2.Scan(&placeholder, &placeholder, &placeholder, &quantity, &placeholder, &placeholder)
					if err != nil {
						logg.Error(err)
					}
					sum2 += quantity
				}
				resp.Close()
				resp2.Close()
				m[name] = []float64{sum, sum2, onedaychange, onedaychangeinpercent, first[1]}
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
			query := fmt.Sprintf("SELECT * FROM tradeHistory%s ORDER BY timestamp DESC LIMIT 100", symbols[0]+"_"+symbols[1])
			resp, err := db.Query(query)
			if err != nil {
				logg.Error(err)
				logg.Info(resp)
				return
			}
			result := [][]string{}
			for resp.Next() {
				err = resp.Scan(&uniqueid, &userid, &side, &quantity, &price, &timestamp)
				if err != nil {
					logg.Info(err)
				}
				result = append(result, []string{uniqueid, side, quantity, price, timestamp})
			}
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

func (c *Connection) initcandlesticksHandler(conn *websocket.Conn, mt int, msg WSStream, e Embed) {
	db := e.Collection.MySQLClient
	var (
		timestamp string
		open      string
		high      string
		low       string
		close     string
		quantity  string
	)
	timeframes := []string{"1Min", "5Min", "15Min", "30Min", "1H", "4H", "12H", "1D"}
	var result [][][]string
	for _, i := range timeframes {
		symbols := strings.Split(msg.Symbol, ":")
		query := "SELECT * FROM candlestickData" + symbols[0] + symbols[1] + i
		rows, err := db.Query(query)
		if err != nil {
			logg.Error(err)
			return
		}
		var res [][]string
		for rows.Next() {
			err := rows.Scan(&timestamp, &open, &high, &low, &close, &quantity)
			if err != nil {
				logg.Error(err)
			}
			row := []string{timestamp, open, high, low, close, quantity}
			res = append(res, row)

		}
		var resres [][]string
		if len(res) > 0 {
			filler, _ := strconv.Atoi(res[0][0])
			logg.Info(len(res))
			logg.Info("000000")
			//end, _ := strconv.Atoi(res[len(res)-1][0])
			end := int(time.Now().Unix())
			i := 0
			close := res[0][4]
			for filler < end {
				if i == len(res) {
					row := []string{fmt.Sprintf("%d", filler), close, close, close, close, "0"}
					resres = append(resres, row)
					filler = filler + 60
					continue
				}
				plh, _ := strconv.Atoi(res[i][0])
				if plh != filler {
					row := []string{fmt.Sprintf("%d", filler), close, close, close, close, "0"}
					//row := resres[len(resres)-1]
					resres = append(resres, row)
				}
				if filler == plh {
					resres = append(resres, res[i])
					close = res[i][4]
					i += 1
				}
				filler = filler + 60

			}

		}
		result = append(result, resres)
	}
	data := &Response{Stream: "candlesticksInit", Data: result}
	res, err := json.Marshal(data)
	if err != nil {
		logg.Error(err)
	}
	err = c.Send(mt, res)
	if err != nil {
		logg.Info("broke")
		return
	}

}

// Handling of CandlestickData
func (c *Connection) candlesticksHandler(conn *websocket.Conn, mt int, msg WSStream, ch <-chan bool, e Embed) {
	db := e.Collection.MySQLClient
	var (
		timestamp int
		open      string
		high      string
		low       string
		close     string
		quantity  string
	)
	timeframes := []string{"1Min", "5Min", "15Min", "30Min", "1H", "4H", "12H", "1D"}
	for {
		select {
		case <-ch:
			return
		default:
			var result [][][]string
			for _, i := range timeframes {
				symbols := strings.Split(msg.Symbol, ":")
				query := "SELECT * FROM candlestickData" + symbols[0] + symbols[1] + i + " ORDER BY timestamp DESC LIMIT 2"
				rows, err := db.Query(query)
				if err != nil {
					logg.Error(err)
					return
				}
				if rows == nil {
					break
				}
				var res [][]string
				for rows.Next() {
					err := rows.Scan(&timestamp, &open, &high, &low, &close, &quantity)
					if err != nil {
						logg.Error(err)
					}
					time_now := time.Now().Unix()
					ts_curr_min := time_now - (time_now % 60)
					if ts_curr_min > int64(timestamp) {
						row := []string{fmt.Sprintf("%d", ts_curr_min), close, close, close, close, "0"}
						res = append(res, row)
					} else {
						row := []string{fmt.Sprintf("%d", ts_curr_min), open, high, low, close, quantity}
						res = append(res, row)
					}
				}
				result = append(result, res)
			}
			data := &Response{Stream: "candlesticks", Data: result}
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
				return
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

func (c *Connection) walletHandler(mt int, msg WSStream, ch <-chan bool, e Embed) {
	db := e.Collection.MySQLClient
	for {
		select {
		case <-ch:
			return
		default:
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
			walletData := &WalletData{WalletBalances: m}
			data := &Response{Stream: "walletData", Data: walletData}
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
	if len(symbol) < 2 {
		symbol[0] = ""
		symbol = append(symbol, "")
	}
	for {
		select {
		case <-ch:
			return
		default:
			query := fmt.Sprintf("SELECT * FROM orders WHERE userid='%s'", msg.Email)
			query2 := fmt.Sprintf("SELECT * FROM orderHistory WHERE userid='%s'", msg.Email)
			query3 := fmt.Sprintf("SELECT * FROM tradeHistory%s WHERE userid='%s'", symbol[0]+"_"+symbol[1], msg.Email)
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
			rows, err := db.Query(query)
			if err != nil {
				logg.Error(err)
				return
			}
			rows2, err := db.Query(query2)
			if err != nil {
				logg.Error(err)
				return
			}
			rows3, err := db.Query(query3)
			if err != nil {
				logg.Error(err)
				return
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

func isTradingActive(conn *sql.DB, pair string) bool {
	var boolvar int
	var placeholder string
	err := conn.QueryRow("SELECT * FROM tradingActive WHERE pair = ?", pair).Scan(&placeholder, &boolvar)
	if err != nil {
		logg.Error(err)
	}
	if boolvar == 1 {
		return true
	}
	return false
}

func validate_input(obj orderbook.Order) error {
	if obj.Symbol == "KISM:JOSH" {
		logg.Info(obj.Quantity)
		s := decimal.NewFromFloat(obj.Quantity).String()
		x := strings.Split(s, ".")
		logg.Info(x)
		var m int
		if len(x) == 1 {
			m = 1
		} else {
			m = len(x[1])
		}
		if obj.Quantity >= 1 && m < 6 {
			s := decimal.NewFromFloat(obj.Price).String()
			x := strings.Split(s, ".")
			var m int
			if len(x) == 1 {
				m = 1
			} else {
				m = len(x[1])
			}
			if obj.Price >= 0.0000001 && m < 6 {
				return nil
			}
		}
	}
	return errors.New("invalid Order Input for this pair")
}

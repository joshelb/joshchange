package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/joshelb/joshchange/internal/orderbook"
	"github.com/roistat/go-clickhouse"
	logg "github.com/sirupsen/logrus"
)

var upgrader = websocket.Upgrader{

	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Connection struct {
	Socket *websocket.Conn
	mu     sync.Mutex
}

type WSStream struct {
	Type        string
	Stream      string
	Symbol      string
	Timeframe   string
	Aggregation string
}

type Embed struct {
	Collection *orderbook.Orderbookcollection
}

func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Headers", "Access-Control-Allow-Headers, X-Requested-With, Content-Type, Accept, Origin, Authorization,Content-Type, Content-Length, X-Auth-Token, Access-Control-Request-Method, Access-Control-Request-Headers")
	(*w).Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS")
}

func (e Embed) OrderHandler(writer http.ResponseWriter, r *http.Request) {
	enableCors(&writer)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	var order orderbook.Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		logg.Error(err)
	}
	fmt.Printf("%+v\n", order.Ordertype)
	if order.Ordertype == "market" {
		e.Collection.Marketorder(order)
	}
	if order.Ordertype == "limit" {
		e.Collection.Limitorder(order)
	}
	logg.Info(e.Collection.Map.Load("btcusd"))
}

func (e Embed) OrderbookHandler(clickconn *clickhouse.Conn) http.HandlerFunc {
	return func(writer http.ResponseWriter, r *http.Request) {
		enableCors(&writer)
		conn, err := upgrader.Upgrade(writer, r, nil)
		if err != nil {
			logg.Error(err)
		}
		defer conn.Close()
		connection := new(Connection)
		connection.Socket = conn
		quitOrderbook := make(chan bool)
		quitCandlesticks := make(chan bool)
		quitTrades := make(chan bool)
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				logg.Error(err)
				break
			}
			var dat WSStream
			if err = json.Unmarshal(msg, &dat); err != nil {
				logg.Error(err)
			}
			if dat.Type == "subscribe" {
				if dat.Stream == "orderbook" {
					go connection.orderbookHandler(mt, dat, quitOrderbook, e)
				}
				if dat.Stream == "candlesticks" {
					go candlesticksHandler(clickconn, conn, mt, dat, quitCandlesticks, e)
				}
				if dat.Stream == "trades" {
					go connection.tradesHandler(clickconn, mt, dat, quitTrades, e)
				}
			}
			if dat.Type == "unsubscribe" {
				if dat.Stream == "orderbook" {
					quitOrderbook <- true
					logg.Info("Client unsubscribed from orderbook")
				}
			}
		}
	}
}

func TradeHandler(writer http.ResponseWriter, r *http.Request) {
	tmp := template.Must(template.ParseFiles("templates/layout.html"))
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	data := symbol
	tmp.Execute(writer, data)

}

func CandlesticksHandler(conn *clickhouse.Conn) http.HandlerFunc {
	return func(writer http.ResponseWriter, r *http.Request) {
		enableCors(&writer)
		vars := mux.Vars(r)
		symbol := vars["symbol"]
		timeframe := vars["timeframe"]
		s := fmt.Sprintf("SELECT * FROM candlesticks.%s%s FINAL", symbol, timeframe)
		q := clickhouse.NewQuery(s)
		iter := q.Iter(conn)
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
		writer.Write(res)
	}
}

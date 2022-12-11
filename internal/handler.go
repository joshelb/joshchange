package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

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

func (e Embed) OrderbookHandler(writer http.ResponseWriter, r *http.Request) {
	enableCors(&writer)
	conn, err := upgrader.Upgrade(writer, r, nil)
	if err != nil {
		logg.Error(err)
	}
	defer conn.Close()
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	for {
		mt, _, err := conn.ReadMessage()
		if err != nil {
			logg.Error(err)
			break
		}
		go func() {
			for {
				orderBook, err := e.Collection.GetOrderbook_bySymbol(symbol)
				if err != nil {
					logg.Error(err)
				}
				data, err := orderBook.MarshalJSON()
				if err != nil {
					logg.Error(err)
				}
				err = conn.WriteMessage(mt, data)
				if err != nil {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()
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

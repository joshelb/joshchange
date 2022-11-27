package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/joshelb/joshchange/internal/orderbook"
	logg "github.com/sirupsen/logrus"
)

type Embed struct {
	Collection *orderbook.Orderbookcollection
}

func (e Embed) OrderHandler(writer http.ResponseWriter, r *http.Request) {
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
	logg.Info(e.Collection.Load("btcusd"))
}

func (e Embed) OrderbookHandler(writer http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	orderBook, err := e.Collection.GetOrderbook_bySymbol(symbol)
	if err != nil {
		logg.Error(err)
	}
	data, err := orderBook.MarshalJSON()
	if err != nil {
		logg.Error(err)
	}
	writer.Write(data)
}

func TradeHandler(writer http.ResponseWriter, r *http.Request) {
	tmp := template.Must(template.ParseFiles("templates/layout.html"))
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	data := symbol
	tmp.Execute(writer, data)

}

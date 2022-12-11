package server

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"
	oj "github.com/joshelb/joshchange/internal/orderbook"
	"github.com/roistat/go-clickhouse"
	logg "github.com/sirupsen/logrus"
)

var ctx = context.Background()

func New() {
	conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHttpTransport())
	collection := &oj.Orderbookcollection{ClickhouseClient: conn}
	collection.InitOrderbook("btcusd")
	logg.Info(collection)
	embed := &Embed{
		Collection: collection,
	}
	logg.Info(collection.Map.Load("btcusd"))
	router := mux.NewRouter()
	fs := http.FileServer(http.Dir("./assets/"))
	router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", fs))
	router.HandleFunc("/order", embed.OrderHandler)
	router.HandleFunc("/orderbook/{symbol}", embed.OrderbookHandler).Methods("GET")
	router.HandleFunc("/trade/{symbol}", TradeHandler)
	router.HandleFunc("/candlesticks/{symbol}/{timeframe}", CandlesticksHandler(conn)).Methods("GET")

	err := http.ListenAndServe(":8080", router)
	if err != nil {
		logg.Error("There is an error with the Server.")
	}

}

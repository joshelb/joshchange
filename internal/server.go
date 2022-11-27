package server

import (
	"net/http"

	"github.com/gorilla/mux"
	oj "github.com/joshelb/joshchange/internal/orderbook"
	logg "github.com/sirupsen/logrus"
)

func New() {
	collection := &oj.Orderbookcollection{}
	collection.InitOrderbook("btcusd")
	logg.Info(collection)
	embed := &Embed{
		Collection: collection,
	}
	logg.Info(collection.Load("btcusd"))
	router := mux.NewRouter()
	fs := http.FileServer(http.Dir("./assets/"))
	router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", fs))
	router.HandleFunc("/order", embed.OrderHandler).Methods("POST")
	router.HandleFunc("/orderbook/{symbol}", embed.OrderbookHandler).Methods("GET")
	router.HandleFunc("/trade/{symbol}", TradeHandler)

	err := http.ListenAndServe(":8080", router)
	if err != nil {
		logg.Error("There is an error with the Server.")
	}

}

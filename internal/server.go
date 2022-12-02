package server

import (
	"net/http"
	"context"

	"github.com/gorilla/mux"
	"github.com/go-redis/redis/v8"
	oj "github.com/joshelb/joshchange/internal/orderbook"
	logg "github.com/sirupsen/logrus"
)

var ctx = context.Background()

func New() {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
    Password: "",
    DB:       0,
  })
	err := rdb.Set(ctx,"key","NUll",0).Err()
	if err != nil {
		logg.Error(err)
	}
	collection := &oj.Orderbookcollection{RedisClient: rdb,}
	collection.InitOrderbook("btcusd")
	logg.Info(collection)
	embed := &Embed{
		Collection: collection,
	}
	logg.Info(collection.Map.Load("btcusd"))
	router := mux.NewRouter()
	fs := http.FileServer(http.Dir("./assets/"))
	router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", fs))
	router.HandleFunc("/order", embed.OrderHandler).Methods("POST")
	router.HandleFunc("/orderbook/{symbol}", embed.OrderbookHandler).Methods("GET")
	router.HandleFunc("/trade/{symbol}", TradeHandler)

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		logg.Error("There is an error with the Server.")
	}

}

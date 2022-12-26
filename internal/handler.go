package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"database/sql"
	"sync"

	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/joshelb/joshchange/internal/orderbook"
	"github.com/roistat/go-clickhouse"
	logg "github.com/sirupsen/logrus"
)

// Upgrade http for websocket support
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

type CustomClaimsExample struct {
	UserID        string `json:"userid"`
	ShouldReject bool   `json:"shouldReject,omitempty"`
}

type User struct{
	UserID string `json:"userid"`
}


// Registers user to backend_db after signup
func RegisterHandler(conn *sql.DB) http.HandlerFunc {
	return func(writer http.ResponseWriter, r *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		var usr User
		err := json.NewDecoder(r.Body).Decode(&usr)
		if err != nil {
		  logg.Error(err)
		}
		user_id := usr.UserID
		query := fmt.Sprintf("INSERT INTO users (user_id) VALUES (%s)", user_id)
		insert, err := conn.Query(query)
		if err != nil {
			logg.Error(err)
		}
		defer insert.Close()
		logg.Info("###############################################")
	}
}

// Handles incoming Orders
func (e Embed) OrderHandler(writer http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value(jwtmiddleware.ContextKey{}).(*validator.ValidatedClaims)
	customClaims := claims.CustomClaims.(*CustomClaimsExample)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	var order orderbook.Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		logg.Error(err)
	}
	fmt.Printf("%+v\n", order.Ordertype)
	if order.Ordertype == "market" {
		e.Collection.Marketorder(order, customClaims.UserID)
	}
	if order.Ordertype == "limit" {
		e.Collection.Limitorder(order, customClaims.UserID)
	}
	logg.Info(e.Collection.Map.Load("btcusd"))
}

// WS Handler for Datastream to Frontend
func (e Embed) WSHandler(clickconn *clickhouse.Conn) http.HandlerFunc {
	return func(writer http.ResponseWriter, r *http.Request) {
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
			logg.Info(msg)
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



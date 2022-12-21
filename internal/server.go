package server

import (
	"context"
	"net/http"
	"errors"
	"net/url"
	"time"

	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/gorilla/mux"
	oj "github.com/joshelb/joshchange/internal/orderbook"
	"github.com/roistat/go-clickhouse"
	"github.com/rs/cors"
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
	/*c := cors.New(cors.Options{
	    AllowedOrigins: []string{"http://localhost:5173"},
			AllowedHeaders: []string{"Content-Type, X-Auth-Token"},
	    AllowCredentials: true,
	    Debug: true,
		})*/
	router := mux.NewRouter()
	fs := http.FileServer(http.Dir("./assets/"))
	router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", fs))
	var handler = http.HandlerFunc(embed.OrderHandler)
	middleware := setupAuth()
	Handler := cors.AllowAll().Handler(middleware.CheckJWT(handler))
	router.Handle("/order", Handler)
	router.HandleFunc("/orderbook/{symbol}", embed.OrderbookHandler(conn)).Methods("GET")
	router.HandleFunc("/trade/{symbol}", TradeHandler)
	router.HandleFunc("/candlesticks/{symbol}/{timeframe}", CandlesticksHandler(conn)).Methods("GET")

	err := http.ListenAndServe(":8080", router)
	if err != nil {
		logg.Error("There is an error with the Server.")
	}

}

func (c *CustomClaimsExample) Validate(ctx context.Context) error {
	if c.ShouldReject {
		return errors.New("should reject was set to true")
	}
	return nil
}

func setupAuth() *jwtmiddleware.JWTMiddleware {
	issuerURL, err := url.Parse("https://dev-q7xsxw5kc72jd045.eu.auth0.com/")
	provider := jwks.NewCachingProvider(issuerURL, 5*time.Minute)
	customClaims := func() validator.CustomClaims {
		return &CustomClaimsExample{}
	}

	jwtValidator, err := validator.New(
		provider.KeyFunc,
		validator.RS256,
		"https://dev-q7xsxw5kc72jd045.eu.auth0.com/",
		[]string{"http://localhost:8080/orderbook/btcusd"},
		validator.WithCustomClaims(customClaims),
	)
	if err != nil {
		logg.Error(err)
	}
	middleware := jwtmiddleware.New(jwtValidator.ValidateToken)
	return middleware
}

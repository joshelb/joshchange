package server

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	"github.com/gorilla/mux"
	oj "github.com/joshelb/joshchange/internal/orderbook"
	"github.com/roistat/go-clickhouse"
	"github.com/rs/cors"
	logg "github.com/sirupsen/logrus"
)

// Initialises HTTP Server
func New() {
	db, err := sql.Open("mysql", "joshelb:chirurgie@tcp(127.0.0.1:3306)/users")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	insert, err := db.Query("CREATE TABLE testuser ( UserID int, wallet_balance varchar(255) )")
	if err != nil {
		panic(err.Error())
	}
	defer insert.Close()


	conn := clickhouse.NewConn("localhost:8123", clickhouse.NewHttpTransport())
	collection := &oj.Orderbookcollection{ClickhouseClient: conn}
	collection.InitOrderbook("btcusd")
	// var of Embed struct to pass Orderbookcollection to Handler
	embed := &Embed{
		Collection: collection,
	}
	middleware := setupAuth()
	fs := http.FileServer(http.Dir("./assets/"))
	orderhandler := http.HandlerFunc(embed.OrderHandler)
	// Allow CORS and check Athorization Token with the JWT middleware
	orderhandler_update := cors.AllowAll().Handler(middleware.CheckJWT(orderhandler))
	registerhandler := http.HandlerFunc(RegisterHandler(conn))	
	registerhandler_update := cors.AllowAll().Handler(registerhandler)
	wshandler := http.HandlerFunc(embed.WSHandler(conn)) 
	wshandler_update := cors.AllowAll().Handler(wshandler)


	router := mux.NewRouter()
	router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", fs))
	router.Handle("/order", orderhandler_update)
	router.Handle("/wsdata", wshandler_update)
	router.Handle("/registerDBEntry", registerhandler_update)

	err = http.ListenAndServe(":8080", router)
	if err != nil {
		logg.Error("There is an error with the Server.")
	}

}

//Validation function for CustomClaims
func (c *CustomClaimsExample) Validate(ctx context.Context) error {
	if c.ShouldReject {
		return errors.New("should reject was set to true")
	}
	return nil
}

// JWT middleware to check Auth
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

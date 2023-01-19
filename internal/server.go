package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"database/sql"

	jwtmiddleware "github.com/auth0/go-jwt-middleware/v2"
	"github.com/auth0/go-jwt-middleware/v2/jwks"
	"github.com/auth0/go-jwt-middleware/v2/validator"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	oj "github.com/joshelb/joshchange/internal/orderbook"
	"github.com/rs/cors"
	logg "github.com/sirupsen/logrus"
)

// Initialises HTTP Server
func New() {
	db, err := sql.Open("mysql", "joshelb:chirurgie@tcp(127.0.0.1:3306)/userInfo")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	collection := &oj.Orderbookcollection{MySQLClient: db}
	collection.InitOrderbook("KISM:JOSH")
	go collection.BackupBook(db, "KISM:JOSH")
	// var of Embed struct to pass Orderbookcollection to Handler
	embed := &Embed{
		Collection: collection,
	}
	middleware := setupAuth()
	fs := http.FileServer(http.Dir("./assets/"))
	orderhandler := http.HandlerFunc(embed.OrderHandler)
	// Allow CORS and check Athorization Token with the JWT middleware
	orderhandler_update := cors.AllowAll().Handler(middleware.CheckJWT(orderhandler))
	cancelhandler := http.HandlerFunc(embed.CancelHandler)
	cancelhandler_update := cors.AllowAll().Handler(middleware.CheckJWT(cancelhandler))
	registerhandler := http.HandlerFunc(RegisterHandler(db))
	registerhandler_update := cors.AllowAll().Handler(registerhandler)
	wshandler := http.HandlerFunc(embed.WSHandler())
	wshandler_update := cors.AllowAll().Handler(wshandler)

	router := mux.NewRouter()
	router.PathPrefix("/assets/").Handler(http.StripPrefix("/assets/", fs))
	router.Handle("/order", orderhandler_update)
	router.Handle("/cancel", cancelhandler_update)
	router.Handle("/wsdata", wshandler_update)
	router.Handle("/registerDBEntry", registerhandler_update)
	router.HandleFunc("/silvester", SilvesterHandler)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		err = srv.ListenAndServe()
		if err != nil {
			logg.Error(err)
		}
	}()

	<-done
	logg.Info("stopped")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
	}()
	if err := srv.Shutdown(ctx); err != nil {
		logg.Error("Server Shutdown Failed:%+v", err)
	}
	log.Print("Server Exited Properly")

}

// Validation function for CustomClaims
func (c *CustomClaimsExample) Validate(ctx context.Context) error {
	if c.ShouldReject {
		return errors.New("should reject was set to true")
	}
	return nil
}

// JWT middleware to check Auth
func setupAuth() *jwtmiddleware.JWTMiddleware {
	issuerURL, err := url.Parse("https://dev-q7xsxw5kc72jd045.eu.auth0.com/")
	if err != nil {
		logg.Error(err)
	}
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

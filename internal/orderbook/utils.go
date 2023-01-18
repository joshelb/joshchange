package orderbook

import (
	"database/sql"
	"errors"
	"fmt"

	ob "github.com/joshelb/orderbook"
	logg "github.com/sirupsen/logrus"
)

// get Orderbook
func (o Orderbookcollection) GetOrderbook_bySymbol(symbol string) (*ob.OrderBook, error) {
	value, ok := o.Map.Load(symbol)
	if ok {
		return value.(*ob.OrderBook), nil
	} else {
		return nil, errors.New("cant find Symbol")
	}
}

func validateOrder(conn *sql.DB, quantity float64, symbol string) error {
	query := fmt.Sprintf("UPDATE %s  SET AvailableBalance = CASE     WHEN AvailableBalance < ? THEN AvailableBalance     ELSE AvailableBalance - ?     END", ("wallet" + symbol))
	results, err := conn.Exec(query, quantity, quantity)
	if err != nil {
		logg.Error(err)
	}
	rowsAffected, err := results.RowsAffected()
	if err != nil {
		logg.Error(err)
	}
	if rowsAffected < 1 {
		return errors.New("order not possible")
	}
	return nil
}

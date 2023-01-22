package orderbook

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	ob "github.com/joshelb/orderbook"
	"github.com/shopspring/decimal"
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

func validateOrder(conn *sql.DB, quantity decimal.Decimal, symbol string, userid string) error {
	tx, err := conn.Begin()
	if err != nil {
		logg.Error(err)
	}
	row, _ := tx.Exec(fmt.Sprintf("SELECT * FROM %s WHERE userid = ? FOR UPDATE", "wallet"+symbol), userid)
	if row != nil {
		logg.Error(row)
	}
	query := fmt.Sprintf("UPDATE %s SET AvailableBalance = CASE WHEN AvailableBalance < ? THEN AvailableBalance ELSE AvailableBalance - ? END WHERE userid = ?", ("wallet" + symbol))
	results, err := tx.Exec(query, quantity, quantity, userid)
	if err != nil {
		logg.Error(err)
	}
	logg.Info(err)
	rowsAffected, err := results.RowsAffected()
	if err != nil {
		logg.Error(err)
	}
	if rowsAffected < 1 {
		tx.Rollback()
		return errors.New("order not possible")
	}

	tx.Commit()
	return nil
}

func (o Orderbookcollection) BackupBook(conn *sql.DB, pair string) {
	for {
		value, ok := o.Map.Load(pair)
		if !ok {
			logg.Error("error")
		}
		symbols := strings.Split(pair, ":")
		data, err := (value.(*ob.OrderBook)).MarshalJSON()
		if err != nil {
			logg.Error(err)
		}
		timestamp := time.Now().Unix()
		query := fmt.Sprintf("INSERT INTO orderbook%s VALUES(?,?)", symbols[0]+"_"+symbols[1])
		_, err = conn.Exec(query, data, timestamp)
		if err != nil {
			logg.Error(err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

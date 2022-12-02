package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

type Order struct {
	Symbol    string
	Side      string
	Ordertype string
	Quantity  float64
	Price     float64
}

func main() {

	for i := 16000; i < 17000; i = i + 100 {
		values := &Order{
			Symbol:    "btcusd",
			Side:      "buy",
			Ordertype: "limit",
			Quantity:  30,
			Price:     float64(i)}

		json_data, err := json.Marshal(values)

		if err != nil {
			log.Fatal(err)
		}
		var order Order
		err = json.Unmarshal(json_data, &order)
		_, err = http.Post("http://localhost:8080/order", "application/json",
			bytes.NewBuffer(json_data))

		if err != nil {
			log.Fatal(err)
		}
	}
	for i := 18000; i >= 17000; i = i - 100 {
		values := &Order{
			Symbol:    "btcusd",
			Side:      "sell",
			Ordertype: "limit",
			Quantity:  25,
			Price:     float64(i)}

		json_data, err := json.Marshal(values)

		if err != nil {
			log.Fatal(err)
		}
		var order Order
		err = json.Unmarshal(json_data, &order)
		_, err = http.Post("http://localhost:8080/order", "application/json",
			bytes.NewBuffer(json_data))

		if err != nil {
			log.Fatal(err)
		}
	}
	    values := &Order{
      Symbol:    "btcusd",
      Side:      "buy",
      Ordertype: "market",
      Quantity:  800,
      Price:     float64(0)}

    json_data, err := json.Marshal(values)

    if err != nil {
      log.Fatal(err)
    }
    var order Order
    err = json.Unmarshal(json_data, &order)
    _, err = http.Post("http://localhost:8080/order", "application/json",
      bytes.NewBuffer(json_data))

    if err != nil {
      log.Fatal(err)
    }

}

package orderbook

import (
	"fmt"

	ob "github.com/muzykantov/orderbook"
	"github.com/shopspring/decimal"
)

func main() {
	orderBook := ob.NewOrderBook()
	fmt.Println(orderBook)
	done, partial, partialQuantityProcessed, err := orderBook.ProcessLimitOrder(ob.Sell, "uinqueID", decimal.New(55, 0), decimal.New(100, 0))
	if err != nil {
		fmt.Println("Error")
	}
	fmt.Println(done)
	fmt.Println(partial)
	fmt.Println(partialQuantityProcessed)
	fmt.Println(orderBook)
	_, _, _, _, _ = orderBook.ProcessMarketOrder(ob.Buy, decimal.New(6, 0))
	fmt.Println(orderBook)
}

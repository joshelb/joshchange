const candles = require('candles');
const Gdax = require('gdax');

const product = ['ETH-USD', 'BTC-USD'];
const timeframe = ['30s', '1m', '2m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '24h'];

const options = {
  timediff: {
    enabled: true,
    fixed: false,
    value: 50,
    samples: 1000
  }
}
const Candlecollection = new candles(options);
Candlecollection.addProduct(product, timeframe);

const websocket = new Gdax.WebsocketClient(product);

websocket.on('message', data => {
if (data.time) {
  Candlecollection.adjustClock(data.time);
}

if (data.type === 'match') {
    Candlecollection.SetSeriesPrice(data.product_id, data.price, data.size);
    }
});

Candlecollection.on('close', candle => {
  console.log(candle);
});
const candles = require('candles');

const product = ['KISM-JOSH'];
const timeframe = ['1m', '2m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '24h'];

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

const WebSocket = require('ws');
const websocket = new WebSocket("wss://joshchange.website/wsdata");

websocket.onopen=() => {
    var msg2 = {Type: "subscribe", Stream: "trades", Symbol: "KISM:JOSH", Timeframe: "hrqe", Aggregation: "qrehg", Email: "ewg"}
    websocket.send(JSON.stringify(msg2))
}

var id = ""

websocket.addEventListener("message", function(evt) {
    var received_msg = evt.data;
    var parsed = JSON.parse(received_msg);
    var dt = (parsed["Data"]).reverse() 
    /*if (parsed.timestamp) {
        Candlecollection.adjustClock(parsed.time);
      }
*/
    if (id != dt[0][0]){
        console.log(dt[0][3])
        var date = dt[0][0]
        if (date) {
            Candlecollection.adjustClock(date);
        }
        Candlecollection.SetSeriesPrice("KISM-JOSH", dt[0][3], dt[0][2]);
    }
    var x = Candlecollection.series['KISM-JOSH'].timeframe['1m'].candles;
    console.log(x)
});





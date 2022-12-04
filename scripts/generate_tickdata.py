from clickhouse_driver import Client
import requests
import json

client = Client(host='localhost')
response_API = requests.get('https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=1000')
data = response_API.text
json_obj = json.loads(data)
print(json_obj[0])


for i in json_obj:
    price = i["price"]
    quantity = i["qty"]
    timestamp = i["time"]
    side = "buy"
    if float(quantity) < 0:
        side = "sell"
    time = i["time"]
    client.execute('INSERT INTO tickdata.btcusd VALUES',[(int(timestamp),str(quantity),str(price),side,)],types_check=True)






h = client.execute('SELECT * FROM tickdata.btcusd')
print(h)

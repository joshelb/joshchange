import pandas as pd
import threading
from clickhouse_driver import Client
from datetime import datetime

client = Client(host='localhost',settings={'use_numpy': True})
def generate_Candles():
    threading.Timer(1, generate_Candles).start ()
    df = client.query_dataframe('SELECT TOP (100) * FROM tickdata.btcusd ORDER BY timestamp DESC')
    print(df)
    df['time'] = pd.to_datetime(df['timestamp'],unit='s')
    xd = df.drop(columns=['timestamp'])
    fd = xd.set_index('time')
    fd["price"] = fd["price"].apply(pd.to_numeric, errors='coerce')
    fd["quantity"] = fd["quantity"].apply(pd.to_numeric, errors='coerce')

    resample_obj_1min = fd.resample('1Min')
    candlestick1min = resample_obj_1min['price'].ohlc()
    candlestick1min['volume'] = resample_obj_1min['quantity'].sum()
    candlestick1min['timestamp'] = candlestick1min.index
    candlestick1min['timestamp'] = candlestick1min['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick1min[['open', 'close','high','low','volume','timestamp']] = candlestick1min[['open', 'close','high','low','volume','timestamp']].astype(str)

    resample_obj_5min = fd.resample('5Min')
    candlestick5min = resample_obj_5min['price'].ohlc()
    candlestick5min['volume'] = resample_obj_5min['quantity'].sum()
    candlestick5min['timestamp'] = candlestick5min.index
    candlestick5min['timestamp'] = candlestick5min['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick5min[['open', 'close','high','low','volume','timestamp']] = candlestick5min[['open', 'close','high','low','volume','timestamp']].astype(str)

    resample_obj_15min = fd.resample('15Min')
    candlestick15min = resample_obj_15min['price'].ohlc()
    candlestick15min['volume'] = resample_obj_15min['quantity'].sum()
    candlestick15min['timestamp'] = candlestick15min.index
    candlestick15min['timestamp'] = candlestick15min['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick15min[['open', 'close','high','low','volume','timestamp']] = candlestick15min[['open', 'close','high','low','volume','timestamp']].astype(str)

    resample_obj_30min = fd.resample('30Min')
    candlestick30min = resample_obj_30min['price'].ohlc()
    candlestick30min['volume'] = resample_obj_30min['quantity'].sum()
    candlestick30min['timestamp'] = candlestick30min.index
    candlestick30min['timestamp'] = candlestick30min['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick30min[['open', 'close','high','low','volume','timestamp']] = candlestick30min[['open', 'close','high','low','volume','timestamp']].astype(str)

    resample_obj_1h = fd.resample('1H')
    candlestick1h = resample_obj_1h['price'].ohlc()
    candlestick1h['volume'] = resample_obj_1h['quantity'].sum()
    candlestick1h.index.names = ['timestamp']
    candlestick1h['timestamp'] = candlestick1h.index
    candlestick1h['timestamp'] = candlestick1h['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick1h[['open', 'close','high','low','volume','timestamp']] = candlestick1h[['open', 'close','high','low','volume','timestamp']].astype(str)

    resample_obj_4h = fd.resample('4H')
    candlestick4h = resample_obj_4h['price'].ohlc()
    candlestick4h['volume'] = resample_obj_4h['quantity'].sum()
    candlestick4h['timestamp'] = candlestick4h.index
    candlestick4h['timestamp'] = candlestick4h['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick4h[['open', 'close','high','low','volume','timestamp']] = candlestick4h[['open', 'close','high','low','volume','timestamp']].astype(str)

    resample_obj_1d = fd.resample('1D')
    candlestick1d = resample_obj_1d['price'].ohlc()
    candlestick1d['volume'] = resample_obj_1d['quantity'].sum()
    candlestick1d['timestamp'] = candlestick1d.index
    candlestick1d['timestamp'] = candlestick1d['timestamp'].apply(lambda x: datetime.timestamp(x))
    candlestick1d[['open', 'close','high','low','volume','timestamp']] = candlestick1d[['open', 'close','high','low','volume','timestamp']].astype(str)

    client.insert_dataframe('INSERT INTO candlesticks.btcusd1min VALUES', candlestick1min)
    client.insert_dataframe('INSERT INTO candlesticks.btcusd5min VALUES', candlestick5min)
    client.insert_dataframe('INSERT INTO candlesticks.btcusd15min VALUES', candlestick15min)
    client.insert_dataframe('INSERT INTO candlesticks.btcusd30min VALUES', candlestick30min)
    client.insert_dataframe('INSERT INTO candlesticks.btcusd1h VALUES', candlestick1h)
    client.insert_dataframe('INSERT INTO candlesticks.btcusd4h VALUES', candlestick4h)
    client.insert_dataframe('INSERT INTO candlesticks.btcusd1d VALUES', candlestick1d)

generate_Candles()

from sqlalchemy import create_engine
import pandas as pd


db_connection_str = 'mysql+pymysql://joshelb:chirurgie@localhost/userInfo'
db_connection = create_engine(db_connection_str)

df = pd.read_sql('SELECT * FROM tradeHistoryKISMJOSH', con=db_connection)
print(df)









"""
timeframe = '1min'

tick_data['DATETIME'] = pd.to_datetime(tick_data['DATE'] + ' ' + tick_data['TIME'])
tick_data.set_index('DATETIME', inplace=True)

ohlcv_data = pd.DataFrame(columns=[
    'SYMBOL_N',
    'open',
    'high',
    'low',
    'close',
    'volume'])

for symbol in tick_data['SYMBOL_N'].unique():
    ohlcv_symbol =  tick_data.loc[tick_data['SYMBOL_N'] == symbol, 'PRICE'].resample(timeframe).ohlc()
    ohlcv_symbol['SYMBOL_N'] = symbol
    ohlcv_symbol['volume'] = (tick_data.loc[tick_data['SYMBOL_N'] == symbol, 'VOLUME'].resample(timeframe).max() - tick_data.loc[tick_data['SYMBOL_N'] == symbol, 'VOLUME'].resample(timeframe).max().shift(1))
    ohlcv_data = ohlcv_data.append(ohlcv_symbol, sort=False)

print(ohlcv_data)
"""
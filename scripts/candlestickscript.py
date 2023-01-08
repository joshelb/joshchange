from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime


db_connection_str = 'mysql+pymysql://joshelb:chirurgie@localhost/userInfo'
db_connection = create_engine(db_connection_str)

tick_data = pd.read_sql('SELECT * FROM tradeHistoryKISMJOSH ORDER BY timestamp DESC LIMIT 10', con=db_connection)
tick_data["timestamp"] = tick_data["timestamp"].apply(lambda x: datetime.fromtimestamp(int(x)))
tick_data.set_index('timestamp', inplace=True, drop=True)
print(tick_data)

convert_dict = {'price': float,
                'quantity': float
                }

timeframes = ["1Min","5Min","15Min","30Min","1H","4H","12H","1D"]
candles=[]
while(True):

    for i in timeframes:
        tick_data = tick_data.astype(convert_dict)
        new_row=pd.DataFrame({'uniqueid':float('NaN'), 'userid':float('NaN'), 'side':float('NaN'), 'quantity': float('NaN'), 'price': float('NaN') }, index=[datetime.now()])
        tick_data = pd.concat([tick_data.loc[:],new_row])
        bar = tick_data["price"].resample(i).ohlc()
        vol = tick_data["quantity"].resample(i).sum()
        bar["quantity"] = vol
        bar = bar.fillna(dict.fromkeys(bar.columns.tolist(), bar.close.ffill()))
        table_name = 'candlestickDataKISMJOSH' + i 
        bar.to_sql(con=db_connection, name=table_name, if_exists='replace')
    time.sleep(1)
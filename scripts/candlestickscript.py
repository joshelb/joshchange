from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime


db_connection_str = 'mysql+pymysql://joshelb:chirurgie@localhost/userInfo'
db_connection = create_engine(db_connection_str)

tick_data = pd.read_sql('SELECT * FROM tradeHistoryKISMJOSH', con=db_connection)
tick_data["timestamp"] = tick_data["timestamp"].apply(lambda x: datetime.fromtimestamp(int(x)))
tick_data.set_index('timestamp', inplace=True, drop=True)

convert_dict = {'price': float,
                'quantity': float
                }

tick_data = tick_data.astype(convert_dict)
print(tick_data.dtypes)
bar = tick_data["price"].resample("1Min").ohlc()
vol = tick_data["quantity"].resample("1Min").sum()
bar["quantity"] = vol

print(bar)
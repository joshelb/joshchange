from datetime import datetime
from uuid import UUID
from clickhouse_driver import Client
import pandas as pd

client = Client(host='localhost', settings={'use_numpy': True})


def get_inserted_data():
    return [
            {
                'UserID': UUID('417ddc5d-e556-4d27-95dd-a34d84e40003'),
                'ObjectID': 1003,
                'ObjectClass': 'Class3',
                'Views': [datetime.now(), datetime.now()],
                'RecDate': datetime.now(),
                #'Events': ['aa', 'bb'] # got error "AttributeError: 'list' object has no attribute 'tolist'"
                'Events': []
            }
        ]

data = []

for item in get_inserted_data():
    data.append([
        item['UserID'],
        item['ObjectID'],
        item['ObjectClass'],
        item['Views'],
        item['RecDate'],
        item['Events']
    ])

client.insert_dataframe(
    'INSERT INTO test.rec_eval_data VALUES',
    pd.DataFrame(data, columns=['UserID', 'ObjectID', 'ObjectClass', 'Views', 'RecDate', 'Events'])
)

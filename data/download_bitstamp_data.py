import json
import logging
import sys
import time
import warnings
from datetime import datetime, timedelta
from datetime import timezone
from pathlib import Path

import pandas
import pystore
import requests

from data.settings import BITSTAMP_PYSTORE_PATH, PYSTORE_STORE, PYSTORE_COLLECTION, BITSTAMP_TRADING_PAIRS, PYSTORE_COLLECTION_SANITIZED

warnings.simplefilter(action='ignore', category=FutureWarning)

STEP = 60
LIMIT = 1000


class DownloadBitstampData:

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, *args, **options):
        Path(BITSTAMP_PYSTORE_PATH).mkdir(parents=True, exist_ok=True)
        # How to use PyStore:
        # https://medium.com/@aroussi/fast-data-store-for-pandas-time-series-data-using-pystore-89d9caeef4e2
        pystore.set_path(BITSTAMP_PYSTORE_PATH)
        store = pystore.store(PYSTORE_STORE)
        collection = store.collection(PYSTORE_COLLECTION)
        collection_sanitized = store.collection(PYSTORE_COLLECTION_SANITIZED)

        for pair in BITSTAMP_TRADING_PAIRS:
            print('')
            print('###############################################################################')
            print(f'Processing {pair}')

            ############################################################################################################
            # Get all data available for that pair
            ############################################################################################################
            print(f'Getting data for {pair}')

            exists = pair in collection.list_items()
            if not exists:
                start = int(datetime(2015, 1, 1, tzinfo=timezone.utc).timestamp())
            else:
                start = int(collection.item(pair).to_pandas().index[-1].to_pydatetime().timestamp()) - 3600

            while start < datetime.today().timestamp() - 3600:
                try:
                    url = f'https://www.bitstamp.net/api/v2/ohlc/{pair}/?step={STEP}&start={start}&limit=1000'
                    print(f'{pair} - {datetime.fromtimestamp(start)} - {url}')
                    response = requests.get(url)
                    assert response.status_code == 200
                    response_data = json.loads(response.text)['data']['ohlc']
                    if len(response_data) == 0:
                        time.sleep(0.1)
                        start += 60 * 60 * 24 * 14
                        continue
                    df = pandas.DataFrame(response_data)
                    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)))
                    df = df.set_index('timestamp')
                    df = df.astype(
                        {'open': 'float64', 'high': 'float64', 'low': 'float64', 'close': 'float64',
                         'volume': 'float64'})
                    ohlc = df['close'].resample('1min').ohlc()
                    volume = df['volume'].resample('1min').sum()
                    df = pandas.merge_ordered(ohlc, volume, on='timestamp').set_index('timestamp')
                    if exists:
                        collection.append(pair, df)
                    else:
                        collection.write(pair, df)
                        exists = True
                    start = int(df.index[-1].to_pydatetime().timestamp())
                except Exception:
                    print("Unexpected error:", sys.exc_info()[0])
                    time.sleep(10)

            ############################################################################################################
            # Sanitize and validate the data
            ############################################################################################################

            print(f'Sanitizing data for {pair}')
            df = collection.item(pair).to_pandas()

            df = df.reindex(pandas.date_range(df.index[0], df.index[-1], freq='1min'), fill_value=None)
            df[['volume']] = df[['volume']].fillna(value=0)

            print(f'Validating data for {pair}')

            # Does it have any gaps?
            deltas = df.index.to_series().diff()[1:]
            gaps = deltas[deltas > timedelta(minutes=1)]
            print(gaps.tail())
            if len(gaps) > 0:
                self.print_error(f'There are gaps in {pair}')
                exit(9)

            # Does it have any NaN values in volume?
            if df['volume'].isnull().sum().sum() > 0:
                self.print_error(f'There are NaN values in {pair}')
                exit(9)

            exists = pair in collection_sanitized.list_items()
            if exists:
                collection_sanitized.append(pair, df)
            else:
                collection_sanitized.write(pair, df)

    def print_error(self, msg):
        print()
        print()
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('!!! ERROR')
        print(f'!!! {msg}')
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print()


if __name__ == "__main__":
    download_bitstamp_data = DownloadBitstampData()
    download_bitstamp_data.run()

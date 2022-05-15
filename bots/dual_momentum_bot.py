import math
from datetime import datetime
from time import sleep

import numpy as np
import pandas

from exchange.bitstamp.bitstamp_client import BitstampClient

running = True


def evaluate_coin_of_the_week(client: BitstampClient) -> str:
    # Get data
    pairs = client.getTradingPairsInfo()
    data = {}
    for pair in pairs:
        if pair['trading'] == 'Enabled' and pair['instant_and_market_orders'] == 'Enabled' and pair['url_symbol'].endswith('usd'):
            bars = client.getBars(pair['url_symbol'], 60 * 60 * 24)['data']['ohlc']
            df = pandas.DataFrame(bars)
            df['timestamp'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)))
            df = df.set_index('timestamp')
            df = df.astype(
                {'open': 'float64', 'high': 'float64', 'low': 'float64', 'close': 'float64',
                 'volume': 'float64'})
            ohlc = df['close'].resample('W-SUN').ohlc()
            volume = df['volume'].resample('W-SUN').sum()
            df = pandas.merge_ordered(ohlc, volume, on='timestamp').set_index('timestamp')
            df = df[df.index <= np.datetime64('today') - np.timedelta64(7, 'D')]
            df['volume_avg'] = df.volume.rolling(4).mean()
            df['roc'] = df.close.pct_change(periods=4)
            df['std'] = df.close.rolling(26).std()
            df['std_avg'] = df.close.rolling(26).mean()
            df['std_pct'] = df['std'] / df['std_avg']
            data[pair['url_symbol']] = df

    # Find the highest volume pair
    max_volume_pair = None
    max_volume = 0
    for pair in data:
        vol = data[pair].iloc[-1]['volume_avg'] * data[pair].iloc[-1]['close']

        if math.isnan(vol):
            vol = 0

        if vol > max_volume:
            max_volume = vol
            max_volume_pair = pair

    std_max_vol = data[max_volume_pair].iloc[-1]['std_pct']
    print(f'{max_volume_pair} has the highest volume')

    # Determine the pair to go long
    long = None
    long_roc = -1
    for pair in data:
        roc = data[pair].iloc[-1]['roc']
        std = data[pair].iloc[-1]['std_pct']
        if std <= std_max_vol * 1.05 and roc > 0.01 and roc > long_roc:
            long_roc = roc
            long = pair

    print(f'{long} is coin of the week')

    return long


def reset_orders(client: BitstampClient, long: str):
    pass


bitstamp_client = BitstampClient()

last = datetime.now()
coin_of_the_week = evaluate_coin_of_the_week(bitstamp_client)

while running:
    now = datetime.now()

    if now.weekday() == 6 and last.weekday() == 5:
        coin_of_the_week = evaluate_coin_of_the_week()

    if now.minute % 5 == 0 and last.minute % 5 != 0:
        reset_orders()

    print(f'tick {now}')

    last = now
    sleep(1)

import math

import matplotlib.pyplot as plt
import numpy as np
import pandas
from pandas import DataFrame
import pystore as pystore

from data.settings import BITSTAMP_PYSTORE_PATH, PYSTORE_STORE, PYSTORE_COLLECTION_SANITIZED, BITSTAMP_TRADING_PAIRS
from util.base_command import BaseCommand


class DualMomentumBitstamp1(BaseCommand):

    def run(self, *args, **options):
        self.logger.info(f'Preparing historic data')
        pystore.set_path(BITSTAMP_PYSTORE_PATH)
        store = pystore.store(PYSTORE_STORE)
        collection = store.collection(PYSTORE_COLLECTION_SANITIZED)

        tf = 'W-SUN'
        data = {}
        position = {}
        for pair in BITSTAMP_TRADING_PAIRS:
            df = collection.item(pair).to_pandas()
            ohlc = df['close'].resample(tf).ohlc()
            volume = df['volume'].resample(tf).sum()
            full = pandas.merge(ohlc, volume, left_index=True, right_index=True)
            full['volume_avg'] = full.volume.rolling(4).mean()
            full['roc'] = full.close.pct_change(periods=4)
            full['std'] = full.close.rolling(26).std()
            full['std_avg'] = full.close.rolling(26).mean()
            full['std_pct'] = full['std'] / full['std_avg']
            data[pair] = full
            position[pair] = 0

        current_date = None
        lead = None
        for pair in data:
            begin = data[pair].index[0]
            if current_date is None or current_date > begin:
                current_date = begin
                lead = pair

        self.logger.info(f'First date found in all series is: {current_date} lead is {lead}')

        ####################################################################################################################################
        # Statistics

        # Plot std dev
        # std_df = DataFrame()
        # columns = []
        # for pair in data:
        #     std_df = pandas.merge(std_df, data[pair][['roc1']], left_index=True, right_index=True, how='outer',
        #                           suffixes=(f'', f'_{pair}'), copy=False)
        #     columns.append(pair)
        #
        # std_df.fillna(method='ffill', inplace=True)
        # plt.plot(std_df.index, std_df.values)
        # plt.legend(columns)
        # plt.show()
        # exit(0)

        ####################################################################################################################################
        # Backtest
        results = []
        equity = 1
        ath = 1
        ath_btc = 1
        max_drawdown = []
        hodl_btc = 1
        hodl_eth = 1
        long = None
        last_long = None
        statistics = {'transactions': 0, 'holdings': {}}

        data[lead] = data[lead][data[lead].index >= np.datetime64('today') - np.timedelta64(365, 'D')]

        for index in data[lead].index:
            next_hodl_btc = data['btcusd'].loc[index]['close'] / data['btcusd'].loc[index]['open'] * hodl_btc
            hodl_btc = next_hodl_btc if not math.isnan(next_hodl_btc) else hodl_btc

            if index in data['ethusd'].index:
                hodl_eth = data['ethusd'].loc[index]['close'] / data['ethusd'].loc[index]['open'] * hodl_eth or hodl_eth

            if long is not None:
                equity = data[long].loc[index]['close'] / data[long].loc[index]['open'] * equity
                if statistics['holdings'].get(long) is None:
                    statistics['holdings'][long] = 0
                statistics['holdings'][long] = statistics['holdings'][long] + 1

            if long != last_long:
                equity = equity * .995
                statistics['transactions'] = statistics['transactions'] + 1

            last_long = long

            ath = equity if equity > ath else ath
            drawdown = 1 - equity / ath
            if len(max_drawdown) < 1 or drawdown > max_drawdown[-1][1]:
                max_drawdown.append([index, drawdown, 0])
            else:
                max_drawdown.append([index, max_drawdown[-1][1], 0])
            if ath == equity:
                max_drawdown[-1][1] = 0

            ath_btc = hodl_btc if hodl_btc > ath_btc else ath_btc
            drawdown_btc = 1 - hodl_btc / ath_btc
            if (len(max_drawdown) < 2) or drawdown_btc > max_drawdown[-2][2]:
                max_drawdown[-1][2] = drawdown_btc
            else:
                max_drawdown[-1][2] = max_drawdown[-2][2]
            if ath_btc == hodl_btc:
                max_drawdown[-1][2] = 0

            max_volume_pair = None
            max_volume = 0
            for pair in data:
                if index in data[pair].index:
                    vol = data[pair].loc[index]['volume_avg'] * data[pair].loc[index]['close']
                    if math.isnan(vol):
                        vol = 0
                    if vol >= max_volume:
                        max_volume_pair = pair
                        max_volume = vol

            long = None
            long_roc = -1

            for pair in data:
                std_max_vol = data[max_volume_pair].loc[index]['std_pct']
                if index in data[pair].index:
                    roc = data[pair].loc[index]['roc']
                    std = data[pair].loc[index]['std_pct']
                    if std <= std_max_vol * 1.05 and roc > 0 and roc > long_roc:
                        long_roc = roc
                        long = pair

            results.append([index, equity, hodl_btc, hodl_eth])
            self.logger.info(f'{index} - {max_volume_pair} - {long} - {equity}')

        DataFrame(results, columns=['timestamp', 'equity', 'hodl_btc', 'hodl_eth']).set_index('timestamp').plot()
        DataFrame(max_drawdown, columns=['timestamp', 'max drawdown', 'max drawdown btc']).set_index('timestamp').plot()

        self.logger.info(f'Equity is {equity}')
        self.logger.info(f'Statistics: \n{statistics}')

        plt.show()


if __name__ == "__main__":
    download_bitstamp_data = DualMomentumBitstamp1()
    download_bitstamp_data.run()

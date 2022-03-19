import os
import re
from decimal import Decimal

from exchange.bitstamp.rest.rest_client import RestClient


class BitstampClient:

    def __init__(self, restClient=None):
        if restClient is None:
            clientid = os.environ['BITSTAMP_CLIENT_ID']
            apikey = os.environ['BITSTAMP_API_KEY']
            secret = os.environ['BITSTAMP_SECRET']
            self._restClient = RestClient(clientid, apikey, bytes(secret, 'UTF-8'))
        else:
            self._restClient = restClient

    def getTradingPairsInfo(self):
        return self._restClient.unauthenticated_get_request('api/v2/trading-pairs-info/').json()

    def getBalance(self):
        return self._restClient.request('api/v2/balance/', None).json()

    def getTicker(self, pair):
        return self._restClient.unauthenticated_get_request(f'api/v2/ticker/{pair}/').json()

    def getOpenOrders(self):
        return self._restClient.request('api/v2/open_orders/all/', None).json()

    def cancelOrder(self, id):
        return self._restClient.request('api/v2/cancel_order/', {'id': id}).json()

    def cancelAllOrders(self):
        return self._restClient.request('api/cancel_all_orders/', None).json()

    def buyLimit(self, pair, price, amount):
        return self._restClient.request(f'api/v2/buy/{pair}/', {'price': price, 'amount': amount}).json()

    def buyMarket(self, pair, amount):
        return self._restClient.request(f'api/v2/buy/market/{pair}/', {'amount': amount}).json()

    def sellLimit(self, pair, price, amount):
        return self._restClient.request(f'api/v2/sell/{pair}/', {'price': price, 'amount': amount}).json()

    def sellMarket(self, pair, amount):
        return self._restClient.request(f'api/v2/sell/market/{pair}/', {'amount': amount}).json()

    def getBars(self, pair, timeframe):
        return self._restClient.unauthenticated_get_request(f'api/v2/ohlc/{pair}?step={timeframe}&limit=1000').json()

    def getTransactions(self, since_id):
        return self._restClient.request(f'api/v2/user_transactions/', {'since_id': since_id, 'sort': 'asc'}).json()

    def getEquity(self):
        balance = self.getBalance()
        equity = Decimal(0)
        for (key, value) in balance.items():
            if re.match('[A-Za-z]*_balance', key) and Decimal(value) > 0:
                if key == 'usd_balance':
                    equity = equity + Decimal(value)
                else:
                    left = key.split('_')[0]
                    ticker = self.getTicker(f'{left}usd')
                    equity = equity + Decimal(value) * Decimal(ticker['last'])
        return equity

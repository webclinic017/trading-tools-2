import json
import os
from decimal import Decimal
from unittest import TestCase
from unittest.mock import Mock

from exchange.bitstamp.bitstamp_client import BitstampClient


class BitstampClientTest(TestCase):

    def test_create_rest_client(self):
        os.environ['BITSTAMP_CLIENT_ID'] = 'client id'
        os.environ['BITSTAMP_API_KEY'] = 'api key'
        os.environ['BITSTAMP_SECRET'] = 'supersecret'

        BitstampClient()

        del os.environ['BITSTAMP_CLIENT_ID']
        del os.environ['BITSTAMP_API_KEY']
        del os.environ['BITSTAMP_SECRET']

    def test_get_equity(self):

        # GIVEN
        balance = Mock()
        with open('exchange/bitstamp/balance.json') as json_file:
            balance_json = json.load(json_file)
            balance.json.side_effect = lambda: balance_json

        def get_ticker(url):
            response_mock = Mock()
            if url == 'api/v2/ticker/ethusd/':
                response_mock.json.side_effect = lambda: {'last': '3.0'}
            elif url == 'api/v2/ticker/btcusd/':
                response_mock.json.side_effect = lambda: {'last': '2.0'}
            else:
                raise Exception()
            return response_mock

        def get_balance(url, payload):
            if url == 'api/v2/balance/' and payload is None:
                return balance
            raise Exception()

        rest_client_mock = Mock()
        rest_client_mock.request.side_effect = lambda url, payload: get_balance(url, payload)
        rest_client_mock.unauthenticated_get_request.side_effect = lambda url: get_ticker(url)

        bitstamp_client = BitstampClient(rest_client_mock)

        # WHEN
        equity = bitstamp_client.getEquity()

        # THEN
        self.assertEqual(Decimal('5000.302'), equity)

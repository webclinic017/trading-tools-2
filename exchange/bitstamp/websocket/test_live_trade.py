from decimal import Decimal
from unittest import TestCase

from exchange.bitstamp.websocket.live_trade import LiveTrade


class LiveTradeTest(TestCase):

    def testInit(self):
        trade = LiveTrade(self.getMessage(), "btcusd")
        self.assertEqual('btcusd', trade.instrument)
        self.assertEqual(111557722, trade.id)
        self.assertEqual(Decimal('0.07'), trade.amount)
        self.assertEqual(Decimal('7749.68'), trade.price)
        self.assertEqual(1225817114599424, trade.sellOrderId)
        self.assertEqual(1225817115553792, trade.buyOderId)

    def getMessage(self):
        return {
            'amount': 0.07,
            'amount_str': '0.07000000',
            'buy_order_id': 1225817115553792,
            'id': 111557722,
            'microtimestamp': '1588106731385000',
            'price': 7749.68,
            'price_str': '7749.68',
            'sell_order_id': 1225817114599424,
            'timestamp': '1588106731',
            'type': 0
        }

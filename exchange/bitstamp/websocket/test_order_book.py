from decimal import Decimal
from unittest import TestCase

from exchange.bitstamp.websocket.order_book import OrderBook


class OrderBookTest(TestCase):

    def testInit(self):
        orderBook = OrderBook(self.getMessage())
        self.assertEqual(4, len(orderBook.asks))
        self.assertEqual(3, len(orderBook.bids))
        self.assertEqual(Decimal('7736.36'), orderBook.asks[0].price)
        self.assertEqual(Decimal('0.27470635'), orderBook.asks[0].amount)
        self.assertEqual(Decimal('7734.37'), orderBook.bids[0].price)
        self.assertEqual(Decimal('0.00234762'), orderBook.bids[0].amount)

        lastBid = orderBook.bids[0].price
        for bid in orderBook.bids:
            self.assertLessEqual(bid.price, lastBid)
            lastBid = bid.price

        lastAsk = orderBook.asks[0].price
        for ask in orderBook.asks:
            self.assertGreaterEqual(ask.price, lastAsk)
            lastAsk = ask.price

    def getMessage(self):
        return {
            'asks': [
                ['7736.36', '0.27470635'],
                ['7736.37', '0.17128284'],
                ['7736.38', '2.20572800'],
                ['7736.51', '0.03000000']
            ],
            'bids': [
                ['7734.37', '0.00234762'],
                ['7729.96', '0.03500000'],
                ['7729.85', '0.03500000']
            ],
            'microtimestamp': '1588108309372574',
            'timestamp': '1588108309'
        }

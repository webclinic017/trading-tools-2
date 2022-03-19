from decimal import Decimal


class OrderBook:

    def __init__(self, message):
        self.bids = list(map(Order, message['bids']))
        self.asks = list(map(Order, message['asks']))


class Order:
    def __init__(self, order):
        self.price = Decimal(order[0])
        self.amount = Decimal(order[1])

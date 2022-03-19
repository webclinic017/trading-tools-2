from decimal import Decimal


class LiveTrade:
    def __init__(self, message, instrument):
        self.instrument = instrument
        self.id = message['id']
        self.price = Decimal(message['price_str'])
        self.amount = Decimal(message['amount_str'])
        self.sellOrderId = message['sell_order_id']
        self.buyOderId = message['buy_order_id']

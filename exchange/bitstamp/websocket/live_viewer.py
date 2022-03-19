import signal
import sys
from datetime import datetime
from queue import Queue

from exchange.bitstamp.websocket.order_book import OrderBook
from exchange.bitstamp.websocket.live_trade import LiveTrade
from exchange.bitstamp.websocket.websocket_client import WebsocketClient

lastBid = None
lastAsk = None
lastPrice = None

queue = Queue()
client = WebsocketClient(['btcusd'])
client.register(queue)
client.start()


def signal_handler(sig, frame):
    client.stop()
    client.join()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def print_info():
    print(f'{datetime.now()}: Price - Bid/Ask: {lastPrice} - {lastBid}/{lastAsk}')


while True:
    message = queue.get()
    change = False

    if isinstance(message, LiveTrade):
        change = change or lastPrice != message.price
        lastPrice = message.price

    if isinstance(message, OrderBook):
        change = change or lastBid != message.bids[0].price or lastAsk != message.asks[0].price
        lastBid = message.bids[0].price
        lastAsk = message.asks[0].price

    if change:
        print_info()

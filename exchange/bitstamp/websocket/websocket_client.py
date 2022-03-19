import json
import logging
import threading
from queue import Queue

import websocket

from exchange.bitstamp.websocket.order_book import OrderBook
from exchange.bitstamp.websocket.live_trade import LiveTrade


class WebsocketClient():

    def __init__(self, instruments):
        # websocket.enableTrace(True)
        assert isinstance(instruments, list)
        assert instruments != None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._instruments = instruments
        self._websocket = None
        self._websocketThread = None
        self._stopped = True
        self._queues = []

    def register(self, queue):
        assert isinstance(queue, Queue)
        self._queues.append(queue)

    def stop(self):
        if self._stopped:
            return

        self._stopped = True
        self._websocket.close()
        self._websocketThread.join()
        self._websocket = None
        self._websocketThread = None

    def join(self, timeout=3):
        if self._websocketThread is not None:
            self._websocketThread.join(timeout)

    def start(self):
        if not self._stopped:
            return

        def _on_ws_message(ws, msg):
            message = json.loads(msg)
            if (message['event'] == 'trade'):
                for queue in self._queues:
                    queue.put(LiveTrade(message['data'], message['channel'].split('_')[-1]))
            elif (message['event'] == 'data'):
                for queue in self._queues:
                    queue.put(OrderBook(message['data']))
            elif (message['event'] == 'bts:subscription_succeeded'):
                pass
            else:
                self._logger.info('UNHANDLED: ' + msg)

        def _on_ws_error(ws, error):
            self._logger.info('--- ws client error ---')
            self._logger.info(error)

        def _on_ws_close(ws, code, message):
            self._logger.info(f'ws client close {code} {message}')

        def _on_ws_open(ws):
            self._logger.info("ws client open")
            for instrument in self._instruments:
                msg = json.dumps({
                    "event": "bts:subscribe",
                    "data": {
                        "channel": "live_trades_" + instrument
                    }
                })
                self._websocket.send(msg)
                msg = json.dumps({
                    "event": "bts:subscribe",
                    "data": {
                        "channel": "order_book_" + instrument
                    }
                })
                self._websocket.send(msg)

        def run():
            self._stopped = False
            self._websocket = websocket.WebSocketApp("wss://ws.bitstamp.net",
                                                     on_message=_on_ws_message,
                                                     on_error=_on_ws_error,
                                                     on_close=_on_ws_close)
            self._websocket.on_open = _on_ws_open
            while not self._stopped:
                try:
                    self._websocket.run_forever(ping_interval=10)
                except:
                    pass

        self._websocketThread = threading.Thread(name='wsThread', target=run)
        self._websocketThread.start()

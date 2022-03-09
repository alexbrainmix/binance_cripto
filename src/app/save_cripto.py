from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import logging
import os
import sys
import time
import threading
from unicorn_fy import UnicornFy
from db_creater import DBHelper


logging.basicConfig(filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")
logging.getLogger('unicorn-log').setLevel(logging.ERROR)
logging.getLogger('unicorn-log').addHandler(logging.StreamHandler())

db = DBHelper()
db.create_database("binance_test")

def save_stream_data_from_stream_buffer(binance_websocket_api_manager):
    while True:
        if binance_websocket_api_manager.is_manager_stopping():
            sys.exit(0)
        oldest_stream_data_from_stream_buffer = binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        if oldest_stream_data_from_stream_buffer is False:
            time.sleep(0.01)
        else:
            try:
                # to activate the save function:
                unicorn_fied_stream_data = UnicornFy.binance_websocket(oldest_stream_data_from_stream_buffer)
                # print(unicorn_fied_stream_data)
                db.insertJSON(unicorn_fied_stream_data)
            except Exception:
                # not able to process the data? write it back to the stream_buffer
                binance_websocket_api_manager.add_to_stream_buffer(oldest_stream_data_from_stream_buffer)


# create instance of BinanceWebSocketApiManager and provide the function for stream processing
binance_websocket_api_manager = BinanceWebSocketApiManager()

markets = {'btcusdt','ethusdt'}
channels = {'ticker', 'kline_1m'}

binance_websocket_api_manager.create_stream(channels, markets)

# start a worker process to process to move the received stream_data from the stream_buffer to a print function
worker_thread = threading.Thread(target=save_stream_data_from_stream_buffer, args=(binance_websocket_api_manager,))
worker_thread.start()

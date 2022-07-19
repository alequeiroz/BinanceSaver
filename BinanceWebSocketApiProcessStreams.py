
import logging
import os
import sys
import time
import threading
from unicorn_fy.unicorn_fy import UnicornFy
from tinydb import TinyDB, Query
import banco


logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(level=logging.ERROR,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

class BinanceWebSocketApiProcessStreams(object):

    @staticmethod
    def process_stream_data(received_stream_data_json, stream_buffer_name="False"):
        #
        #  START HERE!
        #
        # `received_stream_data_json` contains one record of raw data from the stream
        # print it and you see the data like its given from Binance, its hard to work with them, because keys of
        # parameters are changing from stream to stream and they are not self explaining.
        #
        # So if you want, you can use the class `UnicornFy`, it converts the json to a dict and prepares the values.
        # `depth5` for example doesnt include the symbol, but the unicornfied set includes them, because the class
        # extracts it from the channel name, makes it upper size and adds it to the returned values.. just print both
        # to see the difference.
        # Github: https://github.com/LUCIT-Systems-and-Development/unicorn-fy
        # PyPI: https://pypi.org/project/unicorn-fy/
        exchange="binance.com-futures"
        if exchange == "binance.com" or exchange == "binance.com-testnet":
            unicorn_fied_stream_data = UnicornFy.binance_com_websocket(received_stream_data_json)
        elif exchange == "binance.com-futures" or exchange == "binance.com-futures-testnet":
            unicorn_fied_stream_data = UnicornFy.binance_com_futures_websocket(received_stream_data_json)
        elif exchange == "binance.com-margin" or exchange == "binance.com-margin-testnet":
            unicorn_fied_stream_data = UnicornFy.binance_com_margin_websocket(received_stream_data_json)
        elif exchange == "binance.com-isolated_margin" or exchange == "binance.com-isolated_margin-testnet":
            unicorn_fied_stream_data = UnicornFy.binance_com_margin_websocket(received_stream_data_json)
        elif exchange == "binance.je":
            unicorn_fied_stream_data = UnicornFy.binance_je_websocket(received_stream_data_json)
        elif exchange == "binance.us":
            unicorn_fied_stream_data = UnicornFy.binance_us_websocket(received_stream_data_json)
        else:
            logging.error("Not a valid exchange: " + str(exchange))

        # Now you can call different methods for different `channels`, here called `event_types`.
        # Its up to you if you call the methods in the bottom of this file or to call other classes which do what
        # ever you want to be done.
        try:
            if type(unicorn_fied_stream_data) == type(True):
                print(unicorn_fied_stream_data)
            elif 'event_type' in unicorn_fied_stream_data:
                if unicorn_fied_stream_data['event_type'] == "aggTrade":
                    BinanceWebSocketApiProcessStreams.aggtrade(unicorn_fied_stream_data)
                elif unicorn_fied_stream_data['event_type'] == "kline":
                    BinanceWebSocketApiProcessStreams.kline(unicorn_fied_stream_data)
                elif unicorn_fied_stream_data['event_type'] == "depthUpdate":
                    BinanceWebSocketApiProcessStreams.depth(unicorn_fied_stream_data)
                else:
                    BinanceWebSocketApiProcessStreams.anything_else(unicorn_fied_stream_data)
            else:
                print(unicorn_fied_stream_data)
                return
        except KeyError as e:
            print("KeyError: ", e, ' unicorn_fied_stream_data: ', unicorn_fied_stream_data)
            print('Keyerror: ', e, )
            BinanceWebSocketApiProcessStreams.anything_else(unicorn_fied_stream_data)
            
            pass
            return
        except TypeError:
            pass

    @staticmethod
    def aggtrade( stream_data):
        #SALVANDO BANCO
        banco.buffer.append(stream_data)  
        
    @staticmethod
    def kline(stream_data):
        if stream_data['kline']['is_closed']:
            banco.buffer.append(stream_data)

    @staticmethod
    def depth(stream_data):
        banco.buffer.append(stream_data)

    @staticmethod
    def anything_else(stream_data):
        print('\n\n outro stream: ', stream_data)
       
   
   
        




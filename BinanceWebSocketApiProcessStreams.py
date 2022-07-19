
import logging
import os
import sys
import time
import threading
from unicorn_fy.unicorn_fy import UnicornFy
from tinydb import TinyDB, Query
import banco


logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

prcoAntigo = 0.0


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
        #print(unicorn_fied_stream_data)
        try:
            if type(unicorn_fied_stream_data) == type(True):
                print(unicorn_fied_stream_data)
            elif 'event_type' in unicorn_fied_stream_data:
                #print('data: ', unicorn_fied_stream_data)
                if unicorn_fied_stream_data['event_type'] == 'ticker':
                    BinanceWebSocketApiProcessStreams.ticker(unicorn_fied_stream_data)
                elif unicorn_fied_stream_data['event_type'] == "aggTrade":
                    BinanceWebSocketApiProcessStreams.aggtrade(unicorn_fied_stream_data)
                elif unicorn_fied_stream_data['event_type'] == "kline":
                    BinanceWebSocketApiProcessStreams.kline(unicorn_fied_stream_data)
                # elif unicorn_fied_stream_data['event_type'] == "24hrMiniTicker":
                #     BinanceWebSocketApiProcessStreams.miniticker(unicorn_fied_stream_data)
                # elif unicorn_fied_stream_data['event_type'] == "24hrTicker":
                #     BinanceWebSocketApiProcessStreams.ticker(unicorn_fied_stream_data)
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

        #{'stream_type': 'ethusdt@aggTrade', 'event_type': 'aggTrade', 'event_time': 1654978467387, 'symbol': 'ETHUSDT', 
        #'aggregate_trade_id': 873844314, 'price': '1540.00', 'quantity': '0.025', 'first_trade_id': 1728417724, 
        #'last_trade_id': 1728417724, 'trade_time': 1654978467230, 'is_market_maker': True, 'unicorn_fied': ['binance.com-futures', '0.12.2']}
        #print('\n\naggtrade--', stream_data)
        #SALVANDO BANCO
        banco.buffer.append(stream_data)

    @staticmethod
    def trade(stream_data):
        #{"stream":"bnbusdt@trade","data":{"e":"trade","E":1658231646897,"T":1658231646880,"s":"BNBUSDT","t":753331507,"p":"261.720","q":"0.19","X":"MARKET","m":false}}
        # print `trade` data
        nome = str(stream_data['symbol'])
        #print('\n\ntrade--', stream_data)
        #print('. ',config.precin, end = ' ')
        #print('\nlista preco moedas:',config.listaPrecoMoedas, '\n stream data:\n', stream_data)
        
        
    @staticmethod
    def kline(stream_data):
        # {"stream":"bnbusdt@kline_12h","data":{"e":"kline","E":1658231646896,"s":"BNBUSDT","k":{"t":1658188800000,"T":1658231999999,"s":"BNBUSDT","i":"12h","f":752947668,
        # "L":753331506,"o":"263.960","c":"261.720","h":"267.670","l":"255.650","v":"916400.55","n":383836,"x":false,"q":"238845698.24327","V":"445981.95","Q":"116239248.95334","B":"0"}}}
        
        # em kline verificar se é o ultimo para salvar 
        if stream_data['kline']['is_closed']:
            #print('\n\nkline--', stream_data)
            banco.buffer.append(stream_data)
            

    @staticmethod
    def miniticker(stream_data):
        # print `miniTicker` data
        1+1
        #print('\n\nminiticker', stream_data)

    @staticmethod
    def ticker(stream_data):
        # print `ticker` data
        1+1
        print('\n\nticker',stream_data)

    @staticmethod
    def depth(stream_data):
        # print `depth` data
        banco.buffer.append(stream_data)

    @staticmethod
    def outboundAccountInfo(stream_data):
        # print `outboundAccountInfo` data from userData stream
        1+1#print('\n\noutboundAccountInfo', stream_data)

    @staticmethod
    def executionReport(stream_data):
        # print `executionReport` data from userData stream
        1+1#print('\n\nexecutionReport', stream_data)

    @staticmethod
    def anything_else(stream_data):
         # print `trade` data
        #nome = str(stream_data['symbol'])
        #preco = stream_data['price']
        #config.listaPrecoMoedas[nome] = float(preco)
        print('\n\n outro stream: ', stream_data)
       
   
   
        




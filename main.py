from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import unicorn_fy
import logging
import os
import sys
import time
import threading
from tinydb import TinyDB, Query, table, where
from tinydb.table import Document
from tinydb.operations import add
import sqlite3
from BinanceWebSocketApiProcessStreams import BinanceWebSocketApiProcessStreams
import banco


logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

db:sqlite3

def pegarMoedas():
    print('pegando moedas')
    database = TinyDB('moedas.json')
    tabelaMoedas = database.table('moedas')#atualiza a tabela de trade do banco
    moedas = tabelaMoedas.all()
    for moeda in moedas:
        banco.markets.add(moeda['simbolo'])

    print('moedas selecionadas:', banco.markets)



def main():
    db = banco.iniciaBanco()
    pegarMoedas()
    banco.criarTabela()
    # create instance of BinanceWebSocketApiManager and provide the function for stream processing
    binance_websocket_api_manager = BinanceWebSocketApiManager(BinanceWebSocketApiProcessStreams.process_stream_data, exchange="binance.com-futures") #output_default: set to "dict" to convert the received raw data to a python dict, set to "UnicornFy"
    #{'kline_1m', 'kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'aggTrade', 'depth@100ms'}
    binance_websocket_api_manager.create_stream('kline_1m', banco.markets, stream_label='kline_1m')#{'bnbusdt','wavesusdt', 'btcusdt'}
    binance_websocket_api_manager.create_stream('kline_5m', banco.markets, stream_label='kline_5m')
    binance_websocket_api_manager.create_stream('kline_15m', banco.markets, stream_label='kline_15m')
    binance_websocket_api_manager.create_stream('kline_30m', banco.markets, stream_label='kline_30m')
    binance_websocket_api_manager.create_stream('kline_1h', banco.markets, stream_label='kline_1h')
    binance_websocket_api_manager.create_stream('kline_12h', banco.markets, stream_label='kline_12h')
    binance_websocket_api_manager.create_stream('aggTrade', banco.markets, stream_label='aggTrade')
    binance_websocket_api_manager.create_stream('depth@100ms', banco.markets, stream_label='depth@100ms')


    # start a restful api server to report the current status to 'tools/icinga/check_binance_websocket_manager' which can be
    # used as a check_command for ICINGA/Nagios
    #binance_websocket_api_manager.start_monitoring_api(warn_on_update=False)
    binance_websocket_api_manager.start_monitoring_api(port=5013, warn_on_update=True)
    

    # if you like to not only listen on localhost use 'host="0.0.0.0"'
    # for a specific port do 'port=80'
    # binance_websocket_api_manager.start_monitoring_api(host="0.0.0.0", port=80)

    print("Websockets started!")
    while True:
        binance_websocket_api_manager.print_summary()
        banco.atualizarBanco()
        time.sleep(1)


if __name__ == '__main__':
    main()


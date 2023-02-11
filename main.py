from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import logging
import os
import time
import threading
from tinydb import TinyDB, Query, table, where
from tinydb.table import Document
from tinydb.operations import add
import sqlite3
from BinanceWebSocketApiProcessStreams import BinanceWebSocketApiProcessStreams
import banco


logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(level=logging.ERROR,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

db:sqlite3

def pegarMoedas():
    print('Cadastrando moedas')
    database = TinyDB('moedas.json')
    tabelaMoedas = database.table('moedas')#atualiza a tabela de trade do banco
    moedas = tabelaMoedas.all()
    for moeda in moedas:
        banco.markets.add(moeda['simbolo'])

    print('moedas selecionadas:', banco.markets)

def monitorarConexao(binance_websocket_api_manager):
    while True: 
        binance_websocket_api_manager.print_summary()

        time.sleep(1)

def main():
    db = banco.iniciaBanco()
    pegarMoedas()
    banco.criarTabela()
    # create instance of BinanceWebSocketApiManager and provide the function for stream processing
    binance_websocket_api_manager = BinanceWebSocketApiManager(BinanceWebSocketApiProcessStreams.process_stream_data, exchange="binance.com-futures") #output_default: set to "dict" to convert the received raw data to a python dict, set to "UnicornFy"
    #{'kline_1m', 'kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'aggTrade', 'depth@100ms'}
    # binance_websocket_api_manager.create_stream('kline_1m', banco.markets, stream_label='kline_1m')#{'bnbusdt','wavesusdt', 'btcusdt'}
    # binance_websocket_api_manager.create_stream('kline_5m', banco.markets, stream_label='kline_5m')
    # binance_websocket_api_manager.create_stream('kline_15m', banco.markets, stream_label='kline_15m')
    # binance_websocket_api_manager.create_stream('kline_30m', banco.markets, stream_label='kline_30m')
    # binance_websocket_api_manager.create_stream('kline_1h', banco.markets, stream_label='kline_1h')
    # binance_websocket_api_manager.create_stream('kline_12h', banco.markets, stream_label='kline_12h')
    # binance_websocket_api_manager.create_stream('aggTrade', banco.markets, stream_label='aggTrade')
    # binance_websocket_api_manager.create_stream('depth@100ms', banco.markets, stream_label='depth@100ms')
    for moeda in banco.markets:
        print("criando stream para", moeda)
        binance_websocket_api_manager.create_stream('aggTrade', moeda, stream_label='aggTrade '+moeda)

    #binance_websocket_api_manager.start_monitoring_api(port=5013, warn_on_update=True)

    threading.Thread(target=monitorarConexao, args=(binance_websocket_api_manager,), name='MONITORAR WEBSOCKET').start()

    print("Websockets iniciados!")

    while True:

        banco.atualizarBanco()
        


if __name__ == '__main__':
    main()


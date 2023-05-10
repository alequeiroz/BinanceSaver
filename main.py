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

from fastapi import FastAPI, HTTPException, Query, Request

app = FastAPI()

logging.getLogger("unicorn_binance_websocket_api")
logging.basicConfig(level=logging.ERROR,
                    filename=os.path.basename(__file__) + '.log',
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                    style="{")

db:sqlite3
binance_websocket_api_manager:BinanceWebSocketApiManager
markets=set()

csv_path = 'D:\\datasets'

@app.on_event("startup")
async def startup_event():
    print("startupss")
    start_saver()

@app.on_event("shutdown")
async def startup_event():
    import banco
    binance_websocket_api_manager.stop_manager_with_all_streams()
    banco.encerraBanco()

@app.post("/add_moeda")
def read_root(request: Request, senha: str, moeda:str):
    if senha != '123':
        raise HTTPException(status_code=404, detail="Senha incorreta")
    else:
        adicionar_moeda(moeda)
        return {"Moeda adicionada!": moeda}

@app.get("/verificar_status_stream")
def read_root(request: Request, senha: str, moeda:str):
    if senha != '123':
        raise HTTPException(status_code=404, detail="Senha incorreta")
    else:
        str = binance_websocket_api_manager.print_summary(disable_print=False)
        return {"Summary": str}

def adicionar_moeda(moeda:str):
    try:
        database = TinyDB('moedas.json')
        tabelaMoedas = database.table('moedas')#atualiza a tabela de trade do banco
        moedas = tabelaMoedas.all()
        tamanho = len(moedas)
        database.table('moedas').insert({"simbolo": moeda}) #atualiza a tabela de trade do banco
        database.close()
    except Exception as e:
        print("erro ao adiciopnar moeda", e )
        return False

    return True    

def pegarMoedas():
    print('Cadastrando moedas')
    import banco
    global markets
    database = TinyDB('moedas.json')
    tabelaMoedas = database.table('moedas')#atualiza a tabela de trade do banco
    moedas = tabelaMoedas.all()
    for moeda in moedas:
        markets.add(moeda['simbolo'])
        banco.markets.add(moeda['simbolo'])

    print('moedas selecionadas:', markets)

def monitorarConexao(binance_websocket_api_manager):
    while True: 
        binance_websocket_api_manager.print_summary()
        time.sleep(5)

def atualiza_banco():
    import banco
    db = banco.iniciaBanco()
    time.sleep(3)

    while True:
        banco.atualizarBanco()
        
def start_saver():
    try:
        print("starting server")
        pegarMoedas()
        threading.Thread(target=atualiza_banco, name='Monitorar Banco').start()
        
        global binance_websocket_api_manager, markets
        # create instance of BinanceWebSocketApiManager and provide the function for stream processing
        binance_websocket_api_manager = BinanceWebSocketApiManager(BinanceWebSocketApiProcessStreams.process_stream_data, exchange="binance.com-futures") #output_default: set to "dict" to convert the received raw data to a python dict, set to "UnicornFy"
        #{'kline_1m', 'kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'aggTrade', 'depth@100ms'}
        binance_websocket_api_manager.create_stream('kline_1m', markets, stream_label='kline_1m')#{'bnbusdt','wavesusdt', 'btcusdt'}
        binance_websocket_api_manager.create_stream('kline_5m', markets, stream_label='kline_5m')
        binance_websocket_api_manager.create_stream('kline_15m', markets, stream_label='kline_15m')
        binance_websocket_api_manager.create_stream('kline_30m', markets, stream_label='kline_30m')
        # binance_websocket_api_manager.create_stream('kline_1h', banco.markets, stream_label='kline_1h')
        # binance_websocket_api_manager.create_stream('kline_12h', banco.markets, stream_label='kline_12h')
        binance_websocket_api_manager.create_stream('aggTrade', markets, stream_label='aggTrade')
        binance_websocket_api_manager.create_stream('depth@100ms', markets, stream_label='depth@100ms')
        #binance_websocket_api_manager.start_monitoring_api(port=5013, warn_on_update=True)

        threading.Thread(target=monitorarConexao, args=(binance_websocket_api_manager,), name='Monitorar Websocket').start()
        print("Websockets iniciados!")
        
    except Exception as e:
        print("FUDEO O GOIAIS LEK", e)
        

start_saver()
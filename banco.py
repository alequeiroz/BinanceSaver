import sqlite3
import time, json

from pydantic import conint

banco = sqlite3.connect('profitor.db')
cur = banco.cursor()

markets = set()

channels = {'kline_1m', 'kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'aggTrade', 'depth@100ms'}

buffer = []


def iniciaBanco():
    print('Iniciando banco')
    try:
        banco = sqlite3.connect('profitor.db')
    except Exception as e:
        print('Erro ao conectar no banco: ',e)

    return banco
    

def criarTabela(): 
    print('criando tabelas') 
    coin:str  
    for coin in markets:
        moeda = coin.upper()
        for canal in channels: #{'trade', 'kline_1m', 'kline_5m', 'kline_15m', 'kline_30m', 'kline_1h', 'kline_12h', 'depth20', 'aggTrade', 'ticker'}
            if canal == 'aggTrade':
                #consultar se tabela existe para criar
                nome:str
                nome = moeda+'@'+canal
                #print('criando tabela aggTrade da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_aggTrade (PRECO REAL, QUANTIDADE REAL, TEMPO INT)'
                cur.execute(comando)

            elif canal == 'kline_1m':
                #consultar se tabela existe para criar
                # em kline verificar se é o ultimo para salvar 
                nome:str
                nome = moeda+'@'+canal
                #print('criando tabela kline_1m da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_kline_1m (STIME INT , CTIME INT,  OPEN REAL, CLOSE REAL, HIGH REAL, LOW REAL, VOL REAL, NTRADES INT, QUOTE REAL, TAKERVOLUME REAL,  TAKERQUOTEVOLUME REAL)'
                cur.execute(comando)
            
            elif canal == 'kline_5m':
                nome:str
                nome = moeda+'@'+canal
                #print('criando tabela kline_5m da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_kline_5m (STIME INT , CTIME INT,  OPEN REAL, CLOSE REAL, HIGH REAL, LOW REAL, VOL REAL, NTRADES INT, QUOTE REAL, TAKERVOLUME REAL,  TAKERQUOTEVOLUME REAL)'
                cur.execute(comando)
            
            elif canal == 'kline_15m':
                nome:str
                nome = moeda+'@'+canal
                #print('criando tabela kline_15m da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_kline_15m (STIME INT , CTIME INT,   OPEN REAL, CLOSE REAL, HIGH REAL, LOW REAL, VOL REAL, NTRADES INT, QUOTE REAL, TAKERVOLUME REAL,  TAKERQUOTEVOLUME REAL)'
                cur.execute(comando)

            elif canal == 'kline_30m':
                nome:str
                nome = moeda+'@'+canal
                #print('criando tabela kline_30m da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_kline_30m (STIME INT , CTIME INT,  OPEN REAL, CLOSE REAL, HIGH REAL, LOW REAL, VOL REAL, NTRADES INT, QUOTE REAL, TAKERVOLUME REAL,  TAKERQUOTEVOLUME REAL)'
                cur.execute(comando)

            elif canal == 'kline_1h':
  
                #print('criando tabela kline_1h da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_kline_1h (STIME INT , CTIME INT,  OPEN REAL, CLOSE REAL, HIGH REAL, LOW REAL, VOL REAL, NTRADES INT, QUOTE REAL, TAKERVOLUME REAL,  TAKERQUOTEVOLUME REAL)'
                cur.execute(comando)

            elif canal == 'kline_12h':

               #print('criando tabela kline_12h da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_kline_12h (STIME INT , CTIME INT,  OPEN REAL, CLOSE REAL, HIGH REAL, LOW REAL, VOL REAL, NTRADES INT, QUOTE REAL, TAKERVOLUME REAL,  TAKERQUOTEVOLUME REAL)'
                cur.execute(comando)
            
            elif canal == 'depth@100ms':
                #print('criando tabela depth da moeda ', moeda)
                comando = 'CREATE TABLE IF NOT EXISTS '+moeda+'_depthUpdate (TIME INT , TIMEI INT,  TIMEF INT, ASK TEXT)'
                cur.execute(comando)



         
                
def salvarDepth(payload):
    
    #print('especs',payload['symbol'],' :',type(payload['asks']), len(payload['asks']))

    comando:str
    comando = "INSERT INTO "+payload['symbol']+'_depthUpdate VALUES ('+str(payload['event_time'])+','+str(payload['first_update_id_in_event'])+','+str(payload['final_update_id_in_event'])+',"'+str(payload['asks'])+'")'
    #print('\n\n********\nsalvando aggtrade: ', comando)
    cur.execute(comando)
    banco.commit()


def salvarAggtrade(payload):
    comando:str
    comando = "INSERT INTO "+payload['symbol']+'_aggTrade VALUES ('+payload['price']+','+payload['quantity']+','+str(payload['trade_time'])+')'
    #print('\n\n********\nsalvando aggtrade: ', comando)
    cur.execute(comando)
    banco.commit()

def salvarKline(payload):
    if payload['kline']['interval'] == '1m':
        comando:str
        comando = "INSERT INTO "+payload['kline']['symbol']+'_kline_1m VALUES ('+str(payload['kline']['kline_start_time'] )+','+str(payload['kline']['kline_close_time'])+','+payload['kline']['open_price']+','+payload['kline']['close_price']+','+payload['kline']['high_price']+','+payload['kline']['low_price']+','+payload['kline']['base_volume']+','+str(payload['kline']['number_of_trades'])+','+payload['kline']['quote']+','+payload['kline']['taker_by_base_asset_volume']+','+payload['kline']['taker_by_quote_asset_volume']+')'

    elif payload['kline']['interval'] == '5m':
        comando:str
        comando = "INSERT INTO "+payload['kline']['symbol']+'_kline_5m VALUES ('+str(payload['kline']['kline_start_time'] )+','+str(payload['kline']['kline_close_time'])+','+payload['kline']['open_price']+','+payload['kline']['close_price']+','+payload['kline']['high_price']+','+payload['kline']['low_price']+','+payload['kline']['base_volume']+','+str(payload['kline']['number_of_trades'])+','+payload['kline']['quote']+','+payload['kline']['taker_by_base_asset_volume']+','+payload['kline']['taker_by_quote_asset_volume']+')'
    
    elif payload['kline']['interval'] == '15m':
        comando:str
        comando = "INSERT INTO "+payload['kline']['symbol']+'_kline_15m VALUES ('+str(payload['kline']['kline_start_time'] )+','+str(payload['kline']['kline_close_time'])+','+payload['kline']['open_price']+','+payload['kline']['close_price']+','+payload['kline']['high_price']+','+payload['kline']['low_price']+','+payload['kline']['base_volume']+','+str(payload['kline']['number_of_trades'])+','+payload['kline']['quote']+','+payload['kline']['taker_by_base_asset_volume']+','+payload['kline']['taker_by_quote_asset_volume']+')'
        #print('\n\n----\nsalvando aggtrade: ', comando)

    elif payload['kline']['interval'] == '1h':
        comando:str
        comando = "INSERT INTO "+payload['kline']['symbol']+'_kline_1h VALUES ('+str(payload['kline']['kline_start_time'] )+','+str(payload['kline']['kline_close_time'])+','+payload['kline']['open_price']+','+payload['kline']['close_price']+','+payload['kline']['high_price']+','+payload['kline']['low_price']+','+payload['kline']['base_volume']+','+str(payload['kline']['number_of_trades'])+','+payload['kline']['quote']+','+payload['kline']['taker_by_base_asset_volume']+','+payload['kline']['taker_by_quote_asset_volume']+')'
        #print('\n\n----\nsalvando aggtrade: ', comando)

    elif payload['kline']['interval'] == '12h':
        comando:str
        comando = "INSERT INTO "+payload['kline']['symbol']+'_kline_12h VALUES ('+str(payload['kline']['kline_start_time'] )+','+str(payload['kline']['kline_close_time'])+','+payload['kline']['open_price']+','+payload['kline']['close_price']+','+payload['kline']['high_price']+','+payload['kline']['low_price']+','+payload['kline']['base_volume']+','+str(payload['kline']['number_of_trades'])+','+payload['kline']['quote']+','+payload['kline']['taker_by_base_asset_volume']+','+payload['kline']['taker_by_quote_asset_volume']+')'
        #print('\n\n----\nsalvando aggtrade: ', comando)
    cur.execute(comando)
    banco.commit()

def atualizarBanco():
    print('iniciando atualização do banco')
    for atualizacao in buffer[:]:
        if atualizacao['event_type'] == 'kline':
            try:
                salvarKline(atualizacao)
            except Exception as e:
                print('erro:', e)
            buffer.remove(atualizacao)
        
        elif atualizacao['event_type'] == 'aggTrade':
            try:
                salvarAggtrade(atualizacao)
            except Exception as e:
                print('erro:', e)
            
            buffer.remove(atualizacao)
        
        elif atualizacao['event_type'] == 'depthUpdate':
            try:
                #print('salvando depth')
                salvarDepth(atualizacao)
            except Exception as e:
                print('erro:', e)
            buffer.remove(atualizacao)



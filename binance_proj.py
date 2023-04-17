import configparser
import datetime
import pandas as pd
from binance import Client

start_time = (datetime.datetime.now())

config = configparser.ConfigParser()
config.read('auth.ini')

binance_api_key = config['binance']['api_key']
binance_secret_key = config['binance']['secret_key']

client = Client(binance_api_key, binance_secret_key)

info = client.get_all_tickers()

tickers_df = pd.DataFrame(info)

tickers_df_usdt = tickers_df.query('symbol.str.contains("USDT") and not symbol.str.contains("UP") and not symbol.str.contains("DOWN") and not symbol.str.contains("BEAR") and not symbol.str.contains("BULL")')

data = pd.DataFrame(columns = ['timestamp', 'open', 'close', 'symbol'])

for i in tickers_df_usdt.symbol:
    print(i)
    symbol = i

    klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, '1 Jan, 2017')

    temp_df = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    temp_df = temp_df[['timestamp', 'open', 'close']]
    temp_df['symbol'] = symbol
    data = pd.concat([data, temp_df])

data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
data = data.astype({'open':'float', 'close':'float'})
data[['open', 'close']] = data[['open', 'close']].round(2)
data = data.sort_values('timestamp')
data['currency'] = data.symbol.replace({'USDT' : ''}, regex = True)
data['dayly_grow'] = data.close > data.open
data['dayly_grow_percent'] = (1- data.close.div(data.open)).mul(100).round(2)
data['dayly_grow_value'] = (data.close - data.open).abs().round(2)


samp = data.query('dayly_grow_percent.abs() > 10 and dayly_grow_value > 1').sort_values(['symbol', 'timestamp'])

samp.to_csv('result.csv', sep = ';', index = False)

finish_time = (datetime.datetime.now())
print(finish_time-start_time)
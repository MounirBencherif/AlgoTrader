from binance import Client
import os
import pandas as pd 

class BinanceAPI:
    DATABASE = "../pycoin/data/binance"
    FREQS = {
        "1m": Client.KLINE_INTERVAL_1MINUTE,
        "5m": Client.KLINE_INTERVAL_5MINUTE,
        "15m": Client.KLINE_INTERVAL_15MINUTE,
        "30m": Client.KLINE_INTERVAL_30MINUTE,
        "1h": Client.KLINE_INTERVAL_1HOUR,
        "4h": Client.KLINE_INTERVAL_4HOUR,
        "1d": Client.KLINE_INTERVAL_1DAY,
        "1w": Client.KLINE_INTERVAL_1WEEK
    }
    @staticmethod
    def get_data(ticker, start, end, frequency, lazy=True):
        file_ = BinanceAPI.location(ticker, start, end, frequency)
        file = os.path.join(file_, f"{start}_{end}.csv")
        if os.path.exists(file) and lazy:
            return pd.read_csv(file)
        client = Client()
        klines = client.get_historical_klines(ticker, BinanceAPI.FREQS[frequency], start, end)
        #convert all_data to a dataframe
        df = pd.DataFrame(klines, columns =['open_time', 'open', 'high', 'low', 'close',
                                              'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 
                                              'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        #keep only relevant columns 
        df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
        #convert to floats
        df = df.astype(float)
        #Convert open_time to datetime
        df['open_time'] = pd.to_datetime(df['open_time'], unit = 'ms')
        os.makedirs(file_, exist_ok=True)
        df.to_csv(file, index=False)
        return df 
            
    @staticmethod
    def location(ticker, start, end, frequency):
        return os.path.join(BinanceAPI.DATABASE, ticker, frequency)
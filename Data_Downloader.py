from __future__ import absolute_import, with_statement
import os.path
from binance.client import Client
import pickle
from pybit.usdt_perpetual import HTTP
import csv
from datetime import datetime
import numpy as np
from datetime import timezone
import settings
import time

def cut_midnight(filepath, fundamental_period, print_info):
    '''Helper. Loads the data at filepath, and overwrite it with cut data that that starts at the closest timestamps == midnight
    and last candle starts at midnight - fundamental_period
    #therefore doesnt work for period larger than one day:'''
    assert fundamental_period < 1440, 'not implemented for candles larger or equal to one day'
    # see below, wont work either for non multple of 60 if period > 60
    if fundamental_period >60:
        assert fundamental_period%60 ==0, 'candle size must be multiple of one hour'

    data = np.load(filepath)
    # data shall start at midnight
    if datetime.fromtimestamp(data[0,0], tz = timezone.utc).hour == 0 \
            and datetime.fromtimestamp(data[0,0], tz = timezone.utc).minute ==0:
        pass
    else:
        k = 0
        found = False
        while not found:
            if datetime.fromtimestamp(data[k, 0], tz = timezone.utc).hour == 0 \
                    and datetime.fromtimestamp(data[k, 0], tz = timezone.utc).minute == 0:
                found = True
            else:
                k += 1
        data = data[k:, :]

    # data shall end at midnight - period
    if fundamental_period <=60:
        hour_stop = 23
        minute_stop = 60 - fundamental_period
    else:
        hour_stop = 24 - fundamental_period//60
        minute_stop = 0
    if datetime.fromtimestamp(data[-1, 0], tz=timezone.utc).hour == hour_stop \
            and datetime.fromtimestamp(data[-1, 0], tz=timezone.utc).minute == minute_stop:
        pass
    else:
        data = np.flipud(data)
        k = 0
        found = False
        while not found:
            if datetime.fromtimestamp(data[k, 0], tz=timezone.utc).hour == hour_stop \
                    and datetime.fromtimestamp(data[k, 0],tz=timezone.utc).minute == minute_stop:
                found = True
            else:
                k += 1
        data = data[k:, :]
        data = np.flipud(data)

    #saving data to same filepath (overwrite)
    np.save(filepath, data)

    if print_info:
        data = np.load(filepath)
        print('data starts/ends at', datetime.fromtimestamp(data[0, 0], tz=timezone.utc),
              datetime.fromtimestamp(data[-1, 0], tz=timezone.utc))


class Binance_Downloader():

    def __init__(self):
        api_key = settings.binance_public_key
        api_secret = settings.binance_secret_key
        assert len(api_key) !=0, 'you need to provide a valid api key for binance'
        assert len(api_secret) !=0, 'you need to provide a valid api key for binance'
        self.client = Client(api_key, api_secret)
        # todo probably doenst require auth ; check API docs

    def download(self, symbol, fundamental_period, start, end = None, save_file = True, print_info = True):
        '''Downloads the data
        :param symbol: str, eg. 'BTCUSDT'
        :param fundamental_period: int; candle size in minutes
        :param start, end : datetime objects in utc timezone; end is optional; if end is none, end = now()
        :param savefile: optional
        If start date is before actual data exists, binance will automatically starts the download at
        the first available timestamp
        '''

        if not os.path.exists('binance_data/'):
            os.makedirs('binance_data/')
        if not os.path.exists('binance_data/downloads/'):
            os.makedirs('binance_data/downloads')

        # needs to have format, eg. "1 Jun, 2020"
        binance_start = str(start.day) + ' ' + self.number_to_month(start.month) + ', ' + str(start.year)
        if end is None:
            datetime_now = datetime.now()
            binance_end = str(datetime_now.day) + ' ' + self.number_to_month(datetime_now.month) + ', ' + str(datetime_now.year)
        else:
            binance_end = str(end.day) + ' ' + self.number_to_month(end.month) + ', ' + str(end.year)

        #enums
        if fundamental_period == 1:
            interval = self.client.KLINE_INTERVAL_1MINUTE
        elif fundamental_period == 5:
            interval = self.client.KLINE_INTERVAL_5MINUTE
        elif fundamental_period == 15:
            interval = self.client.KLINE_INTERVAL_15MINUTE
        elif fundamental_period == 30:
            interval = self.client.KLINE_INTERVAL_30MINUTE
        elif fundamental_period == 60:
            interval = self.client.KLINE_INTERVAL_1HOUR
        elif fundamental_period == 120:
            interval = self.client.KLINE_INTERVAL_2HOUR
        elif fundamental_period == 240:
            interval = self.client.KLINE_INTERVAL_4HOUR
        elif fundamental_period == 360:
            interval = self.client.KLINE_INTERVAL_6HOUR
        elif fundamental_period == 480:
            interval = self.client.KLINE_INTERVAL_8HOUR
        elif fundamental_period == 720:
            interval = self.client.KLINE_INTERVAL_12HOUR
        elif fundamental_period == 1440:
            interval = self.client.KLINE_INTERVAL_1DAY
        else:
            print('candle size not supported')
            raise NotImplementedError

        filepath_csv = 'binance_data/downloads/' + symbol + str(fundamental_period) + '.csv'
        klines = self.client.get_historical_klines(
            symbol,
            interval,
            binance_start,
            binance_end,
        )
        alldata = self.from_klines_to_list(klines)

        with open(filepath_csv, 'w') as myfile:
            csvwriter = csv.writer(myfile)
            for row in alldata:
                # append only valid points / might happen:
                if datetime.fromtimestamp(row[0]).minute%fundamental_period !=0 and datetime.fromtimestamp(row[0]).second !=0:
                    pass
                else:
                    csvwriter.writerow(row)

        myfile.close()

        filepath_npy = 'binance_data/downloads/' + symbol + str(fundamental_period) + '.npy'
        if save_file:
            d = np.loadtxt(filepath_csv, delimiter=',')
            np.save(filepath_npy, d)
            #default we start data at midnight/end at midnight
            cut_midnight(filepath_npy, fundamental_period, print_info)
            return
        else:
            return np.loadtxt(filepath_csv, delimiter=',')

    def from_klines_to_list(self, klines):
        alldata = []
        for elem in klines:
            #format open timestamp/ end bar timestamp/ohlcv
            data = []
            data.append(int(elem[0] / 1000))
            data.append(int(elem[6] / 1000))
            data.append(eval(elem[1]))
            data.append(eval(elem[2]))
            data.append(eval(elem[3]))
            data.append(eval(elem[4]))
            data.append(eval(elem[5]))
            alldata.append(data)
        return alldata

    def append_data(self):
        #just append data if you already have some; but not the last points
        raise NotImplementedError

    def number_to_month(self, month):
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        return months[month-1]

    def get_ticksizes(self):
        if os.path.exists('binance_data/ticksizes'):
            with open ('binance_data/ticksizes', 'rb') as f:
                ticksizes = pickle.load(f)
            f.close()
            return ticksizes

        info = self.client.futures_exchange_info()
        assets = info['symbols']
        ticksizes = {}
        for elem in assets:
            sym = elem['symbol']
            filters = elem['filters'][0]['tickSize']
            ticksizes.update({sym : eval(filters)})
        with open('binance_data/ticksizes', 'wb') as f:
            ticksizes = pickle.dump(ticksizes, f)
        f.close()
        return ticksizes


class Bybit_Downloader():

    def __init__(self):
        api_key = settings.bybit_public_key
        api_secret = settings.bybit_secret_key
        assert len(api_key) != 0, 'you need to provide a valid api key for binance'
        assert len(api_secret) != 0, 'you need to provide a valid api key for binance'
        self.client = HTTP(
            endpoint="https://api.bybit.com",
            api_key=api_key,
            api_secret=api_secret)
        # todo probably doenst require auth ; check API docs

    def download(self, symbol, fundamental_period, start, end=None, save_file=True, print_info = True):
        '''Downloads the data
        :param symbol: str, eg. 'BTCUSDT'
        :param fundamental_period: int; candle size in minutes
        :param start, end : datetime objects in utc timezone; end is optional; if end is none, end = now()
        :param savefile: optional
        '''

        if not os.path.exists('bybit_data/'):
            os.makedirs('bybit_data/')
        if not os.path.exists('bybit_data/downloads/'):
            os.makedirs('bybit_data/downloads')

        start_time = int(start.timestamp())
        if end is None:
            end_time = int(time.time())
        else:
            end_time = int(end.timestamp())

        #first check that data exists at the start timestamp:
        success = True
        try:
            ans = self.client.query_mark_price_kline(symbol=symbol,
                                                     interval=fundamental_period,
                                                     from_time=str(start_time)
                                                     )
            data = ans['result']
            first_available_timestamp = data[0]['start_at']
            if first_available_timestamp > start_time:
                print('data not available on Bybit before ', datetime.fromtimestamp(first_available_timestamp))
                start_time = first_available_timestamp
        except Exception as e:
            success = False

        if not success:  # try again
            time.sleep(10)
            self.download(symbol, fundamental_period, start, end, save_file, print_info)
        assert end_time > start_time, 'wrong start end dates provided in download'

        # api returns batches of 200 candles only, need to iterate over
        success = True
        time_list = []
        i = 0
        while start_time + i * 200 * fundamental_period * 60 < end_time:
            # next batch of 200 candles
            time_list.append(start_time + i * 200 * fundamental_period * 60)
            i += 1

        # print('downloading', len(time_list), 'files of 200 candle each')
        # same format as binance timestamp start/ts end/ohlcv
        opening_ts = []
        closing_ts = []
        o = []
        h = []
        l = []
        c = []
        v = []

        for start_time in time_list:
            # start time must be a string ; this downloads the markprice of usdt futures
            # changes required if you want inverse perpetuals or spot data
            try:
                ans = self.client.query_mark_price_kline(symbol=symbol,
                                                         interval=fundamental_period,
                                                         from_time=str(start_time)
                                                         )
                data = ans['result']
                print(data[0]['start_at'])
                print(datetime.fromtimestamp(data[0]['start_at']))
            except Exception as e:
                success = False
                print(e)

            if not success:  # try again
                time.sleep(10)
                self.download(symbol, fundamental_period, start, end, save_file, print_info)

            # last query may return an empty list #todo unclear why/when
            if len(data) == 0:
                continue

            # make sure it works properly :
            check = False
            if check:
                print(data)
                # answer of the form : list of {'symbol': 'BTCUSDT', 'period': '5', 'start_at': 1673857200, 'open': 20829.73, 'high': 20860.42, 'low': 20778.82, 'close': 20791}
                from_ts = data[0]['start_at']
                to_ts = data[-1]['start_at']
                print(datetime.fromtimestamp(from_ts))
                print(datetime.fromtimestamp(to_ts))

            for k in range(len(data)):  # 200
                candle = data[k]
                #the last batch may be after the end timestamp so,
                if candle['start_at'] > end_time:
                    break
                opening_ts.append(candle['start_at'])
                closing_ts.append(candle['start_at'] + fundamental_period * 60 - 1)  # add end ts of the candle
                o.append(candle['open'])
                h.append(candle['high'])
                l.append(candle['low'])
                c.append(candle['close'])
                # usdt future mark price doesn't return any volume information, try spot
                v.append(0)

            # avoid api rate limit
            time.sleep(0.05)

        # export to numpy
        X = np.transpose(np.array([opening_ts, closing_ts, o, h, l, c, v]))
        fp = 'bybit_data/downloads/' + symbol + str(fundamental_period) + '.npy'
        np.save(fp, X)
        cut_midnight(fp, fundamental_period, print_info)
    def append_data(self):
        # just append data if you already have some; but not the last points
        raise NotImplementedError

    def get_ticksizes(self):
        if os.path.exists('bybit_data/ticksizes'):
            with open('bybit_data/ticksizes', 'rb') as f:
                ticksizes = pickle.load(f)
            f.close()
            return ticksizes

        ans = self.client.query_symbol()['result']
        ticksizes = {}
        for elem in ans:
            ticksize = elem['price_filter']['tick_size']
            ticksizes.update({elem['name']: eval(ticksize)})
        with open('bybit_data/ticksizes', 'wb') as f:
            ticksizes = pickle.dump(ticksizes, f)
        f.close()
        return ticksizes



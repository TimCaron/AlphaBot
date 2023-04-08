import csv
import os
import multiprocessing as mp
import numpy as np
import settings as settings
import config
from binance.client import Client
from pybit.usdt_perpetual import HTTP
from datetime import datetime, timezone
import time


def cut_midnight(symbol, filepath, fundamental_period, print_info):
    '''Helper. Loads the data at filepath, and overwrite it with cut data that that starts at the closest timestamps == midnight'''
    if fundamental_period >= 1440:
        # not needed since it will already start at midnight
        return

    # see below, won't work either for non-multiple of 60 if period > 60
    if fundamental_period >60: assert fundamental_period%60 ==0, 'candle size must be multiple of one hour when they are greater than one hour'

    # data shall start at midnight
    data = np.load(filepath)
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
        #saving data to same filepath (overwrite)
        np.save(filepath, data)

    if print_info:
        data = np.load(filepath)
        print(symbol, 'data starts/ends at', datetime.fromtimestamp(data[0, 0], tz=timezone.utc),
              datetime.fromtimestamp(data[-1, 0], tz=timezone.utc))



class Binance_Downloader():
    def __init__(self):
        api_key = settings.binance_public_key
        api_secret = settings.binance_secret_key
        assert len(api_key) !=0, 'you need to provide a valid api key for binance'
        assert len(api_secret) !=0, 'you need to provide a valid api key for binance'
        self.client = Client(api_key, api_secret)
        # todo probably doesnt require auth ; check API docs ; anyway

    def download(self, symbol, fundamental_period, start, end = None, print_info = True):
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

        filepath_npy = 'binance_data/downloads/' + symbol + str(fundamental_period) + '.npy'

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
        print('download done')
        first_available_timestamp = alldata[0][0] #unix timestamp
        requested_start = start.timestamp()
        if first_available_timestamp > requested_start:
            print('WARNING: data not available on Binance before ', datetime.fromtimestamp(first_available_timestamp),
                  ' for symbol ', symbol)

        with open(filepath_csv, 'w') as myfile:
            csvwriter = csv.writer(myfile)
            for row in alldata:
                # append only valid points / since this might happen:
                if datetime.fromtimestamp(row[0]).minute%fundamental_period !=0 and datetime.fromtimestamp(row[0]).second !=0:
                    pass
                else:
                    csvwriter.writerow(row)

        myfile.close()
        d = np.loadtxt(filepath_csv, delimiter=',')
        np.save(filepath_npy, d)
        cut_midnight(symbol, filepath_npy, fundamental_period, print_info)
        os.remove(filepath_csv)

    def from_klines_to_list(self, klines):
        alldata = []
        for elem in klines:
            #format open timestamp/ end bar timestamp/ohlcv ; in milliseconds so need to /1000
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

    def download(self, symbol, fundamental_period, start, end=None, print_info = True):
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
            if fundamental_period == 1440:
                interval = 'D'  # special Bybit api encoding
            else: interval = fundamental_period
            ans = self.client.query_mark_price_kline(symbol=symbol,
                                                     interval=interval,
                                                     from_time=str(start_time)
                                                     )
            data = ans['result']
            first_available_timestamp = data[0]['start_at']
            if first_available_timestamp > start_time:
                print('WARNING: data not available on Bybit before ', datetime.fromtimestamp(first_available_timestamp), ' for symbol ', symbol)
                start_time = first_available_timestamp

        except Exception as e:
            print('exception', e)
            print('Note that allowed intervals are', '1 3 5 15 30 60 120 240 360 720 D M W')
            success = False

        if not success:  # try again
            time.sleep(10)
            self.download(symbol, fundamental_period, start, end, print_info)
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
                if fundamental_period == 1440:
                    interval = 'D'  # special Bybit api encoding
                else:
                    interval = fundamental_period
                ans = self.client.query_mark_price_kline(symbol=symbol,
                                                         interval=interval,
                                                         from_time=str(start_time)
                                                         )
                data = ans['result']
                print(data[0]['start_at'])
                print(datetime.fromtimestamp(data[0]['start_at']))
            except Exception as e:
                success = False
                print(e)

            if not success:  # try again
                print('something went wrong, retrying download in 10secs')
                time.sleep(10)
                self.download(symbol, fundamental_period, start, end, print_info)

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
                # usdt future mark price doesn't return any volume information, try spot ? if you need volume
                v.append(0)

            # avoid api rate limit
            time.sleep(0.05)

        # export to numpy
        X = np.transpose(np.array([opening_ts, closing_ts, o, h, l, c, v]))
        fp = 'bybit_data/downloads/' + symbol + str(fundamental_period) + '.npy'
        np.save(fp, X)
        cut_midnight(symbol, fp, fundamental_period, print_info)


    def append_data(self):
        # just append data if you already have some; but not the last points
        raise NotImplementedError


# wrapper function for task
def download_wrapper(args):
    return Binance_Downloader().download(*args)

def parralel_download_binance(symbol_list, fundamental_period, start, end):
    '''for binance only ; for bybit you dont want to hit the rate limit of the API'''
    tasks = []
    for symbol in symbol_list:
        tasks.append([symbol, fundamental_period, start, end])

    print(tasks)
    print('downloading ', len(tasks), ' symbols')
    mp_pool = mp.Pool(config.CPU)
    asyncResult = mp_pool.map_async(download_wrapper, tasks)
    asyncResult.get()
    mp_pool.close()
    mp_pool.join()

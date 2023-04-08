from datetime import datetime
import pytz
import numpy as np
from datetime import timezone
import config


class Experiment():
    def __init__(self):
        pass


    @staticmethod
    def get_experiment(name, exchange, symbol_list, freq, fundamental_period, init_dict, maxlen):

        if len(symbol_list) == 1:
            if name != symbol_list[0][:-4].lower():
                print('redefining name of the experiment as the symbol name')
                name = symbol_list[0][:-4].lower()
        else:
            # for each symbol like btcusdt we will create subexperiments for this symbol, called expebtc, hence,
            # when you have multiple symbols you need a different global name:
            invalid_names = [symbol[:-4].lower() for symbol in symbol_list]
            assert name not in invalid_names, 'invalid name for experiment with multiple symbol; be more creative'


        one_month_in_minutes = 30*24*60
        experiment = {
                     'name': name,
                     'fundamental_period': fundamental_period,
                     'exchange': exchange,

                     'download_start_date': config.default_download_start_date,  # Y, M, D, H, Minute
                     'download_end_date': datetime.now(),

                      # train/val/test split dates can be provided by hand;
                      # if not, default values will apply as defined in config file
                     'start_date': None, # some datetime object you need to define
                     'split_date_1': None,
                     'split_date_2': None,
                     'end_date': None,

                     'symbol_list': symbol_list,
                     'init_dict': init_dict,
                     'frequency': freq, #if you want to use e.g. data in 1h, but trade only every 6 hours, then freq = 6 when fundamental period is 60
                      'offsets': [i for i in range(freq)], #default

                     'max_len': maxlen, #max past history used for eg. smas or momentums
                     'drawdown_window': one_month_in_minutes//(freq*fundamental_period),

                     'order_type': 'limit',
                     'order_parameters':
                        {
                        'fee_limit': 0.01/100, #bybit values for futures
                        'fee_market': 0.06/100, #bybit values for futures
                        'leverage_long': 1,
                        'leverage_short': 1,
                         }
                  }

        #run varuious tests :

        if experiment['start_date'] is None and experiment['start_date'] is None and experiment['start_date'] is None and experiment['start_date'] is None:
            pass

        elif experiment['start_date'] is not None and experiment['split_date_1'] is not None and experiment['split_date_2'] is not None and experiment['end_date'] is not None:
            to = experiment['start_date']
            t1 = experiment['split_date_1']
            t2 = experiment['split_date_2']
            t3 = experiment['end_date']
            assert to < t1, 'start date is not smaller than split_date_1'
            assert t1 < t2, 'split_date_1 is not smaller than split_date_2'
            assert t2 < t3, 'split_date_2 is not smaller than end_date'

        else:
            print('invalid start/train/val/test in experiment; it should be all None, or all not None')
            raise ValueError

        assert freq == int(freq), 'freq must be an integer'
        assert freq >= 1, 'freq must be greater than 1'

        # todo there may be more than that :
        allowed_intervals = [1, 5, 15, 30, 60, 120, 180, 240, 360, 480, 720, 1440]
        # (PS : you can also create your own with the correct choice of freq and period; eg. 90 min is period 30 and freq 3)
        assert fundamental_period in allowed_intervals, 'check API enables that candle size ; maybe it does...'

        assert init_dict is not None, 'init dict cant be None'
        assert exchange in ['binance', 'bybit'], 'only binance and bybit supported for the moment'
        assert len(experiment['offsets']) <= freq, 'there cant be more ofssets than the freq'
        assert max(experiment['offsets']) <= freq - 1, 'max ofsset can not exceed freq-1'

        return experiment





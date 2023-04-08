import random

import numpy as np
from Data_Cleaner import Data_Cleaner
from Data_Downloader import Binance_Downloader, Bybit_Downloader, parralel_download_binance
from Class_Features import Precompute_Features
import os
import glob
import pickle
from datetime import timezone
from datetime import datetime
from time import sleep
import config


class Data():

    def __init__(self, experiment):
        self.experiment = experiment
        self.symbol_list = experiment['symbol_list']
        self.num_symbols = len(self.symbol_list)
        self.fundamental_period = experiment['fundamental_period']
        self.exchange = experiment['exchange']
        self.directory = experiment['exchange'] + '_data/'
        self.expename = 'expe' + self.experiment['name'] + '/'
        self.name = experiment['name']
        self.frequency = experiment['frequency']
        self.offsets = experiment['offsets']
        self.maxlen = experiment['max_len']
        self.download_start_date, self.download_end_date = experiment['download_start_date'], experiment['download_end_date']
        self.expe_start_date = experiment['start_date']
        self.expe_split_1 = experiment['split_date_1']
        self.expe_split_2 = experiment['split_date_2']
        self.expe_end_date = experiment['end_date']
        # default allways check directories are created
        self.init_directories()

    def init_directories(self):
        if not os.path.exists('GP/pools/'):
            os.makedirs('GP/pools/')

        alldirs = ['mas/', 'moms/', 'maxs/', 'mins/', 'alphabeta/', 'localpha/',
                   'concats/', 'optim_orders/', 'pasthighs/', 'pastlows/', 'pasttotaldelta/',
                   'alternateallocations/', 'allocations/', 'bestallocations/', 'randomallocations/']

        for symbol in self.symbol_list:
            name = self.symbol_to_name(symbol)
            one_symbol_name = 'expe' + name + '/'
            if not os.path.exists(self.directory + one_symbol_name):
                os.makedirs(self.directory + one_symbol_name)

            for dirs in alldirs:
                fp = self.directory + one_symbol_name + dirs
                if not os.path.exists(fp):
                    os.makedirs(fp)

        #if expe has multiple symbols, create the target destination folder
        if self.num_symbols >1:
            if not os.path.exists(self.directory + self.expename):
                os.makedirs(self.directory + self.expename)
            for dirs in alldirs:
                fp = self.directory + self.expename + dirs
                if not os.path.exists(fp):
                    os.makedirs(fp)

    def delete_downloaded_data(self):
        print('deleting downloaded data')
        fp1 = [self.directory + 'downloads/' + self.symbol_list[i] + str(self.fundamental_period) + '.npy' for i in
               range(len(self.symbol_list))]
        fp2 = [self.directory + 'downloads/' + self.symbol_list[i] + str(self.fundamental_period) + '.csv' for i in
               range(len(self.symbol_list))]
        for f in fp1:
            if os.path.exists(f):
                os.remove(f)
        for f in fp2:
            if os.path.exists(f):
                os.remove(f)

    def delete_expe_data(self):
        print('deleting expe data')
        # delete expe data by symbol
        for symbol in self.symbol_list:
            name = self.symbol_to_name(symbol)
            one_symbol_name = 'expe' + name + '/'
            fp = self.directory + one_symbol_name + '*.*'
            files = glob.glob(fp)
            for f in files:
                os.remove(f)

    def delete_aggregated_data(self):
        print('deleting aggregated data')
        # delete aggregated data by symbol
        for symbol in self.symbol_list:
            name = self.symbol_to_name(symbol)
            one_symbol_name = 'expe' + name + '/'
            fp = self.directory + one_symbol_name + 'concats/*'
            files = glob.glob(fp)
            for f in files:
                os.remove(f)
        if self.num_symbols >1:
            fp = self.directory + self.expename + 'concats/*'
            files = glob.glob(fp)
            for f in files:
                os.remove(f)

    def delete_feature(self, feature):
        print('deleting feature ', feature)
        fp = self.directory + self.expename + feature + '/*'
        files = glob.glob(fp)
        for f in files:
            os.remove(f)

    def delete_allocations(self):
        print('deleting allocations ')
        #todo avoir
        for x in ['', 'best', 'random', 'alternate', 'prod']:
            fp = self.directory + x + 'allocations/' + '*.*'
            files = glob.glob(fp)
            for f in files:
                os.remove(f)

    def garbage_collector(self):
        init_dict = self.experiment['init_dict']
        if init_dict['delete_downloaded_data']:
            self.delete_downloaded_data()
        if init_dict['delete_expe_data']:
            self.delete_expe_data()
        if init_dict['delete_ma_moms']:
            for feature in ['mas', 'moms']:
                self.delete_feature(feature)
        if init_dict['delete_min_maxs']:
            for feature in ['mins', 'maxs']:
                self.delete_feature(feature)
        if init_dict['delete_pasthighslows']:
            for feature in ['pasthighs', 'pastlows', 'pasttotaldelta']:
                self.delete_feature(feature)
        if init_dict['delete_precomputed_deltas']:
            for feature in ['alphabeta', 'localpha']:
                self.delete_feature(feature)
        if init_dict['delete_aggregated_data']:
            self.delete_aggregated_data()
        if init_dict['delete_allocations']:
            self.delete_allocations()

    def symbol_to_name(self, symbol):
        return symbol[:-4].lower()

    def download_data_for_experiment(self):
        to_download = []
        for symbol in self.symbol_list:
            path = self.exchange + '_data/downloads/' + symbol + str(self.fundamental_period) + '.npy'
            if os.path.exists(path):
                pass
            else:
                to_download.append(symbol)

        if len(to_download):
            if self.exchange == 'binance':
                parralel_download_binance(to_download, self.fundamental_period, self.download_start_date,
                                          self.download_end_date)
            else:
                for symbol in to_download:
                    Bybit_Downloader().download(symbol, self.fundamental_period, self.download_start_date,
                                                self.download_end_date)

    def clean_data_for_experiment(self):
        cleaner = Data_Cleaner(self.experiment)
        cleaner.execute()

    def save_data_for_experiment(self):
        '''experiment start/end date may be different than download date ;
        in which case we cut the data and save it to eg. binance_data/expebtc/BTCUSDT60.npy
        Also, if you run a multisymbol experiment, you may have pbs if one data starts later than the other ones : in which case
        we cut the data at the common starting point
        '''
        if self.expe_start_date is None and self.expe_split_1 is None and self.expe_split_2 is None and self.expe_end_date is None :
            # then autotune it to : expe start = first data point, end = last, and make sure they all start at the same point is critical:
            # also, train/val/test split will be applied by default when possible
            starts = []
            ends = []
            for symbol in self.symbol_list:
                data = np.load(self.exchange + '_data/downloads/' + symbol + str(self.fundamental_period) + '.npy')
                starts.append(data[0,0]) #unix timestamps
                ends.append(data[-1,0])
            start = max(starts)
            end = min(ends)

            if not start < config.default_train_end.timestamp():
                print('not enough historical data for this experiment; please set by hand an appropriate train/val/test split in Experiments.py')
                print('common start timestamp is', datetime.fromtimestamp(start, tz=timezone.utc))
                print('while you want to set end of training set at', config.default_train_end)
                print('more details:')
                for k, symbol in enumerate(self.symbol_list):
                    print(symbol, 'data starts at ', datetime.fromtimestamp(starts[k], tz= timezone.utc))
                raise ValueError
            if not config.default_val_end.timestamp() < end:
                print('not enough historical data for this experiment; set by hand an appropriate train/val/test split in Experiment.py')
                print('common ending timestamp is', datetime.fromtimestamp(end, tz=timezone.utc))
                print('while you set end of validation set at', config.default_val_end)
                raise ValueError

            warning = False
            for s in starts:
                if s != start:
                    warning = True
            for e in ends:
                if e != end:
                    warning = True
            if warning:
                print('WARNING, detected some symbols that do not start or end at the same time')
                print('WARNING data for this experiment will start at ', datetime.fromtimestamp(start, tz=timezone.utc))
                print('WARNING data for this experiment will end at ', datetime.fromtimestamp(end, tz=timezone.utc))

            self.experiment['start_date'] = datetime.fromtimestamp(start, tz=timezone.utc)
            self.experiment['split_date_1'] = config.default_train_end
            self.experiment['split_date_2'] = config.default_val_end
            self.experiment['end_date'] = datetime.fromtimestamp(end, tz=timezone.utc)
            self.expe_start_date = self.experiment['start_date']
            self.expe_split_1 = self.experiment['split_date_1']
            self.expe_split_2 = self.experiment['split_date_2']
            self.expe_end_date = self.experiment['end_date']

        elif self.expe_start_date is not None and self.expe_split_1 is not None and self.expe_split_2 is not None and self.expe_end_date is not None :
            starts = []
            ends = []
            for symbol in self.symbol_list:
                data = np.load(self.exchange + '_data/downloads/' + symbol + str(self.fundamental_period) + '.npy')
                starts.append(data[0, 0])  # unix timestamps
                ends.append(data[-1, 0])
            start = max(starts)
            end = min(ends)

            if not start <= self.expe_start_date.timestamp():
                print('Warning the expe data start date provided is invalid since there is no data available at that time')
                print('common start timestamp is', datetime.fromtimestamp(start, tz=timezone.utc))
                print('while you want to set expe_start_date at', datetime.fromtimestamp(self.expe_start_date, tz=timezone.utc))
                raise ValueError
            if not end >= self.expe_end_date.timestamp():
                print('Warning the expe data start date provided is invalid since there is no data available at that time')
                print('common end timestamp is', datetime.fromtimestamp(start, tz=timezone.utc))
                print('while you want to set expe_end_date at', datetime.fromtimestamp(self.expe_end_date, tz=timezone.utc))
                raise ValueError

        #now cut and save data to eg. binance_data/expebtc/file.npy
        for symbol in self.symbol_list:
            name = self.symbol_to_name(symbol)
            one_symbol_name = 'expe' + name + '/'
            sourcepath = self.directory + 'downloads/' + symbol + str(self.fundamental_period) + '.npy'
            targetpath = self.directory + one_symbol_name + symbol + str(self.fundamental_period) + '.npy'
            if os.path.exists(targetpath):
                print('warning, not redefining data for experiment')
                pass
            else:
                source_data = np.load(sourcepath)
                if source_data[0, 0] <= self.expe_start_date.timestamp():
                    idx_start = np.where(source_data[:, 0] == self.expe_start_date.timestamp())[0][0]

                else:
                    print('Warning the expe data start date provided is invalid since there is no data available at that time')
                    print(f'start timestamp for symbol {symbol} is', datetime.fromtimestamp(source_data[0,0], tz=timezone.utc))
                    print('but you set expe_start_date at', self.expe_start_date)
                    raise ValueError
                if source_data[-1, 0] >= self.expe_end_date.timestamp():
                    idx_end = np.where(source_data[:, 0] == self.expe_end_date.timestamp())[0][0]

                else:
                    print(
                        'Warning the expe data end date provided is invalid since there is no data available at that time')
                    print(f'end timestamp for symbol {symbol} is',
                          datetime.fromtimestamp(source_data[-1, 0], tz=timezone.utc))
                    print('but you set expe_end_date at', self.expe_start_date)
                    raise ValueError
                save_data = source_data[idx_start:idx_end, :]
                np.save(targetpath, save_data)

    def aggregate_by_freq_and_offset(self, data, targetpath, offset):
        '''aggregate data by freq and offset for each symbol independently'''
        d = np.copy(data)  # not sure if required
        d = d[offset:, :]  # recall indeed that data starts by default at midnight
        new_array = np.zeros((d.shape[0] // self.frequency, d.shape[1]))
        # concat
        for i in range(new_array.shape[0]):
            new_array[i, 0] = d[i * self.frequency, 0]
            new_array[i, 1] = d[(i + 1) * self.frequency - 1, 1]
            new_array[i, 2] = d[i * self.frequency, 2]
            new_array[i, 3] = np.max(d[i * self.frequency:(i + 1) * self.frequency, 3])
            new_array[i, 4] = np.min(d[i * self.frequency:(i + 1) * self.frequency, 4])
            new_array[i, 5] = d[(i + 1) * self.frequency - 1, 5]
            new_array[i, 6] = np.sum(d[i * self.frequency: (i + 1) * self.frequency, 6])
        np.save(targetpath, new_array)

    def load_data(self, symbol, offset):
        '''load expe data for a given symbol, at given freq and offset'''
        name = self.symbol_to_name(symbol)
        one_symbol_name = 'expe' + name + '/'
        path = self.directory + one_symbol_name + 'concats/' + symbol + str(self.fundamental_period) \
                     + '_' + str(self.frequency) + '_' + str(offset) + '.npy'
        return np.load(path)

    def concatenate_symbols(self):
        '''concatenate data in four files containing timestamps and opens of each symbol
        same for highs, lows, and closes. repeat for every offset requested.'''

        channels = ['opens', 'highs', 'lows', 'closes']

        for offset in self.offsets:
            opens = []
            highs = []
            lows = []
            closes = []

            for i, symbol in enumerate(self.symbol_list):
                data = self.load_data(symbol, offset)
                # this is a test ; shd not happen but just be sure; end test might happen if
                # expe end date is None and downloaded fil do not end at the same time :
                # in that case provide a expe end date; or re-download all a at once
                if i == 0:
                    L = data.shape[0]
                    starting_ts = data[0, 0]
                    ending_ts = data[-1, 0]
                    ts1 = data[:, 0]
                    ts2 = data[:, 1]
                else:
                    assert data[0, 0] == starting_ts, 'critical start value; concatenation of different symbol cant be done'
                    assert data[-1,0] == ending_ts, 'critical end value; concatenation of different symbol cant be done'
                    assert data.shape[0] == L, 'critical shape issue'

                opens.append(data[:, 2])
                highs.append(data[:, 3])
                lows.append(data[:, 4])
                closes.append(data[:, 5])

            # aggregate in a new array
            alldata = [opens, highs, lows, closes]
            for m, channel in enumerate(channels):
                # if i have only one symbol then self.expename is eg. 'btc', so its fine; else, its the expe name, so:
                # save data to eg 'concats/opens60_4_2.npy'
                targetpath = self.directory + self.expename + 'concats/' + channel + str(self.fundamental_period) \
                             + '_' + str(self.frequency) + '_' + str(offset) + '.npy'
                M = 2 + len(self.symbol_list)
                X = np.zeros((L, M))
                X[:, 0] = ts1
                X[:, 1] = ts2
                for k in range(len(self.symbol_list)):
                    X[:, 2 + k] = alldata[m][k]
                np.save(targetpath, X)

    def load_by_channel(self, channel, offset):
        assert channel in ['opens', 'highs', 'lows', 'closes'], 'error invalid channel name; probably spelling'
        sourcepath = self.directory + self.expename + 'concats/' + channel + str(self.fundamental_period) \
                     + '_' + str(self.frequency) + '_' + str(offset) + '.npy'
        return np.load(sourcepath)

    def precompute_mvg_avg_momentum(self):
        '''Features are computed on the opens values of each bar'''
        targetpath = self.directory + self.expename
        for offset in self.offsets:
            data = self.load_by_channel('opens', offset)
            Precompute_Features.precompute_ma_moms(targetpath, data,
                                          self.frequency, self.maxlen,
                                          self.fundamental_period,
                                          offset)

    def precompute_min_max(self):
        '''Features are computed on the opens values of each bar'''
        targetpath = self.directory + self.expename
        for offset in self.offsets:
            data = self.load_by_channel('opens', offset)
            Precompute_Features.precompute_min_max(targetpath, data,
                                          self.frequency, self.maxlen,
                                          self.fundamental_period,
                                          offset)

    def print_ts_from_data(self, text, d):
        print('')
        print(text, 'has shape', d.shape)
        print(text, 'is from', datetime.fromtimestamp(d[0, 0], tz=timezone.utc),
              datetime.fromtimestamp(d[0, 1], tz=timezone.utc))
        print(text, 'to', datetime.fromtimestamp(d[-1, 0], tz=timezone.utc),
              datetime.fromtimestamp(d[-1, 1], tz=timezone.utc),
              )

    def print_timestamps_infos(self):
        one_offset = random.choice(self.offsets)

        for j, symbol in enumerate(self.symbol_list):
            download_path = self.directory + 'downloads/'+ symbol + str(self.fundamental_period) + '.npy'
            d = np.load(download_path)
            self.print_ts_from_data('downloaded data of symbol ' + symbol, d)

            name = self.symbol_to_name(symbol)
            one_symbol_name = 'expe' + name + '/'
            experiment_path = self.directory + one_symbol_name + symbol + str(self.fundamental_period) + '.npy'
            d = np.load(experiment_path)
            if j==0:
                start, end = d[0, 0], d[-1, 0]
            else:
                assert d[0,0] == start, 'critical error in multisymbol expe; data do not start at the same timestamp'
                assert d[-1,0] == end, 'critical error in multisymbol expe; data do not end at the same timestamp'

            self.print_ts_from_data('experiment data for symbol ' + symbol, d)
            concat_path = self.directory + one_symbol_name + 'concats/' + symbol + str(
            self.fundamental_period) + '_' + str(self.frequency) + '_' + str(one_offset) + '.npy'
            d = np.load(concat_path)
            self.print_ts_from_data(f'concatenate data of symbol {symbol} for offset {one_offset}', d)

        if self.num_symbols > 1:
            experiment_path = self.directory \
                              + self.expename + 'opens' + str(self.fundamental_period) \
                              + '_' + str(self.frequency) + '_' + str(one_offset) + '.npy'
            d = np.load(experiment_path)
            self.print_ts_from_data('opens data for all symbols', d)

    def default_data_initializer(self):
        '''Main default data initializer.'''

        self.garbage_collector()
        print('init data : downloads...')
        self.download_data_for_experiment()

        print('init data : cleaning data...')
        if self.experiment['init_dict']['clean_downloaded_data']:
            cleaner = Data_Cleaner(self.experiment)
            cleaner.execute()
        else:
            print('**** WARNING **** : data may not be cleaned')
            sleep(0.2)
            pass

        # save relevant data for this experiment
        self.save_data_for_experiment()

        # precompute and save aggregated data by freq and offsets
        for symbol in self.symbol_list:
            name = self.symbol_to_name(symbol)
            one_symbol_name = 'expe' + name + '/'
            sourcepath = self.directory + one_symbol_name + symbol + str(self.fundamental_period) + '.npy'
            data = np.load(sourcepath)

            for offset in self.offsets:
                targetpath = self.directory + one_symbol_name + 'concats/' + symbol + str(self.fundamental_period) \
                             + '_' + str(self.frequency) + '_' + str(offset) + '.npy'
                if os.path.exists(targetpath):
                    pass
                else:
                    self.aggregate_by_freq_and_offset(data, targetpath, offset)

        # concatenate symbols/compute files of opens, highs, lows, closes
        self.concatenate_symbols()

        # default data initializer will precompute the features
        if self.experiment['init_dict']['ma_mom_required']:
            self.precompute_mvg_avg_momentum()

        if self.experiment['init_dict']['min_max_required']:
            self.precompute_min_max()

        if self.experiment['init_dict']['print_timestamps_and_shape_infos']:
            self.print_timestamps_infos()



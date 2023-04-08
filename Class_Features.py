import os
import config
import multiprocessing as mp
import numpy as np

class Precompute_Features():
    def __init__(self):
        pass

    def mas_precompute(self, task):
        data, timestamps_open, timestamps_close, ma_length, path = task
        filt = np.ones(ma_length) / ma_length
        filt = np.concatenate([np.zeros(ma_length - 1), filt])
        mov = np.apply_along_axis(lambda m: np.convolve(m, filt, mode='same'), axis=0, arr=data)
        if data.shape[1] == 1:
            mov = mov.reshape(mov.size, 1)
        ts1 = timestamps_open.reshape(timestamps_open.size, 1)
        ts2 = timestamps_close.reshape(timestamps_close.size, 1)
        X = np.hstack((ts1, ts2, mov))
        np.save(path, X)

    def mom_precompute(self, task):
        data, timestamps_open, timestamps_close, mom_length, path = task
        mom = np.zeros_like(data)
        for i in range(mom.shape[0]):
            if i - mom_length >= 0:
                beg = i - mom_length
            else:
                beg = 0
            mom[i] = np.divide(data[i] - data[beg], data[beg])
        if data.shape[1] == 1:
            mom = mom.reshape(mom.size, 1)
        ts1 = timestamps_open.reshape(timestamps_open.size, 1)
        ts2 = timestamps_close.reshape(timestamps_close.size, 1)
        X = np.hstack((ts1, ts2, mom))
        np.save(path, X)





    def maxs_precompute(self, task):
        data, timestamps_open, timestamps_close, max_length, path = task
        array_of_past_maximums = np.empty((data.shape[0], data.shape[1]))
        #todo there may be a way to vectorize this and improves speed
        for i in range(data.shape[0]):
            for k in range(data.shape[1]):
                if i < max_length:
                    pastdata = data[0:i + 1, k]
                else:
                    pastdata = data[i + 1 - max_length:i + 1, k]

                local_past_max = np.max(pastdata)
                current_price = data[i, k]
                deltamax = (local_past_max - current_price) / current_price  # positive definite
                array_of_past_maximums[i, k] = deltamax
        ts1 = timestamps_open.reshape(timestamps_open.size, 1)
        ts2 = timestamps_close.reshape(timestamps_close.size, 1)
        X = np.hstack((ts1, ts2, array_of_past_maximums))

        np.save(path, X)

    def mins_precompute(self, task):
        data, timestamps_open, timestamps_close, min_length, path = task
        array_of_past_minimums = np.empty((data.shape[0], data.shape[1]))
        for i in range(data.shape[0]):
            for k in range(data.shape[1]):
                if i < min_length:
                    pastdata = data[0:i + 1, k]
                else:
                    pastdata = data[i + 1 - min_length:i + 1, k]
                past_min = np.min(pastdata)
                current_price = data[i, k]
                deltamin = (past_min - current_price) / current_price  # negative definite
                array_of_past_minimums[i] = deltamin

        ts1 = timestamps_open.reshape(timestamps_open.size, 1)
        ts2 = timestamps_close.reshape(timestamps_close.size, 1)
        X = np.hstack((ts1, ts2, array_of_past_minimums))
        np.save(path, X)

    @staticmethod
    def precompute_ma_moms(targetpath, data, freq, maxlen, fundamental_period, offset):
        print('called once')
        '''compute missing mov avg files ; if you want to recompute them, you first need to delete it
        with apprpriate functions in Data Class'''
        # data has shape L, 2 + n : 2 timestamps and n symbol
        # targetpath shall be binance_data/myexpename


        required = []
        for ma_length in range(2, maxlen + 1):
            path = targetpath + '/mas/' + str(fundamental_period) \
                   + '_' + str(freq) + '_' + str(offset) + '_' + str(ma_length) + '.npy'
            if os.path.exists(path):
                pass
            else:
                required.append(ma_length)

        if len(required): #mas and momsare computed at opening bar:
            print('pre computing moving avg ...')
            data_opens = data[:, 2:]
            timestamps_open = data[:, 0]
            timestamps_close = data[:, 1]
            tasks = []
            for ma_length in required:
                path = targetpath + '/mas/' + str(fundamental_period) \
                       + '_' + str(freq) + '_' + str(offset) + '_' + str(ma_length) + '.npy'
                tasks.append([data_opens, timestamps_open, timestamps_close, ma_length, path])

            myinstance = Precompute_Features()
            mp_pool = mp.Pool(config.CPU)
            asyncResult = mp_pool.map_async(myinstance.mas_precompute, tasks)
            results = asyncResult.get()
            mp_pool.close()
            mp_pool.join()

        required = []
        for mom_length in range(1, maxlen + 1):
            path = targetpath + '/moms/' + str(fundamental_period) \
                   + '_' + str(freq) + '_' + str(offset) + '_' + str(mom_length) + '.npy'
            if os.path.exists(path):
                pass
            else:
                required.append(mom_length)

        if len(required):
            print('pre computing momentums, ...')
            data_opens = data[:, 2:]
            timestamps_open = data[:, 0]
            timestamps_close = data[:, 1]
            tasks = []
            for my_mom in required:
                path = targetpath + '/moms/' + str(fundamental_period) \
                       + '_' + str(freq) + '_' + str(offset) + '_' + str(my_mom) + '.npy'
                tasks.append([data_opens, timestamps_open, timestamps_close, my_mom, path])

            myinstance = Precompute_Features()
            mp_pool = mp.Pool(config.CPU)
            asyncResult = mp_pool.map_async(myinstance.mom_precompute, tasks)
            results = asyncResult.get()
            mp_pool.close()
            mp_pool.join()


    #################################################
    @staticmethod
    def precompute_min_max(targetpath, data, freq, maxlen, fundamental_period, offset):
        required = []
        for min_length in range(1, maxlen + 1):
            path = targetpath + '/mins/' + str(fundamental_period) \
                   + '_' + str(freq) + '_' + str(offset) + '_' + str(min_length) + '.npy'
            if os.path.exists(path):
                pass
            else:
                required.append(min_length)

        if len(required):
            print('pre computing mins, ...')
            data_opens = data[:, 2:]
            timestamps_open = data[:, 0]
            timestamps_close = data[:, 1]
            tasks = []
            for min_length in required:
                path = targetpath + '/mins/' + str(fundamental_period) \
                       + '_' + str(freq) + '_' + str(offset) + '_' + str(min_length) + '.npy'
                tasks.append([data_opens, timestamps_open, timestamps_close, min_length, path])

            myinstance = Precompute_Features()
            mp_pool = mp.Pool(config.CPU)
            asyncResult = mp_pool.map_async(myinstance.mins_precompute, tasks)
            results = asyncResult.get()
            mp_pool.close()
            mp_pool.join()

        required = []
        for length in range(1, maxlen + 1):
            path = targetpath + '/maxs/' + str(fundamental_period) \
                   + '_' + str(freq) + '_' + str(offset) + '_' + str(length) + '.npy'
            if os.path.exists(path):
                pass
            else:
                required.append(length)

        if len(required):
            print('pre computing maxs ...')
            data_opens = data[:, 2:]
            timestamps_open = data[:, 0]
            timestamps_close = data[:, 1]
            tasks = []
            for min_length in required:
                path = targetpath + '/maxs/' + str(fundamental_period) \
                       + '_' + str(freq) + '_' + str(offset) + '_' + str(min_length) + '.npy'
                tasks.append([data_opens, timestamps_open, timestamps_close, min_length, path])

            myinstance = Precompute_Features()
            mp_pool = mp.Pool(config.CPU)
            asyncResult = mp_pool.map_async(myinstance.maxs_precompute, tasks)
            results = asyncResult.get()
            mp_pool.close()
            mp_pool.join()

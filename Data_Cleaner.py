import numpy as np
from datetime import datetime, timezone

def affine_completion(starting, ending, size, delta_t):
    '''performs affine completion between starting and ending timestamps
    for open and closes'''
    # todo : now it completes volume with zeros, and highs/lows are also afffine; wd be better to somehow
    # generate realistic high/lows based on apst observations

    missing_steps = (ending[0] - starting[0]) / delta_t
    slope = (ending[5] - starting[5]) / missing_steps
    missing_steps = int(missing_steps)
    append = np.zeros((1, size))

    for i in range(missing_steps - 1):
        newdata = np.array(
            [starting[0] + (i + 1) * delta_t,
             starting[1] + (i + 1) * delta_t,
             starting[2] + (i + 1) * slope,
             starting[3] + (i + 1) * slope,
             starting[4] + (i + 1) * slope,
             starting[5] + (i + 1) * slope, 0
             ]).reshape(1, size)

        append = np.vstack((append, newdata))

    append_me = np.delete(append, 0, 0)
    final_return = np.vstack((append_me, ending))
    return final_return

def complete_data(data, delta_t):
    '''clearly need improvements ; not the best choices of implementation made here, but it works'''
    size = data.shape[1]
    opening_timestamp = []
    closing_timestamp = []
    o = []
    h = []
    l = []
    c = []
    v = []

    # init
    opening_timestamp.append(data[0, 0])
    closing_timestamp.append(data[0, 1])
    o.append(data[0, 2])
    h.append(data[0, 3])
    l.append(data[0, 4])
    c.append(data[0, 5])
    v.append(data[0, 6])
    tot_ok = 0
    call_completion = 0

    for i in range(data.shape[0] - 1):
        last_ts_saved = opening_timestamp[-1]
        next_ts = data[i + 1, 0]
        if int(next_ts - last_ts_saved) == delta_t:
            opening_timestamp.append(data[i + 1, 0])
            closing_timestamp.append(data[i + 1, 1])
            o.append(data[i + 1, 2])
            h.append(data[i + 1, 3])
            l.append(data[i + 1, 4])
            c.append(data[i + 1, 5])
            v.append(data[i+1, 6])
            tot_ok += 1
        else:
            print('completing data from', i, datetime.fromtimestamp(data[i, 0]), 'to', datetime.fromtimestamp(data[i + 1, 0]))
            to_append = affine_completion(data[i, :], data[i + 1, :], size, delta_t)
            opening_timestamp = opening_timestamp + list(to_append[:, 0])
            closing_timestamp = closing_timestamp + list(to_append[:, 1])
            o = o + list(to_append[:, 2])
            h = h + list(to_append[:, 3])
            l = l + list(to_append[:, 4])
            c = c + list(to_append[:, 5])
            v = v + list(to_append[:, 6])
            call_completion += 1
    new_data = np.transpose(np.array([opening_timestamp, closing_timestamp, o, h, l, c, v]))
    return new_data

class Data_Cleaner():
    def __init__(self, experiment):
        self.exchange = experiment['exchange']
        self.symbol_list = experiment['symbol_list']
        self.fundamental_period = experiment['fundamental_period']
        self.directory = self.exchange + '_data/' + 'downloads/'
        self.delta_t = int(60 * self.fundamental_period)

    def final_ts_check(self):
        ok = True
        for i in range(self.dat.shape[0] - 1):
            if self.dat[i+1, 0] - self.dat[i, 0] != self.delta_t:
                ok = False
        if not ok:
            print('something went wrong, check')
            raise ValueError

    def execute(self):
        for symbol in self.symbol_list:
            self.fp = self.directory + symbol + str(self.fundamental_period)+'.npy'
            self.dat = np.load(self.fp)

            #there may be non_increasing timestamps !
            self.remove_non_increasing_timestamps()

            #if holes in data - call affine completion :
            self.complete_data()

            #enforce that previous close == next open (? not sure about that one, it shd be true, but its not on the data)
            self.close_correction()
            #if you do, then you have sometimes high that is not the high, and same for low
            self.high_low_correction()

            #safety
            self.final_ts_check()
            np.save(self.fp, self.dat)

    def remove_non_increasing_timestamps(self):
        # init
        opts = [self.dat[0, 0]]
        clts = [self.dat[0, 1]]
        o, h, l, c = [self.dat[0, 2]], [self.dat[0, 3]], [self.dat[0, 4]], [self.dat[0, 5]]
        v = [self.dat[0, 6]]

        for i in range(1, self.dat.shape[0]):
            if self.dat[i, 0] != self.dat[i - 1, 0] + self.delta_t:
                pass
            else:
                opts.append(self.dat[i, 0])
                clts.append(self.dat[i, 1])
                o.append(self.dat[i, 2])
                h.append(self.dat[i, 3])
                l.append(self.dat[i, 4])
                c.append(self.dat[i, 5])
                v.append(self.dat[i, 6])

        self.dat = np.transpose(np.array([opts, clts, o, h, l, c, v]))

    def complete_data(self):
        ok = True
        times = self.dat[:, 0]
        for i in range(self.dat.shape[0] - 1):
            if times[i + 1] - times[i] != self.delta_t:
                ok = False
                break
        if not ok:
            print('missing data, at time', datetime.fromtimestamp(times[i], tz=timezone.utc))
            self.dat = complete_data(self.dat, self.delta_t)

    def high_low_correction(self):
        L = self.dat.shape[0]
        for i in range(L):
            if self.dat[i, 3] < max(self.dat[i, 2], self.dat[i, 4], self.dat[i, 5]):  # high is not the high!
                self.dat[i, 3] = max(self.dat[i, 2], self.dat[i, 4], self.dat[i, 5])
            if self.dat[i, 4] > min(self.dat[i, 2], self.dat[i, 3], self.dat[i, 5]):  # low is not the low!
                self.dat[i, 4] = min(self.dat[i, 2], self.dat[i, 3], self.dat[i, 5])

    def close_correction(self):
        L = self.dat.shape[0]
        for i in range(L - 1):
            self.dat[i, 5] = self.dat[i+1, 2] #close at time i matched to open at time i+1

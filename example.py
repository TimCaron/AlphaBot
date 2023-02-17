from Data_Downloader import Binance_Downloader, Bybit_Downloader
from datetime import datetime
import pytz

start = datetime(2022, 12, 1, 0, 0, tzinfo=pytz.utc) # Y, M, D
end = datetime(2023, 2, 1, 0, 0, tzinfo=pytz.utc) # means the end is 23h... of 31 Jan == last day not included

#download Binance with until now ( end = None) in 2h candles
Binance_Downloader().download('BTCUSDT', 120, start)
# answers : data starts/ends at 2022-12-01 00:00:00+00:00 2023-02-16 22:00:00+00:00

#download Binance with start/end in one hour candles
Binance_Downloader().download('BTCUSDT', 60, start, end)
# answers : data starts/ends at 2022-12-01 00:00:00+00:00 2023-01-31 23:00:00+00:00

#download Bybit with until now ( end = None) in 15min candles
Bybit_Downloader().download('BTCUSDT', 15, start, end)
#answers : data starts/ends at 2022-12-01 00:00:00+00:00 2023-01-31 23:45:00+00:00


# now define an experiment:
experiment = {
    'name': 'btc',
    'fundamental_period': 60,
    'exchange': 'bybit',
    'download_start_date': datetime(2023, 1, 15, 0, 0, tzinfo=pytz.utc),  # Y, M, D, H, Minute
    'download_end_date': datetime.now(),
    'symbol_list': ['BTCUSDT', 'ETHUSDT'],
}

for symbol in experiment['symbol_list']:
    if experiment['exchange'] == 'binance':
        Binance_Downloader().download(symbol,
                                    experiment['fundamental_period'],
                                    experiment['download_start_date'],
                                    experiment['download_end_date']
                                    )
    elif experiment['exchange'] == 'bybit':
        Bybit_Downloader().download(symbol,
                                    experiment['fundamental_period'],
                                    experiment['download_start_date'],
                                    experiment['download_end_date']
                                    )
    else:
        raise NotImplementedError


# lets see how datacompletion works
import numpy as np
import matplotlib.pyplot as plt

data = np.load('bybit_data/downloads/BTCUSDT60.npy')
plt.plot(data[:,2], label = 'open, original')
plt.plot(data[:,3], label = 'high, original')
plt.plot(data[:,4], label = 'low, original')

#remove some lines by hand
data = np.vstack((data[0:116, :], data[231:, :]))
np.save('bybit_data/downloads/BTCUSDT60.npy', data)

# clean data
from Data_Cleaner import Data_Cleaner
Data_Cleaner(experiment).execute()

#plot
data = np.load('bybit_data/downloads/BTCUSDT60.npy')
plt.plot(data[:,2], label = 'open, affine complete')
plt.plot(data[:,3], label = 'high, affine complete')
plt.plot(data[:,4], label = 'low, affine complete')

plt.legend()
plt.savefig('affinecompletion.png')
plt.show()
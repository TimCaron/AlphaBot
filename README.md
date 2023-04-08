# AlphaBot
Python tools for Market Making &amp; Directional Trading Bots on Binance, Bybit &amp; Bitmex 

# Getting Started
pybit and python-binance are required; install with 
pip install pybit 
and 
pip install python-binance

- Note that bybit Downloader works with bybit API v2, so you need pybit v2.4.1 , but not later versions that moved to v3 and v5 API, in my understanding.

- Run example.py to get some basic usage ; see below for a detailed description;

- More basic examples will be published in series of medium posts. 

See e.g. about downloading data : https://medium.com/@tcaron/how-to-download-and-clean-data-from-binance-bybit-

If you want to implement your own startegy using this repo and move it into production, don't hesitate to DM me
timcaron373@gmail.com

Any help is appreciated :
bybit btc : 1MkApQA9Y2K8L4N6De9MwunZtcHLsoKZxx
bybit usdt, BSC (BEP 20) : 0xc3e72656fa54b8c7c7db97cc06fc53e33acb190e


# Project Description, roadmap and todos
Here is a detailed description of all the files.


- settings.py : where you need to put your API keys

- Data_Downloader class has two classes one for Binance, one for Bybit:
  - python-binance and pybit required (this is V2 of API, hence pybit 2.4.1 is fine; not higher since it would go to V3 or V5 and code wont work)
  - main function for both is def download(self, symbol, fundamental_period, start, end = None, print_info = True) that downloads in candles of size "fundamental_period" for some start and optional end date (datetime objects)
  - this methods do not check whether downloaded data already exists or not, and will simply overwrite the files
  - todo add append_new_data method
  - todo check api rate limit of bybit : we can probably speed up the download
  - binance download is pretty slow, so I added a parrallel downloader that downloads multilple symbols at the same time; I never encountered API rate limits even downloading 40 symbols at the same time. Syntax is def parralel_download_binance(symbol_list, fundamental_period, start, end)
  - Note that volume is not handled by the bybit method (probably cause its future; we should try spot data)
  - Finally download automatically calls a method cut_midnight that ensures that the downloaded data starts at midnight utc
  - end product are files like : /binance_data/downloads/BTCUSDT60.npy of shape (L, 7) : columns are opening timestamp/ending timestamp/o, h, l, c, volume
  - Note that if the available data on both exchanges starts later than the requested start, it will print a warning; pay attention to this as many altcoins were not yet listed as of config.default_download_start_date
   - todo add a Class_symbols were you hard save this first available timestamps for many coins


*******************************************************************************************************
The following is heavily based on the central concept of an "experiment", described in class experiment

This is basically a dict that gives : the candle size (fundamental_period), the frequency, and offset,
and other parameters like download start/end date;
train/validation/test split dates for crossvalidation purposes, fees values, etc.

This means, say I download data in hourly (H1) but want to trade every 4Hours, then freq = 4, and I can trade 0:00, 4:00 etc.. this is offset 0
or I can trade every 1am, 5am, etc and this is offset 1. Hence all offsets = [i for i in range(freq)}. This has the advantage to augment the dataset
for purposes of learning and backtesting. Also, if ypoui go below H1 fundamental period, you should be aware that trading at the begining of the hour
is usually different than somewhere in the middle, because in intra-hours data you can easily see that there's a volatility peak at the beginning
of each H1 candle. Here we won't go below H1 anyway (fees are too high, you just lose)...
*******************************************************************************************************

- Class Experiments:
  - This returns a dict; see file for details
  - I usually work with 'download_start_date':  datetime(2020, 10, 1, 0, 0, tzinfo=pytz.utc) and binance data.
      This prevents the analysis of some recent coins like GALA etc but many coins were already listed on Binance at that time
      (Also note that 1 of Oct 2020 is the beginning of BTC bull run; then ETH around Jan 2021; then Alts around Feb/Mar 2021)
  - experiment name will by default be = symbol[-4:].lower() for symbol_list of one symbol long : eg. BTCUSDT -> name = btc
  - for multi symbol expe, just input the name you want
  - data for this experiment will later be stored in binance_data/expebtc/
  - drawdown window by default is one month
  - the order-related part of that dictionnary will depend on the method chosen (market order, limit orders, etc)
  - experiment has in particular an init_dict that says whether you want to delete previous precomputed data/features etc,
      and what features are required, etc, see details
  - experiment['offsets'] is a list ; if you want only one offset, say 0, then define it to be [0], but you can put several offsets,
      it will initialize data for all these offsets ; useful when you want to augment data

*******************************************************************************************************
- Class Data ; must be called with an "experiment dict" (see aboce) via Data(experiment)
      Main class for handling data download and preprocessisng
      It has the following methods:

    - init_directories called by default in __init__ , that creates the required directories if not already created
    - various clean files via os.remove that can be called independently or all called at the same time via self.garbage_collector()
      that delete files according to experiment['init_dict']
    - self.download() that will call previous class download with the experiment parameter (symbol list and strat/end date of download) :
      it will only download data when data is not already downloaded ; if you want to remove and download again, you have to set init_dict['delete_downloaded_data] = True

    - self.clean_data_for_experiment() calls Class Data_Cleaner (see below) and execute it on relevant data for this experiment

    - self.save_data_for_experiment() cuts the data to relevant timestamps according to
        experiment['start_date'], experiment['end_date']
        if they are provided ; run some consistency checks ;
        if the data of different symbols of the exeperiment do not start at the same time, asve data at the first common timestamps and prints a warning
        
      ****************
      Now indenpendently of whether symbol_list has one or several symbols, they will be treated independently, each creating expesymbol1/, expesymbol2/, etc
      if you have several symbols, data will be concatenated later (see below)
      First task is then to aggregate data with given freq and offset ; i want to reduce the data.npy file to data from 1am; 5am; 9am; etc if freq = 4 and offset = 1
      this will be saved under (say symbol = btc) expebtc/concats/BTCUSDT60_freq_offset.npy where 60 here is the fundamental period (could be another value)
      ****************

    - self.aggregate_by_freq_and_offset(self, symbol, freq, offset) aggregates symbol data by freq and offset ; this overwrites by default previous files, but will be called only when files do not exist already : you need to delete aggregated data in init_dict['delete_aggregated_data'] if you need to
      This saves N_symbols*freq files of shape (L, 7) with L == exepedata.shape[0]//freq ; because of offsets, some files may in fact have shape L-1

    - now that we have precomputed all these data by freq and offset and symbol,
       we can call self.load_data(self, symbol, offset) is a small helper that returns the relevant data

    - we need to concatenate data if there are multi-symbols; say we have symbol 1 and symbol 2
      Then, for each symbol and freq/offset, we want to have four separate files, one for opens, highs, lows, closes
      of shape (L, 2 + n_symbols) of format:
      openingtimestamp/closing_timestamp/open_symbol_1/open_symbol_2/...
      this is done in self.concatenate_data()

      This is also valid if there is only one symbol ; if you have only one symbol the expename must be symbol name in lowercase,
      so it will save eg. opens60_4_2.npy of data ts1/ts2/open(symbol) in freq ==4 offset 2, ie 2am, 6am, 10am etc
      and the same for highs/lows/closes; in the path exchange_data/expesymbol/ ; shape is (L, 3)
      If you have n symbols, then path is exchange_data/expename/, and shape is (L, 2+n) : 2 timestamps + n symbols

    - Finally this comes also with a dedicated loader;
      self.load_by_channel(channel, offset) returns either o,h,l,c for the relevant symbol_list/freq/offset

    ************
    At that point we may wanna precompute some features on the concatenated signal ; I will do so on the open values
    meaning that at timestep j, I give myself the past values with j included : this will be used to set market/limit order
    at the beginning of that candle. Thus we precompute the moving average of len L, with L from 2 to self.maxlen
    (maxlen is a free parameter defined in experiment dict); i usually take L = 100 in daily. Example: if fundamental period = 60, 
    freq = 24, L= 32, offset = 15, then it will compute the ma(32) from data concatenated every day at 3pm (UTC time)

    The code at the moment includes prealculation of simple moving averages, and momentums,
    defined as (price(t) - price(t-L))/price(t-L). I actually have more than that and will consider to push it later. You can also easily design your owns. 
    *************

    - Thus, in class data, the default data initializer calls the Class_Feature
      that computes the requested indicators from the o/h/l/c files, znd store them in /expename/concats/

    - self.default_data_initializer() follows instructions of init_dict :
        - delete relevant data,
        - download when required
        - clean the Data
        - cut data and save for experiment,
        - precompute the aggregates by symbol,
        - concatenate the data
        - precompute features
        - print timestamp infos



*******************************************************************************************************
- Class Data_Cleaner
    - is pretty straightforward
    - checks that timsetamps are all sperated by 60 seconds * fundamental period, otherwise calls an affine completion
    - affine completion works only for the opens and close. Highs and lows are just copied and paste (details can be found in the medium post above)
    - if required in config file via config.close_correction, we impose the close at time i to be equal to open(i+1).
      Weirdly enough, this is almost never the case in real downloaded data; I dont know why.
    - If so, it may impact the high low, hence the class ensures that the high/low are indeed the high/low
    - todo later remove large outliers/ only eth and btc (at some point in the past eth data of binance is corrupted
      with a drop of more than 50% on eth on a very regular day ; todo check, give date/hour)

*******************************************************************************************************
- Class Precompute_Features : quite straightforward too
    - We note ma for SMA ; note that ma(1) would be == to the actual price, so we don't compute it.
    - Ma's are computed up to some maximal len in the past, from experiment['max_len'], usually 100 for daily data.
    - The way ma is computed requires that you have at least 2*ma_len data to have a sensible reqult for ma(L) #todo check
    - We note moms for momentums; in fact mom(1) in the code is (price(i) - price(i-1))/price(i-1). 
      Maybe it should be called mom(2); anyway, that's my convention here




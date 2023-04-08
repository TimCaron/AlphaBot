from datetime import datetime, timezone
import pytz

# default download start date + default train/test/val split
default_download_start_date = datetime(2020, 10, 1, 0, 0, tzinfo=pytz.utc) #begining of bull run
default_train_end = datetime(2021, 7, 11,0, 0, tzinfo=pytz.utc) # end of bull run
default_val_end = datetime(2022, 1, 1,0, 0, tzinfo=pytz.utc) # still contains the peak of btc at 65k then huge plunge ; used for validation
# no default end date provided : this will be the last available data point


# data cleaner options
close_correction = True

CPU = 6
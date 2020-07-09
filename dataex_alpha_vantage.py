from alpha_vantage.timeseries import TimeSeries
from openpyxl import load_workbook
from datetime import datetime
from config import config
import pandas as pd
import os.path
import time


#def print_timestamped_text(text = ''):
#    """Prints a timestamped string, e.g. [2020-07-01 22:24:19.256850] Finished [^N225:30min] successfully."""
#    dto = datetime.now()
#    print('[{}] '.format(dto) + text)


#def sleep_handler(nr_of_secs):
#    """Sleeps for a number of seconds, while printing the number of seconds left every 5th seconds."""
#    print("Sleeping {} sec > ".format(nr_of_secs), end="", flush=True)
#    for x in range(nr_of_secs, 0, -1):
#        time.sleep(1)
#        if x % 5 == 0:
#            print(".{}".format(x), end="", flush=True)
#    print(' > Next!')


#def fetch_timeseries_alpha_vantage(lq_output_filename, api_key, ticker_symbol, interval):
#    """Fetch data for 1 symbol and timeframe, and write it to a sheet in an existing or not yet existing excel file."""
#    fq_filename = __file__.replace(os.path.basename(__file__), '') + lq_output_filename
#    # define writer
#    if os.path.isfile(fq_filename):
#        book = load_workbook(lq_output_filename)
#        writer = pd.ExcelWriter(lq_output_filename, engine='openpyxl')
#        writer.book = book

#    else:
#        writer = pd.ExcelWriter(lq_output_filename, engine='xlsxwriter')

#    # define exchange bridge.
#    ts = TimeSeries(key=api_key, output_format='pandas')

#    # try to fetch data and write to excel
#    try:
#        data, meta_data = ts.get_intraday(symbol=ticker_symbol, interval=interval, outputsize='full')
#        print_timestamped_text('Finished [' + ticker_symbol + ':' + interval + '] successfully.')
#        data.to_excel(writer, sheet_name=interval)
#    except:
#        print_timestamped_text('Issue with interval [' + interval + '] for [' + ticker_symbol + ']!!!')

#    writer.save()
#    writer.close()


#def gather_data_alpha_vanatage(api_key, filepath, object_list, max_calls_per_min):
#    """Iterate a list of objects and fetch data related to these objects."""
#    counter = 0

#    # loop the outer container
#    for o in object_list:
#        filename = filepath + o[0]
#        ticker = o[1]
#        # loop the intervals
#        for interval in o[2]:
#            if counter == max_calls_per_min:
#                # sleep for a while ZZZzzz...
#                sleep_handler(75)
#                counter = 1
#                # fetch data
#                fetch_timeseries_alpha_vantage(filename, api_key, ticker, interval)
#            else:
#                counter += 1
#                # fetch data
#                fetch_timeseries_alpha_vantage(filename, api_key, ticker, interval)

## variables that will be used as input for execution.
#filepath = 'datafiles/'
#api_key = config.api_key_alpha_vantage
#max_calls_per_min = 5
##object_list = [['AEX.xlsx', '^AEX', ['30min', '60min']],
##               ['N225.xlsx', '^N225', ['30min', '60min']],
##               ['YM_F.xlsx', 'YM=F', ['30min']],
##               ['MSFT.xlsx', 'MSFT', ['30min']],
##               ['KO.xlsx', 'KO', ['30min']],
##               ['MMM.xlsx', 'MMM', ['30min']],
##               ['JPM.xlsx', 'JPM', ['30min']],
##               ['BA.xlsx', 'BA', ['30min']],
##               ['DIS.xlsx', 'DIS', ['30min']]
##               ]

#object_list = [['AAPL.xlsx', 'AAPL', ['1min']]
#               ]

## execute the data gatherer...
#gather_data_alpha_vanatage(api_key, filepath, object_list, max_calls_per_min)

## Errors
##['DJI.xlsx', 'DJI', ['30min', '60min']]
##['IXIC.xlsx', 'IXIC', ['30min', '60min']]


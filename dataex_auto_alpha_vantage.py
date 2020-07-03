from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
import dateutil.parser as DP
import urllib.request as request
from contextlib import closing
from config import config
import os.path
import shutil
import time
import csv

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
index_file = ROOT_DIR + '/index_files/nasdaq_dl_hist.txt'
nasdaq_file = ROOT_DIR + '/data_files/nasdaqlisted.txt'
genesis_date = '2000-01-01T00:00:00.000000'
max_calls_per_day = 500
max_calls_per_min = 5
api_key = config.api_key_alpha_vantage


def print_timestamped_text(text=''):
    """Prints a timestamped string, e.g. [2020-07-01 22:24:19.256850] Finished [^N225:30min] successfully."""
    dto = datetime.now()
    print('[{}] '.format(dto) + text)


def csv_to_dict(index_file, key_index):
    """ Read a csv into a dictionary """
    try:
        symbol_dict = {}
        with open(index_file, 'r') as fl:
            lines = fl.readlines()
            for line in lines:
                counter = 0
                row = []
                cols = line.split(',')
                for val in cols:
                    if counter == key_index:
                        counter = counter + 1
                        continue
                    else:
                        row.append(val)
                    counter = counter + 1
                symbol_dict[cols[key_index]] = row
    except:
        symbol_dict = {}
    return symbol_dict


def initiate_nq_index_file(index_file, nasdaq_file):
    """ If no index file exists, create it and initiate it.  """
    # Create and fill the index file
    try:
        with open(index_file, 'w+') as ixf:
            with open(nasdaq_file, 'r') as ndf:
                lines = ndf.readlines()
                for line in lines:
                    fields = line.split('|')
                    symbol = fields[0]
                    if symbol == 'Symbol' or symbol[0:18] == 'File Creation Time':
                        continue
                    else:
                        ixf.write('{},{},init\n'.format(symbol, genesis_date))
        print('---Index file initiated---')
        return True
    except:
        print('Error: index file could not be initiated')
        return False


def fetch_nq_ticker_file(ftp_path, nasdaq_file):
    """check if nasdaq file exists, if not, fetch it"""
    try:
        if not os.path.isfile(nasdaq_file):
            with closing(request.urlopen(ftp_path)) as r:
                with open(nasdaq_file, 'wb') as f:
                    shutil.copyfileobj(r, f)
        print('---nasdaq file fetched---')
        return True
    except:
        print('Error: nasdaq file not fetched!')
        return False


def fetch_timeseries_alpha_vantage(api_key, ticker_symbol, interval, file_path):
    """Fetch data for 1 symbol and timeframe, and write it to a sheet in an existing or not yet existing excel file."""
    timestr = time.strftime("%Y%m%d%H%M%S")
    fq_filename = file_path + '/' + ticker_symbol + '_' + interval + '_' + timestr

    # define exchange bridge
    ts = TimeSeries(key=api_key, output_format='pandas')

    # try to fetch data and write to excel
    try:
        data, meta_data = ts.get_intraday(symbol=ticker_symbol, interval=interval, outputsize='full')
        print_timestamped_text('Finished [' + ticker_symbol + ':' + interval + '] successfully.')
        data.to_csv(fq_filename)
        return True
    except:
        print_timestamped_text('Issue with interval [' + interval + '] for [' + ticker_symbol + ']!!!')
        return False


def auto_load_nasdaq_hist(api_key, path_out, index_file, max_calls_per_min, max_calls_per_day, interval):
    """ Load historic data in an intelligent way. ;) """
    #todo
    # 1 iterate symbol_dict and load data. Take into account the restrictions of 5 calls/min and 500 calls/day
    # 2 check if data load is necessary in the index file
    # 3 always write the status of the data load to the index file.

    # Load csv to dict
    symbol_dict = csv_to_dict(index_file, 0, 1)
    symbol_dict_out = {}

    # if dict is not empty, iterate it and fetch data
    if len(symbol_dict) > 0:
        try:
            # iterate symbol_dict
            for key in symbol_dict:
                retry_counter = 5
                while True:
                    if fetch_timeseries_alpha_vantage(api_key, symbol_dict[key], interval,
                                                      path_out) or retry_counter == 0:
                        symbol_dict_out
                        break

        except:
            # write history back to file
            print('No magic today :( ')


def main():
    """Main application"""
    # Test if the index file exists...
    if not os.path.isfile(index_file):
        # Check if nasdaq file exists, if not > try to fetch it from ftp server.
        if fetch_nq_ticker_file('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt', nasdaq_file):
            # Index file successfully fetched > initiate.
            if initiate_nq_index_file(index_file, nasdaq_file):
                # Start the magic!
                auto_load_nasdaq_hist(index_file)
    else:
        # Start the magic!
        path_out = ROOT_DIR + '/data_files'
        auto_load_nasdaq_hist(api_key, path_out, index_file, max_calls_per_min, max_calls_per_day, '1min')


if __name__ == "__main__":
    """ Execute main application """
    main()

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


def initiate_nq_index_file(index_file, nasdaq_file, genesis_date = '2000-01-01T00:00:00.000000'):
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


def append_dl_stats(stats_file_path, context, api_key_index, date, nr_of_calls):
    """Append a line to dl_stats file"""
    try:
        with open(stats_file_path, 'a') as fd:
            fd.write(context + ',{},{},{}'.format(api_key_index, date, nr_of_calls))
        print("---Stats written to stats file---")
        return True
    except:
        print("Error: cannot write to stats file.")
        return False


def fetch_dl_stats(stats_file):
    """Returns counter information of previous download sessions"""
    # todo output_dict should be like {key_index: [key_index,earliest_date(where > -24h),Sum(nr_of_calls)]}
    output_dict = {}
    try:
        header_skipped = 0
        hit_dict = {}
        threshold_dt = datetime.now() - timedelta(hours=24, minutes=1)
        with open(stats_file, 'r') as fl:
            lines = fl.readlines()
            for line in lines:
                if header_skipped == 0:
                    header_skipped = 1
                    continue
                cols = line.split(',')

                # evaluate the new datetime and sum the calls
                t_new = DP.parse(cols[1])
                if threshold_dt < t_new:
                    if cols[0] in hit_dict:
                        t_old = DP.parse(hit_dict[cols[0]][0])
                        last_date = t_old if t_old < t_new else t_new
                        sum_calls = hit_dict[cols[0]][1] + cols[2]
                        hit_dict[cols[0]] = [last_date.isoformat(), sum_calls]
                    else:
                        hit_dict[cols[0]] = [cols[1], cols[2]]

        print('---dl_stats dictionary created---')
    except:
        print('Error: no dl_stats dictionary could be made...')
        output_dict = {}
    return output_dict


def auto_load_nasdaq_hist(stats_file_base, exchange, path_out, index_file, max_calls_per_min, max_calls_per_day, interval):
    """ Load historic data in an intelligent way. ;) """
    # Load csv to dict
    symbol_dict = csv_to_dict(index_file, 0, 1)
    symbol_dict_out = {}

    # fetch stats to know how many download are left for a specific key_index
    stats_file = stats_file_base + exchange
    dl_stats = fetch_dl_stats(stats_file)

    # fetch api keys
    api_key_list = config.api_key_list_single[exchange]

    # iterate the api key list
    i = 0
    while i < len(api_key_list):
        # load key
        api_key = api_key_list[i]
        i += 1

        #todo:
        # 1. check how many iterations are still left in total
        # 2. load the index file
        #   a. check if there have been loads in the last minute and how many we can still do
        #   b. start loading data
        # 3. write back to index file
        # 4. write back to dl_stats file
        earliest_date_within_day = dl_stats[i][0]
        sum_nr_of_calls_within_day = dl_stats[i][1]

        # if dict is not empty, iterate it and fetch data
        if len(symbol_dict) > 0:
            try:
                # iterate symbol_dict
                for key in symbol_dict:
                    retry_counter = 5
                    while True:
                        if fetch_timeseries_alpha_vantage(api_key, symbol_dict[key], interval,path_out) or retry_counter == 0:
                            symbol_dict_out
                            break

            except:
                # write history back to file
                print('No magic today :( ')
        else:
            print('Error: Dictionary with symbols is empty upon arrival!')


def main():
    """Main application"""

    # Variables
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    index_file = ROOT_DIR + '/index_files/nasdaq_dl_hist.txt'
    nasdaq_file = ROOT_DIR + '/data_files/nasdaqlisted.txt'
    raw_data_directory = '/home/raw_data'
    genesis_date = '2000-01-01T00:00:00.000000'
    interval = '1min'
    max_calls_per_day = 500
    max_calls_per_min = 5
    exchange = "alpha_vantage"
    stats_file_base = ROOT_DIR + '/config/dl_stats_'

    # Test if the index file exists...
    if not os.path.isfile(index_file):
        # Check if nasdaq file exists, if not > try to fetch it from ftp server.
        if fetch_nq_ticker_file('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt', nasdaq_file):
            # Nasdaq file successfully fetched > initiate index file if needed.
            if initiate_nq_index_file(index_file, nasdaq_file, genesis_date):
                # Use the index file to start fetching data...
                auto_load_nasdaq_hist(stats_file_base, exchange, raw_data_directory, index_file, max_calls_per_min, max_calls_per_day, interval)
    else:
        # Start the magic!
        path_out = ROOT_DIR + '/data_files'
        auto_load_nasdaq_hist(stats_file_base, exchange, raw_data_directory, index_file, max_calls_per_min, max_calls_per_day, interval)


if __name__ == "__main__":
    """ Execute main application """
    main()

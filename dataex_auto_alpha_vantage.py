#!/usr/bin/env python
#
###############################################################################
#
# Copyright (C) 2019-2020 Paul Geudens
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You might have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
from config import config
from df_helpers import DfHelpers
from df_ftp_fetch import FTPFetch

import dateutil.parser as dp
import urllib.request as request
import os.path
import shutil
import time


def index_file_to_dict(index_file):
    """ Read a csv into a dictionary """
    try:
        symbol_dict = {}
        with open(index_file, 'r') as fl:
            lines = fl.readlines()
            for line in lines:
                cols = line.split(',')
                t_symbol = cols[0]
                t_last_upd_date = dp.parse(cols[1]).isoformat()
                t_status = cols[2].rstrip()
                if t_symbol not in symbol_dict:
                    symbol_dict[t_symbol] = [t_last_upd_date, t_status]
                else:
                    print('Error: symbol [' + t_symbol + '] was being processed multiple times...')
        print('index_file_to_dict successfully executed')
    except:
        symbol_dict = {}
        print('Error: symboldict not filled...')
    return symbol_dict


def dict_to_index_file(index_file, backup_file_template, ix_dict):
    """ write an updated dict to a new index file and backup the old one """
    # backup old index file
    timestr = time.strftime("%Y%m%d%H%M%S")
    backup_file = backup_file_template + '_' + timestr
    os.replace(index_file, backup_file)

    try:
        with open(index_file, 'w+') as ixf:
            for symbol in ix_dict:
                ixf.write('{},{},{}'.format(symbol, ix_dict[symbol][0], ix_dict[symbol][1]))
        print('---Index file rewritten---')
    except:
        print('Error: index file could not be initiated')


def initiate_nq_index_file(index_file, nasdaq_file, genesis_date='2000-01-01T00:00:00.000000'):
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


def fetch_timeseries_alpha_vantage(api_key, ticker_symbol, interval, file_path):
    """Fetch data for 1 symbol and timeframe, and write it to a sheet in an existing or not yet existing excel file."""
    timestr = time.strftime("%Y%m%d%H%M%S")
    fq_filename = file_path + '/' + ticker_symbol + '_' + interval + '_' + timestr

    # define exchange bridge
    ts = TimeSeries(key=api_key, output_format='pandas')

    # try to fetch data and write to excel
    try:
        data, meta_data = ts.get_intraday(symbol=ticker_symbol, interval=interval, outputsize='full')
        DfHelpers.print_timestamped_text('Finished [' + ticker_symbol + ':' + interval + '] successfully.')
        data.to_csv(fq_filename)
        return True
    except:
        DfHelpers.print_timestamped_text('Issue with interval [' + interval + '] for [' + ticker_symbol + ']!!!')
        return False


def append_dl_stats(stats_file_path, api_key_index, date, nr_of_calls):
    """Append a line to dl_stats file"""
    try:
        with open(stats_file_path, 'a') as fd:
            fd.write('{},{},{}'.format(api_key_index, date, nr_of_calls))
        print("---Stats written to stats file---")
        return True
    except:
        print("Error: cannot write to stats file.")
        return False


def fetch_dl_stats(stats_file, hours_back):
    """Returns counter information of previous download sessions"""
    output_dict = {}
    try:
        header_skipped = 0
        threshold_dt = datetime.now() - timedelta(hours=hours_back, minutes=1)
        with open(stats_file, 'r') as fl:
            lines = fl.readlines()
            for line in lines:
                # Skip first line and last empty line
                if header_skipped == 0 or len(line.strip()) == 0:
                    header_skipped = 1
                    continue
                cols = line.split(',')
                # evaluate the new datetime and sum the calls
                t_index = int(cols[0].rstrip())
                t_new = dp.parse(cols[1])
                nr_new = int(cols[2].rstrip())

                if threshold_dt < t_new:
                    if t_index in output_dict:
                        t_old = dp.parse(output_dict[t_index][0])
                        last_date = t_old if t_old < t_new else t_new
                        sum_calls = output_dict[t_index][1] + nr_new
                        output_dict[t_index] = [last_date.isoformat(), sum_calls]
                    else:
                        output_dict[t_index] = [cols[1], nr_new]
        print('---dl_stats dictionary created---')
    except:
        print('Error: no dl_stats dictionary could be made...')
        output_dict = {}

    if len(output_dict) == 0:
        for i in range(0, len(config.api_key_list_single)):
            output_dict[i] = ['2000-01-01T00:00:00.000000', 0]

    return output_dict


def auto_load_nasdaq_hist(stats_file_base, exchange, path_out, index_file, backup_file_template, max_calls_per_min,
                          max_calls_per_day, interval):
    """ Load historic data in an intelligent way. ;) """
    # Load csv to dict > {'AACG': ['2000-01-01T00:00:00', 'init'], 'AAL': ['2000-01-01T00:00:00', 'init']]
    symbol_dict = index_file_to_dict(index_file)
    if len(symbol_dict) == 0:
        return
    symbol_dict_out = {}
    retry_threshold = 1
    days_before_new_processing = 30
    dont_process_after = datetime.now() - timedelta(days=days_before_new_processing)

    # fetch stats to know how many download are left for a specific key_index
    stats_file = stats_file_base + exchange
    dl_stats = fetch_dl_stats(stats_file, 24)

    # fetch api keys
    api_key_dict = config.api_key_list_single[exchange]

    # iterate the api key list
    api_key_index = 0
    while api_key_index < len(api_key_dict):
        print('Start loading data for key index {}'.format(api_key_index))

        # load key
        api_key = api_key_list[api_key_index]

        calls_left_for_key_today = max_calls_per_day - dl_stats[api_key_index][1]
        calls_made_for_key = 0
        calls_left_minute = max_calls_per_min

        # only continue if calls are still left
        if calls_left_for_key_today > 0:

            # iterate it and fetch data
            try:
                print('Start iterating symbols, calls_left_for_key_today={}'.format(calls_left_for_key_today))
                # iterate symbol_dict
                for symbol in symbol_dict:
                    # only continue if calls are still left
                    if calls_left_for_key_today > 0:
                        last_process_time = dp.parse(symbol_dict[symbol][0])
                        status = symbol_dict[symbol][1]
                        retry_counter = retry_threshold

                        if last_process_time > dont_process_after and status == 'succeeded':
                            # write back to output list
                            symbol_dict_out[symbol] = symbol_dict[symbol]
                        else:
                            while True:
                                # only continue if calls are still left
                                if calls_left_for_key_today > 0:
                                    if calls_left_minute > 0:
                                        retry_counter -= 1
                                        # try fetch data
                                        data_fetched = fetch_timeseries_alpha_vantage(api_key, symbol, interval,path_out)
                                        calls_made_for_key += 1
                                        if data_fetched:
                                            symbol_dict_out[symbol] = [datetime.now().isoformat(), 'succeeded']
                                            break
                                        elif retry_counter == 0:
                                            symbol_dict_out[symbol] = [datetime.now().isoformat(), 'failed']
                                            break
                                    else:
                                        calls_left_minute = max_calls_per_min
                                        DfHelpers.sleep_handler(65)
                                else:
                                    # No more calls left for api key index
                                    symbol_dict_out[symbol] = symbol_dict[symbol]
                                    break
                    else:
                        if (api_key_index+1) < len(api_key_list):
                            print('No more calls left for api key index [{}]. Continuing with next key...'.format(api_key_index))
                            break
                        else:
                            symbol_dict_out[symbol] = symbol_dict[symbol]
            except:
                # write history back to file
                print('Error: auto_load_nasdaq_hist screwed, somewhere...')

        # write back to dl_stats
        if calls_made_for_key > 0:
            append_dl_stats(stats_file, api_key, datetime.now().isoformat(), calls_made_for_key)

        # Increase iterator
        api_key_index += 1

    # write back to index file...
    if len(symbol_dict_out) > 0:
        dict_to_index_file(index_file, backup_file_template, symbol_dict_out)


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
    backup_file_template = ROOT_DIR + '/index_files/backup/nasdaq_dl_hist'

    # Test if the index file exists...
    if not os.path.isfile(index_file):
        # Check if nasdaq file exists, if not > try to fetch it from ftp server.
        if FTPFetch.fetch_nq_ticker_file('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt', nasdaq_file):
            # Nasdaq file successfully fetched > initiate index file if needed.
            if initiate_nq_index_file(index_file, nasdaq_file, genesis_date):
                # Use the index file to start fetching data...
                auto_load_nasdaq_hist(stats_file_base, exchange, raw_data_directory, index_file, backup_file_template,
                                      max_calls_per_min, max_calls_per_day, interval)
    else:
        print('Let\'s do this!')
        # Start the magic!
        auto_load_nasdaq_hist(stats_file_base, exchange, raw_data_directory, index_file, backup_file_template,
                              max_calls_per_min, max_calls_per_day, interval)

if __name__ == "__main__":
    """ Execute main application """
    main()

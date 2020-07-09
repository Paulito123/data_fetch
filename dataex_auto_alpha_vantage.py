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
from datetime import datetime, timedelta
from data_fetch.df_helpers import DfHelpers
from data_fetch.tickers import Tickers

import dateutil.parser as dp
import os.path


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
    #index_file = ROOT_DIR + '/index_files/nasdaq_dl_hist.txt'
    #nasdaq_file = ROOT_DIR + '/datafiles/nasdaqlisted.txt'
    #raw_data_directory = '/home/raw_data'
    genesis_date = '2000-01-01T00:00:00.000000'
    interval = '1min'
    #max_calls_per_day = 500
    #max_calls_per_min = 5
    exchange = "alpha_vantage"
    #stats_file_base = ROOT_DIR + '/config/dl_stats_'
    #backup_file_template = ROOT_DIR + '/index_files/backup/nasdaq_dl_hist'

    # Test if the index file exists...
    if not os.path.isfile(index_file):
        # Check if nasdaq file exists, if not > try to fetch it from ftp server.
        if Tickers.fetch_nq_ticker_file('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt', nasdaq_file):
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

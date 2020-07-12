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
import os.path
import configparser

import data_fetch.extractor as extr
import data_fetch.data_sources as datas
import data_fetch.tickers as tckrs
import data_fetch.data_source_limits as lims
import data_fetch.download_stats as dls

from data_fetch.helpers import Helpers as dh

from datetime import datetime, timedelta


def main():
    """Main method of the data_fetch application"""
    # Load configurations
    root_dir = os.path.dirname(os.path.abspath(__file__)) + '/..'
    conf_file = root_dir + '/config/data_fetch.conf'
    config = configparser.ConfigParser()
    config.read(conf_file)

    # initialize ticker object
    ticker_obj = tckrs.Ticker(config['ticker'])

    # Try get Nasdaq file...
    target_file = root_dir + '/' + config['tickers']['path_file']
    nq_fetched = ticker_obj.fetch_nq_ticker_file()
    if not nq_fetched and not os.path.isfile(target_file):
        print("EXIT APPLICATION: Missing ticker file!")
        exit()
    else:
        # Update ticker database
        ticker_obj.nasdaq_ticker_file_to_db_sync()

    # Iterate keys in endless loop
    while True:
        # Get data_source iterator
        data_source_obj = datas.DataSource(root_dir, config['data_source'])
        ds_list = data_source_obj.get_active_data_source()

        # Iterate data sources
        for ds in ds_list:
            # Create an extractor object
            extractor_obj = extr.Extractor(root_dir, config['extractor'], ds['key'], ds['data_source'])

            # Fetch the limits for the given data_source, to use in the query for getting the download stats.
            # {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0, 'minute_limit': 0}
            limits_obj = lims.DSLimits(root_dir, config['ds_limits'])
            limits = limits_obj.get_ds_limits(ds['data_source'])

            # Get an updated list of all symbols that need a refresh of data
            date_as_from = (
                    datetime.now() - timedelta(days=int(config['extractor']['days_before_refresh']))).isoformat()
            symbol_iterator = 0
            symbol_list = ticker_obj.get_tickers_later_then(date_as_from)

            # Get dl_stats and limits for data source
            # {'data_source' = '', 'conn_name' = '', 'day_calls_left': 0, 'hour_calls_left': 0, 'minute_calls_left': 0}
            dl_stats_obj = dls.DownloadStats(root_dir, config['download_stats'])
            calls_left = dl_stats_obj.get_calls_left_for_connection(
                ds['data_source'],
                ds['name'],
                limits)

            calls_left_day = calls_left['day_calls_left']
            calls_left_hour = calls_left['hour_calls_left']
            calls_left_minute = calls_left['minute_calls_left']
            stats_counter = 0
            i_can_pass = True

            if limits['day_limit'] == 0 or calls_left_day > 0:
                while i_can_pass:

                    if  (limits['day_limit'] == 0 or calls_left_day > 0) and \
                        (limits['hour_limit'] == 0 or calls_left_hour > 0) and \
                        (limits['minute_limit'] == 0 or calls_left_minute > 0) and \
                        symbol_iterator < len(symbol_list):
                        # We can perform 1 call...
                        data_fetched = extractor_obj.fetch_timeseries(symbol_list[symbol_iterator]['ticker'])

                        stats_counter += 1
                        symbol_iterator += 1
                        # Decrease limits

                        # Write back to
                    #elif :
                        # Check why we cannot pass










        # Fetch all tickers within the result of the search with limits

        # Start main iteration with checks on ds limits

        # Fetch data and keep the stats

        # Write the result of a fetch back to the tickers database

        # Write stats back at end of day or at end of key iteration

        #dh.sleep_handler(3600)
        break

if __name__ == "__main__":
    main()

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
import config.configuration as cf
from data_fetch.tickers import Tickers as tickrs
from data_fetch.df_helpers import DfHelpers as dh


def main():
    """Main method of the data_fetch application"""
    # Main variables
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + '/..'
    db_file = ROOT_DIR + '/database/config_db.json'
    db_name = 'main_prod'

    # Get configurations
    conf = cf.Configuration(db_file, db_name)
    conf_dict = conf.get_main_config()

    # Try get Nasdaq file...
    target_file = ROOT_DIR + conf_dict['path_local_nasdaq_ticker_file']
    nq_fetched = tickrs.fetch_nq_ticker_file(conf_dict['path_nasdaq_ticker_ftp'], target_file)
    if not nq_fetched and not os.path.isfile(target_file):
        print("EXIT APPLICATION: Missing ticker file!")
        exit()
    else:
        # Update ticker database
        ticker_db_file = ROOT_DIR + conf_dict['path_ticker_db']
        tickrs.nasdaq_ticker_file_to_db_sync(target_file, ticker_db_file, conf_dict['tabnm_tickers'])

    # Iterate keys in endless loop
    main_loop = True
    while main_loop:
        # Get dl_stats todo
        x=1


    '''
    main_loop = True
    while main_loop:
        #
        if:
            x = 1
        else:
            # Wait 1 hour and try again
            dh.sleep_handler(3600)

        break
    '''

    # Get latest version of nasdaq file


if __name__ == "__main__":
    main()

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
# ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt
# ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt
#
###############################################################################
import shutil
import urllib.request as request
import os.path
from contextlib import closing
from tinydb import TinyDB, Query
from df_helpers import DfHelpers


class Tickers:

    @staticmethod
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

    @staticmethod
    def nasdaq_ticker_file_to_db_sync(ticker_file, db_file, ticker_db_name):
        ''''''
        # Check if ticker file exists.
        if not os.path.isfile(ticker_file):
            DfHelpers.print_timestamped_text("ticker file [{}] does not exist.".format(ticker_file))
            return False

        # Check if db exists.
        if not os.path.isfile(db_file):
            DfHelpers.print_timestamped_text("database file [{}] does not exist.".format(db_file))
            return False

        try:
            # Open the database
            with TinyDB(db_file) as db:
                # Open the ticker table
                ticker_table = db.table(ticker_db_name)

                # Iterate the file values
                with open(ticker_file, 'r') as ndf:
                    # Get lines in the file and iterate them
                    lines = ndf.readlines()
                    for line in lines:
                        # Split the line and take the ticker symbol
                        fields = line.split('|')
                        symbol = fields[0]
                        if symbol == 'Symbol' or symbol[0:18] == 'File Creation Time':
                            # Skip irrelevant lines
                            continue
                        else:
                            # Check if symbol exists in DB
                            qy = Query()
                            res = ticker_table.search(qy.symbol == symbol)
                            if len(res) == 0:
                                # Symbol does not exist, therefore insert it
                                ticker_table.insert({'ticker': symbol, 'last_update_date': '2000-01-01T00:00:00.000000',
                                                     'last_status': 'init'})
            return True
        except:
            DfHelpers.print_timestamped_text("Error: cannot open database.")
            return False

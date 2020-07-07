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
from tinydb import *
from df_helpers import DfHelpers as h
import dateutil.parser as dp


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
        ''' Update the ticker database with the latest tickers '''
        # Check if ticker file exists.
        if not os.path.isfile(ticker_file):
            h.print_timestamped_text("ticker file [{}] does not exist.".format(ticker_file))
            return False

        try:
            # Open the database, if it doesn't exist, create it.
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
                            res = ticker_table.search(qy.ticker == symbol)
                            if len(res) == 0:
                                # Symbol does not exist, therefore insert it
                                ticker_table.insert({
                                    'ticker': symbol,
                                     'last_update_date': '2000-01-01T00:00:00.000000',
                                     'last_update_date_epoch': dp.parse('2000-01-01T00:00:00.000000').timestamp(),
                                     'last_status': 'init'})
                                h.print_timestamped_text('Symbol [{}] added.'.format(symbol))
                            else:
                                h.print_timestamped_text('Symbol [{}] already exists.'.format(symbol))
            return True
        except:
            h.print_timestamped_text("Error: cannot open or create database.")
            return False


    @staticmethod
    def update_ticker_status(db_file, ticker_table_name, ticker_info):
        ''' Update the ticker db with ticker info. Requires a dict with following elements: ticker, last_update_date, last_status'''

        # Check if db exists.
        if not os.path.isfile(db_file):
            h.print_timestamped_text("database file [{}] does not exist.".format(db_file))
            return False

		# Check if ticker info is well formatted.
        if 'ticker' not in ticker_info:
            print('Error: missing element [ticker] in ticker_info.')
        elif 'last_update_date' not in ticker_info:
            print('Error: missing element [last_update_date] in ticker_info.')
        elif 'last_status' not in ticker_info:
            print('Error: missing element [last_status] in ticker_info.')

        try:
            # Open the database
            with TinyDB(db_file) as db:
                # Open the ticker table and create query object
                ticker_table = db.table(ticker_table_name)
                qy = Query()

                # Upsert the new ticker info
                ticker_table.upsert({'ticker': ticker_info['ticker'], 
                                     'last_update_date': ticker_info['last_update_date'],
                                     'last_update_date_epoch': dp.parse(ticker_info['last_update_date']).timestamp(),
                                     'last_status': ticker_info['last_status']}, qy['ticker'] == ticker_info['ticker'])
                
                h.print_timestamped_text('Symbol [{}] upserted.'.format(ticker_info['ticker']))

            return True
        except:
            h.print_timestamped_text("Error: cannot open database.")
            return False

    @staticmethod
    def get_tickers_later_then(db_file, ticker_table_name, relevance_date_as_from):
        ''' Update the ticker db with ticker info. Requires a dict with following elements: ticker, last_update_date, last_status'''

        # Check if db exists.
        if not os.path.isfile(db_file):
            h.print_timestamped_text("database file [{}] does not exist.".format(db_file))
            return {}

        try:
            rel_date_epoch = dp.parse(relevance_date_as_from).timestamp()
        except:
            print('Error: relevance_date_as_from not a valid date')
            return

        try:
            # Open the database
            with TinyDB(db_file) as db:
                # Open the ticker table and create query object
                ticker_table = db.table(ticker_table_name)
                qy = Query()

                # Get all tickers that have not been process in the time between relevance_date_as_from and now.
                relevant_tickers_list = ticker_table.search(qy.last_update_date_epoch < rel_date_epoch)

            return relevant_tickers_list
        except:
            h.print_timestamped_text("Error: cannot open database.")
            return {}

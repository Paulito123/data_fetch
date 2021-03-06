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
import re
from contextlib import closing
from tinydb import *
from data_dief.helpers import Helpers as h
import dateutil.parser as dp


class Ticker:

    def __init__(self, root_dir, config):
        self._config = config
        self.root_dir = root_dir

    def fetch_nq_ticker_file(self):
        """check if nasdaq file exists, if not, fetch it"""

        # Local variables
        nasdaq_file = self.root_dir + '/' + self._config['nasdaq']['path_file']

        try:
            if not os.path.isfile(nasdaq_file):
                with closing(request.urlopen(self._config['nasdaq']['path_ftp'])) as r:
                    with open(nasdaq_file, 'wb') as f:
                        shutil.copyfileobj(r, f)

            print('---nasdaq file fetched---')
            return True

        except:
            print('Error: nasdaq file not fetched!')
            return False

    def nasdaq_ticker_file_to_db_sync(self):
        ''' Update the ticker database with the latest tickers '''

        #Local variables
        ticker_file = self.root_dir + '/' + self._config['nasdaq']['path_file']
        db_file = self.root_dir + '/' + self._config['ticker']['path_db']
        ticker_db_name = self._config['nasdaq']['table_name']

        # Check if ticker file exists.
        if not os.path.isfile(ticker_file):
            h.print_timestamped_text("Error: ticker file [{}] does not exist.".format(ticker_file))
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
                        columns = line.split('|')
                        symbol = columns[0]
                        if symbol == 'Symbol' or symbol[0:18] == 'File Creation Time':
                            # Skip irrelevant lines
                            continue
                        else:
                            # Check if symbol exists in DB
                            res = ticker_table.search(Query()['ticker'] == symbol)
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

    def update_nq_ticker_status(self, ticker_info):
        '''
        Update the ticker db with ticker info. Requires a dict with following elements:
        ticker, last_update_date, last_status
        '''

        # Local variables
        ticker_db_name = self._config['nasdaq']['table_name']
        db_file = self.root_dir + '/' + self._config['ticker']['path_db']

        # Check if db exists.
        if not os.path.isfile(db_file):
            h.print_timestamped_text("Error: database file [{}] does not exist.".format(db_file))
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
                ticker_table = db.table(ticker_db_name)

                # Upsert the new ticker info
                ticker_table.upsert({'ticker': ticker_info['ticker'], 
                                     'last_update_date': ticker_info['last_update_date'],
                                     'last_update_date_epoch': dp.parse(ticker_info['last_update_date']).timestamp(),
                                     'last_status': ticker_info['last_status']},
                                    Query()['ticker'] == ticker_info['ticker'])

            return True
        except:
            h.print_timestamped_text("Error: cannot upsert ticker info.")
            return False

    def get_nq_tickers_later_then(self, relevance_date_as_from):
        '''Get the tickers that meet the date requirements.'''

        # Local variables
        ticker_db_name = self._config['nasdaq']['table_name']
        db_file = self.root_dir + '/' + self._config['ticker']['path_db']

        # Check if db exists.
        if not os.path.isfile(db_file):
            h.print_timestamped_text("Error: database file [{}] does not exist.".format(db_file))
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
                ticker_table = db.table(ticker_db_name)
                qy = Query()

                # Get all tickers that have not been process in the time between relevance_date_as_from and now.
                relevant_tickers_list = ticker_table.search(qy.last_update_date_epoch < rel_date_epoch)

            return relevant_tickers_list
        except:
            h.print_timestamped_text("Error: cannot open database.")
            return {}

    def sp500_ticker_file_to_db_sync(self):
        ''' Update the ticker database with the latest tickers '''

        #Local variables
        ticker_file = self.root_dir + '/' + self._config['sp500']['path_file']
        db_file = self.root_dir + '/' + self._config['ticker']['path_db']
        ticker_db_name = self._config['sp500']['table_name']

        # Check if ticker file exists.
        if not os.path.isfile(ticker_file):
            h.print_timestamped_text("Error: ticker file [{}] does not exist.".format(ticker_file))
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
                        columns = re.split(r'\t+', line.rstrip('\t'))

                        symbol = columns[0]
                        name = columns[1]
                        sector = columns[2]
                        if len(symbol.strip()) == 0:
                            # Skip irrelevant lines
                            continue
                        else:
                            # Check if symbol exists in DB
                            res = ticker_table.search(Query()['ticker'] == symbol)
                            if len(res) == 0:
                                # Symbol does not exist, therefore insert it
                                ticker_table.upsert({
                                    'ticker': symbol,
                                    'company': name,
                                    'sector': sector,
                                    'last_update_date': '2000-01-01T00:00:00.000000',
                                    'last_update_date_epoch': dp.parse('2000-01-01T00:00:00.000000').timestamp(),
                                    'last_status': 'init'},
                                    Query()['ticker'] == symbol)
                                h.print_timestamped_text('Symbol [{}] added.'.format(symbol))
                            else:
                                h.print_timestamped_text('Symbol [{}] already exists.'.format(symbol))
            return True
        except:
            h.print_timestamped_text("Error: cannot open or create database.")
            return False

    def get_sp500_tickers(self):
        '''Get all sp500 tickers.'''

        # Local variables
        ticker_db_name = self._config['sp500']['table_name']
        db_file = self.root_dir + '/' + self._config['ticker']['path_db']

        # Check if db exists.
        if not os.path.isfile(db_file):
            h.print_timestamped_text("Error: database file [{}] does not exist.".format(db_file))
            return []

        try:
            # Open the database
            with TinyDB(db_file) as db:
                # Open the ticker table and create query object
                ticker_table = db.table(ticker_db_name)
                return ticker_table.all()
        except:
            h.print_timestamped_text("Error: cannot open database.")
            return []

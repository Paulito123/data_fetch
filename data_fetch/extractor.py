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
from data_fetch.helpers import Helpers as h
from tinydb import TinyDB, Query
import dateutil.parser as dp
from alpha_vantage.timeseries import TimeSeries as avts
import sys
import os.path
import time

class Extractor:

    def __index__(self, root_dir, config, api_key, data_source):
        self._config = config
        self._api_key = api_key
        self.root_dir = root_dir
        self.data_source = data_source

    def fetch_timeseries(self, ticker_symbol):
        """Dispatch method"""
        method_name = sys._getframe().f_code.co_name + '_' + str(self.data_source)

        # Get the method from 'self'. Default to a lambda.
        method = getattr(self, method_name, lambda: [])

        # Call the method as we return it
        return method(ticker_symbol)

    def fetch_timeseries_alpha_vantage(self, ticker_symbol):
        """
        Fetch data for 1 symbol and timeframe, and write it to a sheet in an existing or not yet existing excel file.
        """
        timestr = time.strftime("%Y%m%d%H%M%S")
        fq_filename = self._config['data_output_path'] + ticker_symbol + '_' + self._config['interval'] + '_' + timestr

        # define exchange bridge
        ts = avts(key=self._api_key, output_format='pandas')

        # try to fetch data and write to excel
        try:
            data, meta_data = ts.get_intraday(symbol=ticker_symbol, interval=self._config['interval'], outputsize='full')
            h.print_timestamped_text('Finished [' + ticker_symbol + ':' + self._config['interval'] + '] successfully.')
            data.to_csv(fq_filename)
            return True
        except:
            h.print_timestamped_text('Issue with interval [' + self._config['interval'] + '] for [' + ticker_symbol + ']!!!')
            return False

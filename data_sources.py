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
###############################################################################
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
from config.configuration import Configuration as conf
from df_helpers import DfHelpers
from tinydb import TinyDB, Query
from tickers import Tickers

import dateutil.parser as dp
import urllib.request as request
import os.path
import shutil
import time


class df_alpha_vantage:

    @staticmethod
    def fetch_timeseries(api_key, ticker_symbol, interval, file_path):
        """
        Fetch data for 1 symbol and timeframe, and write it to a sheet in an existing or not yet existing excel file.
        """
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
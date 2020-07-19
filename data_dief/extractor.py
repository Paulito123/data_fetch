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
from data_dief.helpers import Helpers as h
from tinydb import TinyDB, Query
import dateutil.parser as dp
import finnhub
import pandas as pd
from alpha_vantage.timeseries import TimeSeries as avts
import sys
import os.path
import time

class Extractor:

    def __init__(self, root_dir, config, api_key, data_source):
        self._config = config
        self._api_key = api_key
        self.root_dir = root_dir
        self.data_source = data_source

        if self.data_source == 'finnhub':
            # Configure API key
            self.configuration = finnhub.Configuration(
                api_key={
                    'token': self._api_key
                }
            )

    def fetch_timeseries(self, ticker_symbol, interval, from_date=None, to_date=None):
        """Dispatch method"""
        method_name = sys._getframe().f_code.co_name + '_' + str(self.data_source)

        # Get the method from 'self'. Default to a lambda.
        method = getattr(self, method_name, lambda: [])

        # Call the method as we return it
        return method(ticker_symbol, interval, from_date, to_date)

    def fetch_timeseries_alpha_vantage(self, ticker_symbol, interval, from_date, to_date):
        """
        Fetch data for 1 symbol and timeframe, and write it to a flat file.
        Returns the status of the result of the operation.
        """
        timestr = time.strftime("%Y%m%d%H%M%S")
        fq_filename = \
            self._config['fq_data_output_path'] + '/' + self.data_source + '/' + \
            ticker_symbol + '_' + interval + '_' + timestr

        # define exchange bridge
        ts = avts(key=self._api_key, output_format='pandas')

        # try to fetch data and write to excel
        try:
            data, meta_data = ts.get_intraday(
                symbol=ticker_symbol,
                interval=interval,
                outputsize='full')
            data.to_csv(fq_filename)
            h.print_timestamped_text('Finished [' + ticker_symbol + ':' + interval + '] successfully.')
            return {'status': 'succeeded'}
        except FileNotFoundError:
            err_msg = 'Error: file {} was not found!'.format(fq_filename)
            h.print_timestamped_text(err_msg)
            return {'status': 'error', 'message': err_msg}
        except:
            err_msg = 'Error: issue with interval [' + interval + '] for [' + ticker_symbol + ']!!!'
            h.print_timestamped_text(err_msg)
            return {'status': 'error', 'message': err_msg}

    def fetch_timeseries_finnhub(self, ticker_symbol, interval, from_date, to_date):
        """
        from=1590988249
        to=1591852249
        Interval <code>1, 5, 15, 30, 60, D, W, M </code>
        """
        if not from_date is None and not to_date is None:
            fq_filename = \
                self._config['fq_data_output_path'] + '/' + self.data_source + '/' + ticker_symbol + '_' + \
                interval + '_' + from_date[:10] + '-' + to_date[:10]
        else:
            err_msg = "Error: datetime string is empty."
            print(err_msg)
            return {'status': 'error', 'message': err_msg}

        from_epoch = h.datetime_string_to_epoch(from_date)
        to_epoch = h.datetime_string_to_epoch(to_date)

        if from_epoch == 0 or to_epoch == 0:
            err_msg="Error: datetime string could not be converted to epoch time."
            return {'status': 'error', 'message': err_msg}

        #print('{}:{}:{}:{}'.format(ticker_symbol, interval, from_epoch, to_epoch))

        try:
            finnhub_client = finnhub.DefaultApi(finnhub.ApiClient(self.configuration))

            a = finnhub_client.stock_candles(ticker_symbol, interval, from_epoch, to_epoch)
            a = a.to_dict()

            if a['s'] == 'ok':
                a.pop('s', None)
            else:
                err_msg="Error: response not in expected format."
                return {'status': 'error', 'message': err_msg}

            df = pd.DataFrame({key: pd.Series(value) for key, value in a.items()})
            df.to_csv(fq_filename, encoding='utf-8', index=False)
            h.print_timestamped_text('Finished [' + ticker_symbol + ':' + interval + '] successfully.')
            return {'status': 'succeeded'}

        except FileNotFoundError:
            err_msg='Error: file {} was not found!'.format(fq_filename)
            h.print_timestamped_text(err_msg)
            return {'status': 'error', 'message': err_msg}
        except:
            err_msg='Error: issue with interval [' + interval + '] for [' + ticker_symbol + ']!!!'
            h.print_timestamped_text(err_msg)
            return {'status': 'error', 'message': err_msg}

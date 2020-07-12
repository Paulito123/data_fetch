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
# todo: check https://finnhub.io/docs/api#stock-candles
#
###############################################################################
from datetime import datetime, timedelta
from data_fetch.helpers import Helpers as h
from tinydb import TinyDB, Query
import dateutil.parser as dp


class DownloadStats:

    def __init__(self, root_dir, config):
        self._config = config
        self.root_dir = root_dir

    def append_dl_stats(self, conn_info):
        """
        Add a statistic to the specified stats database. conn_info should be like
        {'data_source'='','conn_name'='','start_datetime'='','calls'=0}
        """
        # Local variables
        stats_db = self.root_dir + '/' + self._config['path_db']
        stats_table = self._config['table_name']

        # Open the database
        with TinyDB(stats_db) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(stats_table)

            # Insert the new ticker info
            stats_table_obj.insert({'data_source': conn_info['data_source'],
                                    'conn_name': conn_info['conn_name'],
                                    'start_datetime': conn_info['start_datetime'],
                                    'start_datetime_epoch': dp.parse(conn_info['start_datetime']).timestamp(),
                                    'calls': conn_info['calls']})

            h.print_timestamped_text('Stats for [{}] inserted.'.format(conn_info['conn_name']))


    def get_calls_left_for_connection(self, data_source, conn_name, limits):
        """
        Returns number of calls left for a connection. Output format is:
        {'data_source'='','conn_name'='', 'day_calls_left': 0, 'hour_calls_left': 0, 'minute_calls_left': 0}
        When there is no limit, -1 is returned.
        """
        # Local variables
        stats_db = self.root_dir + '/' + self._config['path_db']
        stats_table = self._config['table_name']

        # Calculate epoch times for day and hour limits
        ts_day = (datetime.now() - timedelta(hours=24, minutes=1)).timestamp()
        ts_hour = (datetime.now() - timedelta(hours=1, minutes=1)).timestamp()
        ts_min = (datetime.now() - timedelta(minutes=1)).timestamp()

        # Open the database
        with TinyDB(stats_db) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(stats_table)
            qy = Query()
            res = stats_table_obj.search((qy['data_source'] == data_source) &
                                         (qy['conn_name'] == conn_name) &
                                         (qy['start_datetime_epoch'] > ts_day))

            if len(res) == 0:
                return {'data_source': data_source, 'conn_name': conn_name,
                        'day_calls_left': limits['day_limit'],
                        'hour_calls_left': limits['hour_limit'],
                        'minute_calls_left': limits['minute_limit']}
            else:
                calls_made_day=0
                calls_made_hour=0
                calls_made_minute=0

                for entry in res:
                    # Count call for last day
                    if limits['day_limit'] > 0:
                        calls_made_day += entry['calls']

                    # Count calls for last hour
                    if limits['hour_limit'] > 0:
                        if entry['start_datetime_epoch'] > ts_hour:
                            calls_made_hour += entry['calls']

                    # Count calls for last minute
                    if limits['minute_limit'] > 0:
                        if entry['start_datetime_epoch'] > ts_min:
                            calls_made_minute += entry['calls']

                return {'data_source': data_source, 'conn_name': conn_name, 
                        'day_calls_left': -1 if limits['day_limit'] == 0 else limits['day_limit'] - calls_made_day,
                        'hour_calls_left': -1 if limits['hour_limit'] == 0 else limits['hour_limit'] - calls_made_hour,
                        'minute_calls_left': -1 if limits['minute_limit'] == 0 else limits['minute_limit'] - calls_made_minute}

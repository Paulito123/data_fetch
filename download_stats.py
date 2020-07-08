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
from df_helpers import DfHelpers as h
from tinydb import TinyDB, Query
from config.configuration import Configuration as conf

import dateutil.parser as dp
import urllib.request as request
import os.path
import shutil
import time

class DownloadStats:


    @staticmethod
    def append_dl_stats(stats_db, stats_table, conn_info):
        """
        Add a statistic to the specified stats database. data_box should be like
        {'data_source'='','conn_name'='','start_datetime'='','calls'=0}
        """

        # Open the database
        with TinyDB(stats_db) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(stats_table)
            qy = Query()

            # Insert the new ticker info
            stats_table_obj.insert({'data_source': conn_info['data_source'],
                                    'conn_name': conn_info['conn_name'],
                                    'start_datetime': conn_info['start_datetime'],
                                    'start_datetime_epoch': dp.parse(conn_info['start_datetime']).timestamp(),
                                    'calls': conn_info['calls']})

            h.print_timestamped_text('Stats for [{}] inserted.'.format(conn_info['conn_name']))


    @staticmethod
    def fetch_dl_stats_for_connection(stats_db, stats_table, data_source, conn_name):
        """Returns counter information of previous download sessions"""
        # Create output container
        output_dict = {}

        # Fetch the limits for the given data_source, to use in the query for getting the download stats.
        limits = conf.get_ds_limits(data_source)

        # Calculate epoch times for day and hour limits
        ts_day = (datetime.now() - timedelta(hours=24, minutes=1)).timestamp()
        ts_hour = (datetime.now() - timedelta(minutes=1)).timestamp()

        # Open the database
        with TinyDB(stats_db) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(stats_table)
            qy = Query()
            res = stats_table_obj.search(Query()['data_source'] == data_source &
                                         qy['conn_name'] == conn_name &
                                         qy['start_datetime_epoch'] < ts_day)

            if len(res) == 0:
                return {'data_source': data_source, 'conn_name': conn_name, 'calls': 0}
            elif len(res) == 1:
                calls_made = 0
                return {'data_source': data_source, 'conn_name': conn_name, 'calls': calls_made}
            else:






        try:
            header_skipped = 0
            threshold_dt = datetime.now() - timedelta(hours=hours_back, minutes=1)
            with open(stats_file, 'r') as fl:
                lines = fl.readlines()
                for line in lines:
                    # Skip first line and last empty line
                    if header_skipped == 0 or len(line.strip()) == 0:
                        header_skipped = 1
                        continue
                    cols = line.split(',')
                    # evaluate the new datetime and sum the calls
                    t_index = int(cols[0].rstrip())
                    t_new = dp.parse(cols[1])
                    nr_new = int(cols[2].rstrip())

                    if threshold_dt < t_new:
                        if t_index in output_dict:
                            t_old = dp.parse(output_dict[t_index][0])
                            last_date = t_old if t_old < t_new else t_new
                            sum_calls = output_dict[t_index][1] + nr_new
                            output_dict[t_index] = [last_date.isoformat(), sum_calls]
                        else:
                            output_dict[t_index] = [cols[1], nr_new]
            print('---dl_stats dictionary created---')
        except:
            print('Error: no dl_stats dictionary could be made...')
            output_dict = {}

        if len(output_dict) == 0:
            for i in range(0, len(config.api_key_list_single)):
                output_dict[i] = ['2000-01-01T00:00:00.000000', 0]

        return output_dict

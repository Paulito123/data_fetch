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
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
from df_helpers import DfHelpers
from tinydb import TinyDB, Query
from tickers import Tickers

import dateutil.parser as dp
import urllib.request as request
import os.path
import shutil
import time

class DownloadStats:

    #todo:
    # - Method that adds latest dl stats
    # - Method that returns calls left for a key


    @staticmethod
    def append_dl_stats(stats_file_path, api_key_index, date, nr_of_calls):
        """Append a line to dl_stats file"""
        try:
            with open(stats_file_path, 'a') as fd:
                fd.write('{},{},{}'.format(api_key_index, date, nr_of_calls))
            print("---Stats written to stats file---")
            return True
        except:
            print("Error: cannot write to stats file.")
            return False

    @staticmethod
    def fetch_dl_stats(stats_file, hours_back):
        """Returns counter information of previous download sessions"""
        output_dict = {}
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

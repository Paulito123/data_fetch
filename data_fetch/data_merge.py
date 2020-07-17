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
import pandas as pd
from pathlib import Path
import time


class DataPlakker:

    def __init__(self, root_dir, config):
        self._config = config
        self.root_dir = root_dir

    def merge_files_by_symbol(self, symbol, index_column_name, data_directory, interval, target_directory):
        '''Merge multiple files into one by symbol.'''
        search_string = symbol + '_' + interval +  r'_*'
        dfs = [pd.read_csv(p, parse_dates=[index_column_name]) for p in Path(data_directory).glob(search_string)]
        df = pd.concat(dfs).drop_duplicates(index_column_name).set_index(index_column_name).sort_index()

        min_date = time.strftime('%Y%m%d', time.localtime(int(df.index.min())))
        max_date = time.strftime('%Y%m%d', time.localtime(int(df.index.max())))

        fq_filename = target_directory + '/' + symbol + '_' + interval + '_' + min_date + '_' + max_date
        df.to_csv(fq_filename, encoding='utf-8', index=False)


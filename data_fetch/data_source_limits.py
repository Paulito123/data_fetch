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
from data_fetch.helpers import Helpers as h
from tinydb import TinyDB, Query


class DSLimits:

    def __init__(self, root_dir, config):
        self._config = config
        self.root_dir = root_dir

    def get_ds_limits(self, data_source):
        """
        Get limits for a specific data_source. Returns a dict like:
        {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0, 'minute_limit': 0}
        """
        # Local variables
        path_db = self.root_dir + '/' + self._config['path_db']
        table_name = self._config['table_name']

        # Open the database
        with TinyDB(path_db) as db:
            config_table = db.table(table_name)
            res = config_table.search(Query()['data_source'] == data_source)

            if len(res) == 1:
                return res[0]
            else:
                return {'data_source': data_source, 'day_limit': 0, 'hour_limit': 0, 'minute_limit': 0}

    def set_ds_limits(self, data_source_dict):
        """
        Set limits opposed for a specific data_source. Takes a dict data_source_dict like:
        {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0, 'minute_limit': 0}
        """
        # Local variables
        path_db = self.root_dir + '/' + self._config['path_db']
        table_name = self._config['table_name']

        # Open the database
        with TinyDB(path_db) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(table_name)
            qy = Query()

            # Insert the new ticker info
            stats_table_obj.upsert({'data_source': data_source_dict['data_source'],
                                    'day_limit': data_source_dict['day_limit'],
                                    'hour_limit': data_source_dict['hour_limit'],
                                    'minute_limit': data_source_dict['minute_limit']},
                                   qy['data_source'] == data_source_dict['data_source'])

        h.print_timestamped_text('Limits updated for [{}].'.format(data_source_dict['data_source']))
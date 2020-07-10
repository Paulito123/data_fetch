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
import os.path


class Configuration:

    def __init__(self, db_file, db_name):
        self._db_file = db_file
        self._db_name = db_name
        self.main_config = self.load_main_config()

    def load_main_config(self):
        """Return a dict with key-value pairs for the main configuration."""
        with TinyDB(self._db_file) as cdb:
            ctable = cdb.table(self._db_name)
            out = {}
            for c in ctable:
                out[c['context']] = c['value']
        return out

    def get_keys_for_data_source(self, data_source):
        """Get a list of keys for a given data source from the configuration database."""

        # List to be filled and returned
        out = []

        # Check if ticker file exists.
        if not os.path.isfile(self.main_config['path_data_sources_db']):
            h.print_timestamped_text(
                "Error: database file [{}] does not exist.".format(
                    self.main_config['path_data_sources_db']
                )
            )
            return out

        # Query the DB for the requested keys
        with TinyDB(self.main_config['path_data_sources_db']) as db:
            config_table = db.table(self.main_config['tabnm_data_sources'])
            qy = Query()
            res = config_table.search(qy["data_source"] == data_source)

        if len(res) > 0:
            for c in res:
                if c is not None:
                    out.append(c)

        return out

    def get_ds_limits(self, data_source):
        """
        Get limits for a specific data_source. Returns a dict like:
        {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0, 'minute_limit': 0}
        """

        # Open the database
        with TinyDB(self.main_config['path_ds_limits_db']) as db:
            config_table = db.table(self.main_config['tabnm_ds_limits'])
            qy = Query()
            res = config_table.search(qy.data_source == data_source)

            if len(res) == 0:
                return {'data_source': data_source, 'day_limit': 0, 'hour_limit': 0, 'minute_limit': 0}
            elif len(res) == 1:
                return res[0]
            else:
                return {'data_source': data_source, 'day_limit': 0, 'hour_limit': 0, 'minute_limit': 0}


    @staticmethod
    def set_ds_limits(self, data_source_dict):
        """
        Set limits opposed for a specific data_source. Takes a dict data_source_dict like:
        {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0, 'minute_limit': 0}
        """

        # Open the database
        with TinyDB(self.main_config['path_ds_limits_db']) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(self.main_config['tabnm_ds_limits'])
            qy = Query()

            # Insert the new ticker info
            stats_table_obj.upsert({'data_source': data_source_dict['data_source'],
                                    'day_limit': data_source_dict['day_limit'],
                                    'hour_limit': data_source_dict['hour_limit'],
                                    'minute_limit': data_source_dict['minute_limit']},
                                   qy['data_source'] == data_source_dict['data_source'])

        h.print_timestamped_text('Limits updated for [{}].'.format(data_source_dict['data_source']))

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
from df_helpers import DfHelpers as h
from tinydb import TinyDB, Query
import os.path


class Configuration:

    @staticmethod
    def get_keys_for_data_source(data_source, db_file='config_db.json', db_name='config_db'):
        """Get a list of keys for a given data source from the configuration database."""

        # List to be filled and returned
        out = []

        # Check if ticker file exists.
        if not os.path.isfile(db_file):
            h.print_timestamped_text("Error: database file [{}] does not exist.".format(db_file))
            return out

        # Query the DB for the requested keys
        with TinyDB(db_file) as db:
            config_table = db.table(db_name)
            qy = Query()
            res = config_table.search(qy.data_source == data_source)

        if len(res) > 0:
            for c in res:
                if c is not None:
                    out.append(c)

        return out


    @staticmethod
    def get_ds_limits(data_source, db_file='config_db.json', db_name='limits_db'):
        """
        Get limits opposed for a specific data_source. Returns a dict like:
        {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0}
        """

        # Open the database
        with TinyDB(db_file) as db:
            config_table = db.table(db_name)
            qy = Query()
            res = config_table.search(qy.data_source == data_source)

            if len(res) == 0:
                return {'data_source': data_source, 'day_limit': 0, 'hour_limit': 0}
            elif len(res) == 1:
                return res
            else:
                return {'data_source': data_source, 'day_limit': 0, 'hour_limit': 0}


    @staticmethod
    def set_ds_limits(data_source_dict, db_file='config_db.json', db_name='limits_db'):
        """
        Set limits opposed for a specific data_source. Takes a dict data_source_dict like:
        {'data_source'='XXXXX','day_limit'=0,'hour_limit'=0}
        """

        # Open the database
        with TinyDB(db_file) as db:
            # Open the ticker table and create query object
            stats_table_obj = db.table(db_name)
            qy = Query()

            # Insert the new ticker info
            stats_table_obj.upsert({'data_source': data_source_dict['data_source'],
                                    'day_limit': data_source_dict['day_limit'],
                                    'hour_limit': data_source_dict['hour_limit']},
                                   qy['data_source'] == data_source_dict['data_source'])

        h.print_timestamped_text('Limits updated for [{}].'.format(data_source_dict['data_source']))

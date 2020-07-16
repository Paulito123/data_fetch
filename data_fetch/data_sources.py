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
from data_fetch.helpers import Helpers as h
from tinydb import TinyDB, Query
import os.path
import time


class DataSource:
    def __init__(self, root_dir, config):
        self._config = config
        self.root_dir = root_dir

    def get_active_data_source(self):
        """Get a list of keys for a given data source from the configuration database."""

        # local variables
        out = []
        path_db = self.root_dir + '/' + self._config['path_db']

        # Check if ticker file exists.
        if not os.path.isfile(path_db):
            h.print_timestamped_text(
                "Error: database file [{}] does not exist.".format(
                    self._config['path_db']
                )
            )
            return out

        # Query the DB for the requested keys
        with TinyDB(path_db) as db:
            config_table = db.table(self._config['table_name'])
            res = config_table.search(Query()["enabled"] == 1)

        if len(res) > 0:
            for c in res:
                if c is not None:
                    out.append(c)

        return out

    def get_data_source_by_name(self, name):
        """Get a list of keys for a given data source from the configuration database."""

        # local variables
        path_db = self.root_dir + '/' + self._config['path_db']

        # Check if ticker file exists.
        if not os.path.isfile(path_db):
            h.print_timestamped_text(
                "Error: database file [{}] does not exist.".format(
                    self._config['path_db']
                )
            )
            return []

        # Query the DB for the requested keys
        with TinyDB(path_db) as db:
            ds_table = db.table(self._config['table_name'])
            return ds_table.search((Query()["data_source"] == name) and (Query()["enabled"] == 1))

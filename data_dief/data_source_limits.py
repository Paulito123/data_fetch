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
from data_dief.helpers import Helpers as h
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
        if self._config.has_option(data_source, 'day_limit') and \
           self._config.has_option(data_source, 'hour_limit') and \
           self._config.has_option(data_source, 'minute_limit'):
            return {'day_limit': int(self._config[data_source]['day_limit']),
                    'hour_limit': int(self._config[data_source]['hour_limit']),
                    'minute_limit': int(self._config[data_source]['minute_limit'])}
        else:
            return {'day_limit': 0, 'hour_limit': 0, 'minute_limit': 0}

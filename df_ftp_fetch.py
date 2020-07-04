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
from contextlib import closing
import urllib.request as request
import os.path
import shutil


class FTPFetch:

    @staticmethod
    def fetch_nq_ticker_file(self, ftp_path, nasdaq_file):
        """check if nasdaq file exists, if not, fetch it"""

        try:
            if not os.path.isfile(nasdaq_file):
                with closing(request.urlopen(ftp_path)) as r:
                    with open(nasdaq_file, 'wb') as f:
                        shutil.copyfileobj(r, f)

            print('---nasdaq file fetched---')
            return True

        except:
            print('Error: nasdaq file not fetched!')
            return False

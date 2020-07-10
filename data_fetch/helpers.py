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
from datetime import datetime, timedelta
import time


class Helpers:

    @staticmethod
    def print_timestamped_text(text=""):
        """Prints a timestamped string."""

        dto = datetime.now()
        print('[{}] '.format(dto) + text)

    @staticmethod
    def sleep_handler(nr_of_secs=1):
        """Sleeps for a number of seconds, while printing the remaining number of seconds, every 5th seconds."""

        print("Sleeping {} sec > ".format(nr_of_secs), end="", flush=True)
        for x in range(nr_of_secs, 0, -1):
            time.sleep(1)
            if x % 5 == 0:
                print(".{}".format(x), end="", flush=True)
        print('Wake up!')

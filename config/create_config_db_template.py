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
# ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt
# ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt
#
###############################################################################

from tinydb import *

db_file = 'config_db.json'
db_name = 'config_db'

with TinyDB(db_file) as db:
    config_db = db.table(db_name)
    config_db.insert({
        'name': 'av1',
        'data_source': 'alpha_vantage',
        'login': 'email1@gmail.com',
        'key': 'XXXXXXXXXX',
        'secret': 'XXXXXXXXXX'})
    config_db.insert({
        'name': 'av2',
        'data_source': 'alpha_vantage',
        'login': 'email2@gmail.com',
        'key': 'XXXXXXXXXX',
        'secret': 'XXXXXXXXXX'})

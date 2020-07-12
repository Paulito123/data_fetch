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

from tinydb import *
import configparser

db_file = '../database/config_db.json'
db_name = 'data_sources'

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

db_name = 'limits'

with TinyDB(db_file) as db:
    config_db = db.table(db_name)
    config_db.upsert({
        'data_source': 'alpha_vantage',
        'day_limit': 500,
        'hour_limit': 0,
        'minute_limit': 5},
        Query()['data_source'] == 'alpha_vantage')

main_config_file = '../config/data_fetch.conf'
config = configparser.ConfigParser()
config['download_stats'] = {'path_db': 'database/dl_stats_db.json',
                            'table_name': 'dl_stats'}
config['data_source'] = {'path_db': 'database/data_sources_db.json',
                         'table_name': 'data_sources'}
config['ds_limits'] = {'path_db': 'database/data_sources_db.json',
                       'table_name': 'limits'}
config['tickers'] = {'path_db': 'database/tickers_db.json',
                     'table_name': 'tickers',
                     'path_ftp': 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt',
                     'path_file': 'datafiles/nasdaqlisted.txt'}
config['application'] = {'genesis_date': '2000-01-01T00:00:00.000000',
                         'days_before_refresh': '30',
                         'interval': '1min',
                         'data_output_path': 'datafiles'}
with open(main_config_file, 'w') as configfile:
   config.write(configfile)

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

db_name = 'main_prod'

with TinyDB(db_file) as db:
    config_db = db.table(db_name)

    config_db.upsert({
        'context': 'path_dl_stats_db',
        'description': 'The fq file name of the download statistics database.',
        'type': 'string',
        'value': '/database/dl_stats_db.json'}, Query()['context'] == 'path_dl_stats_db')
    config_db.upsert({
        'context': 'tabnm_dl_stats',
        'description': 'The name of the download statistics table.',
        'type': 'string',
        'value': 'dl_stats'}, Query()['context'] == 'tabnm_dl_stats')
    config_db.upsert({
        'context': 'path_data_sources_db',
        'description': 'The fq file name of the data sources database.',
        'type': 'string',
        'value': '/database/data_sources_db.json'}, Query()['context'] == 'path_data_sources_db')
    config_db.upsert({
        'context': 'tabnm_data_sources',
        'description': 'The name of the data sources table.',
        'type': 'string',
        'value': 'data_sources'}, Query()['context'] == 'tabnm_data_sources')
    config_db.upsert({
        'context': 'path_ticker_db',
        'description': 'The fq file name of the ticker database.',
        'type': 'string',
        'value': '/database/tickers_db.json'}, Query()['context'] == 'path_ticker_db')
    config_db.upsert({
        'context': 'tabnm_tickers',
        'description': 'The name of the tickers table.',
        'type': 'string',
        'value': 'tickers'}, Query()['context'] == 'tabnm_tickers')

    config_db.upsert({
        'context': 'path_nasdaq_ticker_ftp',
        'description': 'The fq ftp path to the main nasdaq ticker file.',
        'type': 'string',
        'value': 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'}, Query()['context'] == 'path_nasdaq_ticker_ftp')
    config_db.upsert({
        'context': 'path_local_nasdaq_ticker_file',
        'description': 'The fq path to the local nasdaq ticker file.',
        'type': 'string',
        'value': '/datafiles/nasdaqlisted.txt'}, Query()['context'] == 'path_local_nasdaq_ticker_file')

    config_db.upsert({
        'context': 'genesis_date',
        'description': 'A date used for initializations.',
        'type': 'datetime',
        'value': '2000-01-01T00:00:00.000000'}, Query()['context'] == 'genesis_date')

import shutil
import urllib.request as request
from contextlib import closing

''' Fetch ticker information from nasdaq '''
with closing(request.urlopen('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt')) as r:
    with open('data_files/nasdaqlisted.txt', 'wb') as f:
        shutil.copyfileobj(r, f)

with closing(request.urlopen('ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt')) as r:
    with open('data_files/otherlisted.txt', 'wb') as f:
        shutil.copyfileobj(r, f)





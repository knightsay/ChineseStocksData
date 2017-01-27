import tushare as ts
import pandas as pd
import numpy as np
import os
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta


# This function downloads the stock returns we want
# If start = None, then use the time-to-market date (entire history)
# frequency: {'D': daily, 'W': week, 'M': month, '5': 5 minutes, '15': 15 minutes,
#             '30': 30 minutes, '60': 60 minutes}

def download_stock_returns(start     = str(date.today() - timedelta(1)),
                           end       = str(date.today()),
                           frequency = 'D',
                           path      = 'C:/work/ChinaStocks/'):

    os.chdir(path)
    assert(end <= str(date.today()))
    
    print('-----------------------------------------------------')
    print('Downloading all stock codes...')
    stock_info = ts.get_stock_basics()
    # codes      = stock_info.index
    codes = list(np.load('to_downloade.npy'))
    counter    = len(codes)
        
    # loop through each stock
    for code in codes:

        name = stock_info.ix[code]['name']
        
        # make sure the stock is actually on the market now
        if stock_info.ix[code]['timeToMarket'] != 0:
            print('-------')
            print('Downloading returns data for ' + name + '...')
            ipo_date = datetime.strptime(str(stock_info.ix[code]['timeToMarket']), '%Y%m%d')

            # if start = None, we will download all returns data for this stock
            if start == None:                
                downloaded = ts.get_k_data(code, start = ipo_date.strftime('%Y-%m-%d'),
                                              end = end, ktype = frequency)
            else:
                downloaded = ts.get_k_data(code, start = start, end = end, ktype = frequency)
                
            downloaded.to_csv('data/stock_returns/' + str(code) + '_' +
                              str(frequency) + '.csv', index = False)
            counter = counter - 1
            print(str(counter) + ' to go...\n')
        else:
            counter = counter - 1
            
    print('-----------------------------------------------------')
    print('All done!')

download_stock_returns(start = None, end = '2017-01-15')

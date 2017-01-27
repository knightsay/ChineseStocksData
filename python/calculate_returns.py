import tushare as ts
import numpy as np
import pandas as pd
import os

def calculate_returns(y):

    if 'ret_0_1_d' in y.columns.values:
        return y
    
    # create daily, weekly, monthly, quarterly, bi-annually, and annually returns
    y['ret_0_1_d']   = (y.close - y.close.shift(1))   / y.close.shift(1)
    y['ret_0_5_d']   = (y.close - y.close.shift(5))   / y.close.shift(5)
    y['ret_0_21_d']  = (y.close - y.close.shift(21))  / y.close.shift(21)
    y['ret_0_62_d']  = (y.close - y.close.shift(62))  / y.close.shift(62)
    y['ret_0_123_d'] = (y.close - y.close.shift(123)) / y.close.shift(123)
    y['ret_0_246_d'] = (y.close - y.close.shift(246)) / y.close.shift(246)

    # round to save some space
    y['ret_0_1_d']   = y['ret_0_1_d'].round(5)
    y['ret_0_5_d']   = y['ret_0_5_d'].round(5)
    y['ret_0_21_d']  = y['ret_0_21_d'].round(5)
    y['ret_0_62_d']  = y['ret_0_62_d'].round(5)
    y['ret_0_123_d'] = y['ret_0_123_d'].round(5)
    y['ret_0_246_d'] = y['ret_0_246_d'].round(5)
    
    # create moving averages
    y['ma5']   = y['close'].rolling(center = False, min_periods = 3,
                                    window = 5).mean().values
    y['ma21']  = y['close'].rolling(center = False, min_periods = 11,
                                    window = 21).mean().values
    y['ma62']  = y['close'].rolling(center = False, min_periods = 31,
                                    window = 62).mean().values
    y['ma123'] = y['close'].rolling(center = False, min_periods = 62,
                                    window = 123).mean().values
    y['ma246'] = y['close'].rolling(center = False, min_periods = 123,
                                    window = 246).mean().values  

    y['ma5']   = y['ma5'].round(5)
    y['ma21']  = y['ma21'].round(5)
    y['ma62']  = y['ma62'].round(5)
    y['ma123'] = y['ma123'].round(5)
    y['ma246'] = y['ma246'].round(5)

    return(y)


os.chdir('C:/work/ChinaStocks/data/stock_returns/')
files = ['sz_composite_D.csv', 'sh_composite_D.csv']
counter = len(files)
for f in files:
    print('---------------------------------------')
    print('Processing returns for ' + f + '...')
    x = pd.read_csv(f, dtype = {'code': object})
    x = calculate_returns(x)
    x.to_csv(f, encoding = 'utf-8', index = False)
    counter = counter - 1
    print(str(counter) + ' to go...')

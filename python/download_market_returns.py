import pandas as pd
import numpy as np
import tushare as ts
import os

def download_market_returns(path = 'C:/work/ChinaStocks/'):
    os.chdir(path)
    # download Sh composite and Sz composite, Husheng 300, Zhong zheng 500
    sh_composite = ts.get_k_data('000001', index = True)
    sz_composite = ts.get_k_data('399106', index = True)
    hs300        = ts.get_k_data('000300', index = True)
    zz500        = ts.get_k_data('000905', index = True)

    # download risk-free rates: shibor and 
    

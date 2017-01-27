import tushare as ts
import numpy as np
import pandas as pd
import os
from collections import Counter

def download_stock_basics_info(path = 'C:/work/ChinaStocks/'):
    os.chdir(path)
    print('-----------------------------------------------------')
    print('Downloading stocks basic information...')
    stock_info = ts.get_stock_basics()
    
    # want to make rowname (code) as a column too
    stock_info.reset_index(inplace = True)
    
    # create new column 'exch' (exchange) based on code
    codes = stock_info.code
    names = stock_info.name
    stock_info['exch'] = 'Hushi'
    stock_info.loc[stock_info['code'].str.startswith('300'), 'exch'] = 'Chuangye'
    stock_info.loc[stock_info['code'].str.startswith('000'), 'exch'] = 'Shenshi'
    stock_info.loc[stock_info['code'].str.startswith('002'), 'exch'] = 'Zhongxiao'
    stock_info.to_csv('data/stock_info.csv', index = False, encoding = 'utf-8')
    
    print('-----------------------------------------------------')
    print('Downloading stock industries...')
    stock_industries = ts.get_industry_classified().rename(columns = {'c_name': 'industry'})
    stock_industries = stock_industries.groupby(['code', 'name']).industry.unique().reset_index()
    stock_industries.to_csv('data/stock_industries.csv', index = False, encoding = 'utf-8')
    
    print('\n-----------------------------------------------------')
    print('Downloading stock concepts...')
    stock_concepts = ts.get_concept_classified().rename(columns = {'c_name': 'concept'})
    stock_concepts = stock_concepts.groupby(['code', 'name']).concept.unique().reset_index()
    stock_concepts.to_csv('data/stock_concepts.csv', index = False, encoding = 'utf-8')
    
    print('\n-----------------------------------------------------')
    print('Downloading components of HuShen300...')
    hs300 = ts.get_hs300s()
    hs300.to_csv('data/hs300s.csv', index = False, encoding = 'utf-8')
    hs300_names = hs300.name.values
    
    print('-----------------------------------------------------')
    print('Downloading components of ZhongZheng500...')
    zz500 = ts.get_zz500s()
    zz500.to_csv('data/zz500s.csv', index = False, encoding = 'utf-8')    
    zz5_names = zz500.name.values

    # mark which stocks are in hs300 and zz500
    stock_info['in_hs300'] = stock_info['name'].isin(hs300.name.values).values
    stock_info['in_zz500'] = stock_info['name'].isin(zz500.name.values).values
    
    # Join these into one bigger data frame
    print('-----------------------------------------------------')
    print('Merging all downloaded data...')
    stock_info = pd.merge(stock_info, stock_industries, how = 'left',
                          on = ['code', 'name'])
    stock_info = pd.merge(stock_info, stock_concepts, how = 'left',
                          on = ['code', 'name'])
    stock_info.to_csv('data/all_stock_info.csv', index = False, encoding = 'utf-8')
    
    print('-----------------------------------------------------')
    print('All done!\n')
    

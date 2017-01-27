import numpy as np
import pandas as pd
import tushare as ts
import os
from datetime import date, timedelta

def download_financial_data(path = 'C:/work/ChinaStocks/'):
    os.chdir(path)

    # 1. download the dividend and profit data
    all_dividends = pd.DataFrame([])
    for year in range(2003, date.today().year):
        print('----------- Getting dividends data for ' + str(year) + '----------')
        try:
            div = ts.profit_data(year = year, top = 5000)
        except Exception as e:
            print(str(e))
        if not div.empty:
            pd.concat([all_dividends, div], ignore_index = True)

    all_dividends.to_csv('data/financials/dividends.csv',
                         index = False, encoding = 'utf-8')
    
    # 2. download earnings forecast (we can use this to see how good these forecasts are)
    all_earn_forecast = pd.DataFrame([])
    for year in range(2003, date.today().year):
        print('\n----------- Getting earnings forecast data for ' +
              str(year) + '----------')
        for quarter in range(1, 5):
            try:            
                earn_forecast = ts.forecast_data(year = year, quarter = quarter)
                print('')
            except Exception as e:
                print(str(e))

            if not earn_forecast.empty:
                earn_forecast['year']    = year
                earn_forecast['quarter'] = quarter
                all_earn_forecast = pd.concat([all_earn_forecast, earn_forecast], ignore_index = True)
                
    all_earn_forecast.to_csv('data/financials/earnings_forecast.csv',
                             index = False, encoding = 'utf-8')

    
    # 3. download xian shou gu data (and see if its imminence actually drives prices down)
    all_xsg = pd.DataFrame([])
    for year in range(2003, date.today().year):
        print('\n----------- Getting Xian Shou Gu data for ' +
              str(year) + '----------')
        for month in range(1, 13):
            xsg = pd.DataFrame([])
            try:            
                xsg = ts.xsg_data(year = year, month = month)
            except Exception as e:
                print(str(e))

            if not xsg.empty:
                xsg['year']  = year
                xsg['month'] = month
                all_xsg = pd.concat([all_xsg, xsg], ignore_index = True)
                
    all_xsg.to_csv('data/financials/xsg.csv',
                             index = False, encoding = 'utf-8')


    # 4. download fund holdings data for each stock
    all_fund = pd.DataFrame([])
    for year in range(2003, date.today().year):
        print('\n----------- Getting fund holdings data for ' +
              str(year) + '----------')
        for quarter in range(1, 5):
            fund = pd.DataFrame([])
            try:            
                fund = ts.fund_holdings(year = year, quarter = quarter)
                print('')
            except Exception as e:
                print(str(e))

            if not fund.empty:
                fund['year']  = year
                fund['quarter'] = quarter
                all_fund = pd.concat([all_fund, fund], ignore_index = True)
                
    all_fund.to_csv('data/financials/fund_holdings.csv',
                             index = False, encoding = 'utf-8')



    # 5. download new IPO stocks (to research if they keep climbing or not given their statistics)
    new = ts.new_stocks()
    new.to_csv('data/new_stocks.csv', index = False, encoding = 'utf-8')

    # 6. download margin leverage and short sales dollar amounts for each stock on Shanghai exchange
    # This might indicate buying belief or shorting needs
    sh_margin = pd.DataFrame([])
    for year in range(2003, 2018):
        print('Download Shanghai margin data for year' + str(year))
        margin = ts.sh_margin_details(start = str(year) + '-01-01',
                                         end   = str(year) + '-12-31')
        if not margin.empty:
            sh_margin = pd.concat([sh_margin, margin], ignore_index = True)            
            
    sh_margin.to_csv('data/financials/sh_margin_details.csv', index = False, encoding = 'utf-8')

    # 7. download margin for Shenzhen exchange
    for year in range(2017, date.today().year + 1):
        sz_margin = pd.DataFrame([])
        print('Download Shenzhen margin for year ' + str(year))        
        start = date(year, 1, 1)
        end   = date(year, 1, 26)
        while start <= end:
            print(str(start))
            margin = ts.sz_margin_details(str(start))
            if not margin.empty:
                sz_margin = pd.concat([sz_margin, margin], ignore_index = False)
            start = start + timedelta(1)
        sz_margin.to_csv('data/financials/sz_margin_details' + str(year) + '.csv',
                         index = False, encoding = 'utf-8')

    
download_financial_data()

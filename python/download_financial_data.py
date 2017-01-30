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
                sz_margin = pd.concat([sz_margin, margin], ignore_index = True)
            start = start + timedelta(1)
        sz_margin.to_csv('data/financials/sz_margin_details' + str(year) + '.csv',
                         index = False, encoding = 'utf-8')

    # 8. download report data for each quarter
    all_reports = pd.DataFrame([])
    for year in range(2003, date.today().year):
        year_report = pd.DataFrame([])
        print('-------------------------------------------')
        print('Download report data for year ' + str(year))
        for quarter in range(1, 5):
            report = ts.get_report_data(year, quarter)
            report['year'] = year
            report['quarter'] = quarter
            if not report.empty:
                year_report = pd.concat([year_report, report], ignore_index = True)
            print('')
        if not year_report.empty:
            year_report.to_csv('data/financials/report_' + str(year) + '.csv', index = False, encoding = 'utf-8')
            all_reports = pd.concat([all_reports, year_report], ignore_index = True)
    if not all_reports.empty:
        all_reports.to_csv('data/financials/report.csv', index = False, encoding = 'utf-8')
    

    # 9. download profit data for each quarter
    all_profits = pd.DataFrame([])
    for year in range(2003, date.today().year):
        year_profit = pd.DataFrame([])
        print('-------------------------------------------')
        print('Download profit data for year ' + str(year))
        for quarter in range(1, 5):
            profit = ts.get_profit_data(year, quarter)
            profit['year'] = year
            profit['quarter'] = quarter
            if not profit.empty:
                year_profit = pd.concat([year_profit, profit], ignore_index = True)
            print('')
        if not year_profit.empty:
            year_profit.to_csv('data/financials/profit_' + str(year) + '.csv', index = False, encoding = 'utf-8')
            all_profits = pd.concat([all_profits, year_profit], ignore_index = True)
    if not all_profits.empty:
        all_profits.to_csv('data/financials/profit.csv', index = False, encoding = 'utf-8')
    
    # 10. download operation data for each quarter
    all_operations = pd.DataFrame([])
    for year in range(2003, date.today().year):
        year_operation = pd.DataFrame([])
        print('-------------------------------------------')
        print('Download operation data for year ' + str(year))
        for quarter in range(1, 5):
            operation = ts.get_operation_data(year, quarter)
            operation['year'] = year
            operation['quarter'] = quarter
            if not operation.empty:
                year_operation = pd.concat([year_operation, operation], ignore_index = True)
            print('')
        if not year_operation.empty:
            year_operation.to_csv('data/financials/operation_' + str(year) + '.csv', index = False, encoding = 'utf-8')
            all_operations = pd.concat([all_operations, year_operation], ignore_index = True)
    if not all_operations.empty:
        all_operations.to_csv('data/financials/operation.csv', index = False, encoding = 'utf-8')
    
    # 11. download growth data for each quarter
    all_growths = pd.DataFrame([])
    for year in range(2003, date.today().year):
        year_growth = pd.DataFrame([])
        print('-------------------------------------------')
        print('Download growth data for year ' + str(year))
        for quarter in range(1, 5):
            growth = ts.get_growth_data(year, quarter)
            growth['year'] = year
            growth['quarter'] = quarter
            if not growth.empty:
                year_growth = pd.concat([year_growth, growth], ignore_index = True)
            print('')
        if not year_growth.empty:
            year_growth.to_csv('data/financials/growth_' + str(year) + '.csv', index = False, encoding = 'utf-8')
            all_growths = pd.concat([all_growths, year_growth], ignore_index = True)
    if not all_growths.empty:
        all_growths.to_csv('data/financials/growth.csv', index = False, encoding = 'utf-8')
    
    # 12. download debt-paying data for each quarter
    all_debtpayings = pd.DataFrame([])
    for year in range(2003, date.today().year):
        year_debtpaying = pd.DataFrame([])
        print('-------------------------------------------')
        print('Download debt-paying data for year ' + str(year))
        for quarter in range(1, 5):
            debtpaying = ts.get_debtpaying_data(year, quarter)
            debtpaying['year'] = year
            debtpaying['quarter'] = quarter
            if not debtpaying.empty:
                year_debtpaying = pd.concat([year_debtpaying, debtpaying], ignore_index = True)
            print('')
        if not year_debtpaying.empty:
            year_debtpaying.to_csv('data/financials/debtpaying_' + str(year) + '.csv', index = False, encoding = 'utf-8')
            all_debtpayings = pd.concat([all_debtpayings, year_debtpaying], ignore_index = True)
    if not all_debtpayings.empty:
        all_debtpayings.to_csv('data/financials/debtpaying.csv', index = False, encoding = 'utf-8')
    
    # 13. download cashflow data for each quarter
    all_cashflows = pd.DataFrame([])
    for year in range(2011, date.today().year):
        year_cashflow = pd.DataFrame([])
        print('-------------------------------------------')
        print('Download cashflow data for year ' + str(year))
        for quarter in range(1, 5):
            cashflow = ts.get_cashflow_data(year, quarter)
            cashflow['year'] = year
            cashflow['quarter'] = quarter
            if not cashflow.empty:
                year_cashflow = pd.concat([year_cashflow, cashflow], ignore_index = True)
            print('')
        if not year_cashflow.empty:
            year_cashflow.to_csv('data/financials/cashflow_' + str(year) + '.csv', index = False, encoding = 'utf-8')
            all_cashflows = pd.concat([all_cashflows, year_cashflow], ignore_index = True)
    if not all_cashflows.empty:
        all_cashflows.to_csv('data/financials/cashflow.csv', index = False, encoding = 'utf-8')
        
download_financial_data()

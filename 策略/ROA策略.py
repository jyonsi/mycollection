# 克隆自聚宽文章：https://www.joinquant.com/post/33211
# 标题：坚持ROA策略，你就是下一个巴菲特
# 作者：yyxs2020

# 克隆自聚宽文章：https://www.joinquant.com/post/33211
# 标题：坚持ROA策略，你就是下一个巴菲特
# 作者：yyxs2020

import pandas as pd
import numpy as np
import time
import datetime
from jqdata import*


def initialize(context):
    run_daily(period,'every_bar')
    g.stocksnum = 10
    g.period = 1
    g.day = 0
    g.idxwarn=True
    g.signal=None

def paused_filter(security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if not current_data[stock].paused]
    return security_list

def delisted_filter(security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if not '退' in current_data[stock].name]
    return security_list

def st_filter(security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if not current_data[stock].is_st]
    return security_list
    
def get_stock(context):
    year = context.current_dt.year
    print(year)
    sc = get_index_stocks('000001.XSHG') + get_index_stocks('399001.XSHE')
    sc_filter_pd = paused_filter(sc)
    sc_filter_pdst = st_filter(sc_filter_pd)
    stocks = delisted_filter(sc_filter_pdst)
    q = query(indicator.code,indicator.roa).filter(indicator.code.in_(stocks))
    
    #for i in range(1,5):
    
    df1 = get_fundamentals(q,statDate=year-4)
    df2 = get_fundamentals(q,statDate=year-3)
    df3 = get_fundamentals(q,statDate=year-2)
    df4 = get_fundamentals(q,statDate=year-1)

    df1.index = df1.code
    df1 = df1.drop('code',axis = 1)
    df2.index = df2.code
    df2 = df2.drop('code',axis = 1)
    df3.index = df3.code
    df3 = df3.drop('code',axis = 1)
    df4.index = df4.code
    df4 = df4.drop('code',axis = 1)
    
    df = pd.concat([df1,df2,df3,df4],axis = 1)

    new_col = ['roa1', 'roa2','roa3','roa4']
    df.columns = new_col

    df = df[df['roa1']>20]
    df = df[df['roa2']>20]
    df = df[df['roa3']>20]
    df = df[df['roa4']>20]
    
    dff = df.index.tolist()
    q1 = get_fundamentals(query(valuation.code,valuation.market_cap).filter(indicator.code.in_(dff)).order_by(valuation.market_cap.desc()))
    buylist = list(q1['code'][:g.stocksnum])
    return buylist


def period(context):
    if g.day%g.period == 0:
        stock = get_stock(context)
        print(stock)

        for i in context.portfolio.positions:
           if i not in stock or get_signal(context)==False:
              order_target(i,0)
              print('%s is not in stock' % i)
        if len(stock)==0:
            Mvalue=0
        else:
            Mvalue = context.portfolio.available_cash/len(stock)
        #Mvalue = context.portfolio.available_cash/5
        print(Mvalue)      

        for i in stock:
            if i not in context.portfolio.positions and get_signal(context)==True:
                print('%s is in stock' % i)
                order_value(i,Mvalue)

    print(context.portfolio.positions)
    g.day = g.day + 1







def get_signal(context):
    if MA_closeprice_jincha('000300.XSHG', 70) or MA_avgline_jincha('000300.XSHG', 70, 90):
        g.signal = True
        print('沪深300前1日收盘价大于70日均线，或者70日均线大于90日均线，可调仓')
    elif MA_avgline_xiafang('000300.XSHG', 70, 90) and MA_closeprice_xiafang('000300.XSHG', 70):
        g.signal = False
        print('沪深300 70日均线大于前1日收盘价，且90日均线大于70日均线时，要清仓')
    return g.signal
# 昨日收盘价与均线金叉
def MA_closeprice_jincha(stock, n): 
    hist = attribute_history(stock, n+2, '1d', 'close', df=False) 
    ma1 = mean(hist['close'][-n:]) 
    ma2 = mean(hist['close'][-(n+1):-1]) 
    close1 = hist['close'][-1] 
    close2 = hist['close'][-2] 
    if (close2 < ma2) and (close1 >= ma1): 
        return True

# 昨日收盘价小于均线
def MA_closeprice_xiafang(stock, n):
    hist = attribute_history(stock, n+2, '1d', 'close', df=False)
    ma1 = mean(hist['close'][-n:]) 
    close1 = hist['close'][-1]
    if close1 < ma1:
        return True

# 短期均线与长期均线金叉
def MA_avgline_jincha(stock, n, m):  
    closePrice = attribute_history(stock, m+2,'1d','close') 
    avgMaS = closePrice.close[-n:].mean() 
    avgMaSp = closePrice.close[-(n+1):-1].mean() 
    avgMaL = closePrice.close[-m:].mean() 
    avgMaLp = closePrice.close[-(m+1):-1].mean() 
    if avgMaS >= avgMaL and avgMaSp < avgMaLp:
        return True

# 短期均线小于长期均线
def MA_avgline_xiafang(stock, n, m):  
    closePrice = attribute_history(stock, m+2,'1d','close') 
    avgMaS = closePrice.close[-n:].mean() 
    avgMaL = closePrice.close[-m:].mean() 
    if avgMaS < avgMaL: 
        return True

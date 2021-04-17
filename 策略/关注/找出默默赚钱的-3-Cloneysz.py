# 克隆自聚宽文章：https://www.joinquant.com/post/31254
# 标题：2020年年化293%回撤13% 找出默默赚钱的第三版
# 作者：苦咖啡

import csv
import os
import xlrd
import math
import jqdata
import numpy as np 
import pandas as pd
from six import BytesIO
from jqlib.alpha101 import *
from pandas import DataFrame,Series
from jqdata import *
from kuanke.wizard import *

# 初始化程序, 整个回测只运行一次
def initialize(context):
    g.index_security = '000300.XSHG'
    # 设定沪深300作为基准
    set_benchmark(g.index_security)
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    before_market_open(context)
    g.filter_paused = True
    g.max_hold_stocknum = 3
    g.day1 = 180
    g.day2 = 120
    g.day3 = 60
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003,close_commission=0.0003, min_commission=5), type='stock')

    # run_daily(risk_control,'open') 
    run_monthly(before_market_open,1,time='before_open', reference_security=g.index_security) 
    run_weekly(market_open,1,time='9:35', reference_security='000300.XSHG')
    

def avg_volume(security,timeperiod):
    avg_volume = attribute_history(security, timeperiod, '1d', ['volume'])['volume'].mean()
    return avg_volume
    

## 开盘时运行函数
def market_open(context):
    #卖出不在买入列表中的股票

    g.buy_lists = []
    g.buy_list = []
    g.buy_list2 = []
    g.buy_list1 = []
      
            
    a = query(valuation.code).filter(valuation.code.in_(g.stocks_exsit),valuation.market_cap > 1000,valuation.pe_ratio > 15,valuation.pe_ratio < 70)
    g.buy_list2  = list(get_fundamentals(a).code)
    g.close4 = history(g.day1,'1d','close',g.buy_list2)
    
    
    test = '000338.XSHE'
    #arr_low = max_high_3_parts(test, g.day1)
    #h = attribute_history(test, g.day1, '1d', ('close'))
    #log.info('%s ：%s - %s - %s :%s : %s'%(get_security_info(test).display_name,arr_low[0],arr_low[1],arr_low[2],h['close'][0]/h['close'][-1],max_high(test,g.day3,0,g.day3-1)/h['close'][-1]))
 
    for security in (g.buy_list2):
        arr_low = max_high_3_parts(security, g.day1)
        if 0.75<arr_low[0]/arr_low[1]< 1\
        and 0.75<arr_low[1]/arr_low[2] < 1:
            if  0.75 > g.close4[security][0]/g.close4[security][-1] > 0.5:
                if max_high(security,g.day3,0,g.day3-1)/g.close4[security][-1]<1.15\
                and g.close4[security][-1] < 500:
                    g.buy_list1.append(security)
                    log.info('%s ：%s - %s - %s :%s : %s'%(get_security_info(security).display_name,arr_low[0],arr_low[1],arr_low[2],g.close4[security][0]/g.close4[security][-1],max_high(security,g.day3,0,g.day3-1)/g.close4[security][-1]))
    

    a = query(valuation.code).filter(valuation.code.in_(g.buy_list1)).order_by(valuation.circulating_market_cap.desc()).limit(g.max_hold_stocknum)
    g.buy_list  = list(get_fundamentals(a).code)
  
    
    
    hold_stock = list(context.portfolio.positions.keys())
    for security in g.buy_list:
        #买不在持股列表中的股票
        #log.info('%s ：%s：%s：%s'%(security,get_security_info(security).display_name,max_high(security,20,0,10)))
        if security  not in hold_stock:
             g.buy_lists.append(security)
    
    if len(g.buy_list):
        log.info('___'*10)
        log.info('股票池：%d '%(len(g.buy_list)))
        log.info(g.buy_list)       

    sell(context,g.buy_list)
    #买入不在持仓中的股票，按要操作的股票平均资金
    buy(context,g.buy_lists)
#交易函数 - 买入
def buy(context, buy_lists):
    # 获取最终的 buy_lists 列表
    log.info('buy')

            
    Num = len(buy_lists)
    # buy_lists = buy_lists[:Num]
    # 买入股票

    if len(buy_lists)>0:
        # 分配资金
        cash = context.portfolio.total_value
        #cash = context.portfolio.available_cash
        amount = cash/Num
        #result = order_style(context,buy_lists,len(buy_lists), g.order_style_str, g.order_style_value)
        hold_stock = list(context.portfolio.positions.keys())
        for stock in buy_lists:
            if stock  in hold_stock:
  
                log.info(stock + get_security_info(stock).display_name +' 购买资金：' +str(amount)  + ' 剩余资金：' + str(cash) +  ' 热度：' +  str(avg_volume(stock,7)/avg_volume(stock,180)))
                order_target_value(stock,amount)
                #order_value(stock,amount)
        for stock in buy_lists:
            if stock  not in hold_stock:
  
                log.info(stock + get_security_info(stock).display_name +' 购买资金：' +str(amount)  + ' 剩余资金：' + str(cash) +  ' 热度：' +  str(avg_volume(stock,7)/avg_volume(stock,180)))
                order_target_value(stock,amount)

    return

       
# 交易函数 - 出场
def sell(context, buy_lists):
    # 获取 sell_lists 列表
    log.info('sell')
    hold_stock = list(context.portfolio.positions.keys())
    for s in hold_stock:
        #卖出不在买入列表中的股票
        if s not in buy_lists:
            log.info(s  + get_security_info(s).display_name + ' sell')
            order_target_value(s,0)   


    



def before_market_open(context):
    g.stocks_exsit = get_industry_stocks('HY001') + get_industry_stocks('HY002') \
            + get_industry_stocks('HY003') + get_industry_stocks('HY004') \
            + get_industry_stocks('HY005') + get_industry_stocks('HY006') \
            + get_industry_stocks('HY008') \
            + get_industry_stocks('HY009') + get_industry_stocks('HY010') \
            + get_industry_stocks('HY496') + get_industry_stocks('HY497') \
            + get_industry_stocks('HY501') 
    g.stocks_exsit = set(filter_special(context,g.stocks_exsit))  #当天上市的所有股票，过滤了ST等
    # g.stocks_exsit =get_stock_list(context)
###------------------------------------------------------------------------------  

    
def max_high(security,time_period,start,end):
    return attribute_history(security, time_period, '1d', ['high'], df=False)['high'][start:end].max()

    
def min_low(security, time_period, start, end):
    return attribute_history(security, time_period, '1d', ['low'], df=False)['low'][start:end].min()

def max_high_3_parts(security, time_period):
    # type: (str, int) -> (float, float, float)
    """
    将过去的time_period天分为3段，返回3段各自low的最大值
    例如将过去180天分为3段，返回过去180-120天，过去120-60天，过去60-0天的low的最大值
    """
    arr_low = attribute_history(security, time_period, '1d', ['high'], df=False)['high']
    step = time_period // 3
    return arr_low[0 * step:1 * step].max(), arr_low[1 * step:2 * step].max(), arr_low[2 * step:3 * step].max()
    
def filter_special(context,stock_list):# 过滤器，过滤停牌，ST，科创，新股
    curr_data = get_current_data()

    #stock_list=[stock for stock in stock_list if stock[0:3] != '688'] #过滤科创板'688' 
    stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
    stock_list = [stock for stock in stock_list if not curr_data[stock].paused] 
    # stock_list = [stock for stock in stock_list if 'ST' not in curr_data[stock].name]
    # stock_list = [stock for stock in stock_list if '*'  not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if '退' not in curr_data[stock].name]
    # stock_list = [stock for stock in stock_list if  curr_data[stock].day_open>1]
    stock_list = [stock for stock in stock_list if  (context.current_dt.date()-get_security_info(stock).start_date).days>150]

    return   stock_list 
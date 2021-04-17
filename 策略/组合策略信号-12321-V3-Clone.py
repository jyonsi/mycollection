# 克隆自聚宽文章：https://www.joinquant.com/post/31379
# 标题：15年1500倍，年化收益60%，实盘直播中
# 作者：曹经纬

# -*- coding:utf-8 -*-
from kuanke.wizard import *
from jqdata import *
import numpy as np
import pandas as pd
import talib
import time
import math
import urllib.request
import requests
import json
import uuid

from csv import DictReader
from sys import version_info
from six import StringIO

#----------+----------+----------+----------+----------+----------+----------+----------+
g_auth_code = '6985C4E4-5EA2-401A-A038-E4C6D51028A2-07DD8CBA-C695-4F21-8B52-9B87BE6B2F5F'

g_max_turnover = 1.0 # 最大仓位
g_http_url = ['http://47.115.155.199:12321']
g_strategy_name = '组合策略信号-12321-V3'
g_log_path = 'log/%s.log' % g_strategy_name
g_signal_path = 'signal/组合策略信号-%s.csv' % g_strategy_name
g_original_signal_path = 'signal/%s-original-signal.csv' % g_strategy_name
g_last_signal_path = 'signal/%s-lasts-original.csv' % g_strategy_name
g_max_hold_stocknum = 10
g_show_account_info = False


class ReqOrderInfo():
    def __init__(self):
        self.action = ''
        self.strategy_id = 'joinquant_stock_small_value'
        self.symbol = ''
        self.side = 'long' # long short
        self.type = 'buy' # sell buy
        self.price = 0.0
        self.time = '1990-01-01 00:00:00'
        self.balance_rate = 0.1
        
class QuoteInfo():
    def __init__(self):
        self.symbol = ''
        self.price = 0.0
        self.time = '1990-01-01 00:00:00'

class ReqQuoteInfo():
    def __init__(self):
        self.action = ''
        self.quotes = []
        
class ReqHeldOrderInfo():
    def __init__(self):
        self.action = ''
        self.time = '1990-01-01 00:00:00'
        self.orders = []

## 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    write_file(g_log_path, '', append=False)
    write_file(g_signal_path, 'id,status,symbol,time,type,trend,price,balance_rate,try_count,order_id,strategy_id\n', append=False)
    write_file(g_original_signal_path, 'id,status,symbol,time,type,trend,price,balance_rate,try_count,order_id,strategy_id\n', append=False)
    
    # 设定基准
    set_benchmark('000001.XSHG')
    # 设定滑点
    set_slippage(FixedSlippage(0.01))
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True)
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    

    
def after_code_changed(context):    
    # 个股最大持仓比重
    g.security_max_proportion = 1
    # 选股频率
    g.check_stocks_refresh_rate = 1
    # 买入频率
    g.buy_refresh_rate = 1
    # 卖出频率
    g.sell_refresh_rate = 1
    # 最大建仓数量
    g.max_hold_stocknum = g_max_hold_stocknum

    # 选股频率计数器
    g.check_stocks_days = 0
    # 买卖交易频率计数器
    g.buy_trade_days=0
    g.sell_trade_days=0
    # 获取未卖出的股票
    g.open_sell_securities = []
    # 卖出股票的dict
    g.selled_security_list={}
    # 有风险的股票
    g.realtime_held_securities = ['000001.XSHE']

    # 出场初始化函数
    sell_initialize()
    # 入场初始化函数
    buy_initialize()
    # 风控初始化函数
    risk_management_initialize()

    # 关闭提示
    log.set_level('order', 'info')
    
    g.init_time = string_to_datetime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    signals = read_csv(g_last_signal_path)
    g.held_stocks = [signal['symbol'] for signal in signals if -2 <= (context.current_dt - string_to_datetime(signal['time'])).days < 90]
    log.info(context.current_dt, 'held order: ', g.held_stocks)
    
    g.buy_stock_count = 5
    g.http_cookie = '%s#%s' % (str(uuid.uuid1()), datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    g.is_first_realtime_event = True
    g.max_balance = 0.0

    timer_event_get_new_order(context)


    unschedule_all()    
    if g_show_account_info:
        run_daily(timer_show_account_info, time='14:42', reference_security='000001.XSHG')
    
    run_daily(timer_event_get_held_order, time='14:42', reference_security='000001.XSHG')    
    
    
    
def timer_show_account_info(context):
    vbalance = context.portfolio.total_value
    g.max_balance = max(g.max_balance, vbalance)
    drawdown = int((g.max_balance - vbalance) / g.max_balance * 100)
    turnover = int((context.portfolio.total_value - context.portfolio.available_cash) / context.portfolio.total_value * 100)
    record(动态回撤=drawdown, 仓位比例=turnover)
    #record(动态回撤=drawdown)
    
    symbols = [position.security for position in list(context.portfolio.positions.values())]
    write_log('%s 仓位[%5.2f%%] 回撤[%.2f%%] 持仓：%d %r' % (datetime_to_string(context.current_dt), turnover, drawdown, len(symbols), symbols))
    
    for position in list(context.portfolio.long_positions.values()):  
        days = max(1, (position.transact_time - position.init_time).days)
        turnover = position.value / context.portfolio.total_value
        profit = (position.value - position.avg_cost * position.total_amount) / (position.avg_cost * position.total_amount) / days
        write_log('%s %s 仓位[%.2f%%] 价值[%.2f %.2f] 浮动盈亏[%.2f%%] 建仓时间[%s] 最后时间[%s]' % (datetime_to_string(context.current_dt), position.security, turnover * 100,
            position.value, position.avg_cost * position.total_amount, profit * 100, datetime_to_string(position.init_time), datetime_to_string(position.transact_time)))

    
def timer_event_get_new_order(context):
    if get_last_price(context, '000001.XSHG') < 0.0:
        return        
    
    request = ReqOrderInfo()
    request.action = 'order'
    request.cookie = g.http_cookie
    request.time = datetime_to_string(context.current_dt)
    request.auth = g_auth_code
    
    orders = []
    for url in g_http_url:
        orders.extend(get_order_from_http_server(context, request, url))
        
    for vorder in orders:
        if vorder.type == 'buy':
            if vorder.symbol not in g.held_stocks:
                g.held_stocks.append(vorder.symbol)
        if vorder.type == 'sell':
            if vorder.symbol in g.held_stocks:
                del g.held_stocks[g.held_stocks.index(vorder.symbol)]
    
    if ((g.init_time - context.current_dt).days <= 90):
        update_last_signal(context, g.held_stocks)
    
    g.check_out_lists = g.held_stocks
    if len(g.realtime_held_securities) > 0:
        g.check_out_lists = g.held_stocks
    else:
        g.check_out_lists = []
        
    adjust_position(context)
    
    # TODO test
    for vorder in orders:
        vclose = get_last_price(context, vorder.symbol)
        write_original_signal('123653,done,%s,%s,%s,%s,%.2f,%.2f,%d,%s,%s' % (vorder.symbol, datetime_to_string(context.current_dt), vorder.type, 'long', vclose, 0.1, 1, 'XYZ', 'XYZ'))


def timer_event_get_held_order(context):
    if get_last_price(context, '000001.XSHG') < 0.0:
        return
    
    request = ReqOrderInfo()
    request.action = 'order'
    request.cookie = g.http_cookie
    request.time = datetime_to_string(context.current_dt)
    request.auth = g_auth_code
    
    orders = []
    for url in g_http_url:
        orders.extend(get_held_order_from_http_server(context, request, url))
        
    held_symbols = [vorder.symbol for vorder in orders]
    log.info(context.current_dt, 'held order: ', held_symbols)
    write_log('%s held order[%d]' % (datetime_to_string(context.current_dt), len(held_symbols)))
    
    if ((g.init_time - context.current_dt).days <= 90):
        update_last_signal(context, held_symbols)
    
    g.check_out_lists = held_symbols
    adjust_position(context)


## 出场初始化函数
def sell_initialize():
    # 设定是否卖出buy_lists中的股票
    g.sell_will_buy = False

    # 固定出仓的数量或者百分比
    g.sell_by_amount = None
    g.sell_by_percent = None

## 入场初始化函数
def buy_initialize():
    # 是否可重复买入
    g.filter_holded = True

    # 委托类型
    g.order_style_str = 'by_cap_mean'
    g.order_style_value = 100

## 风控初始化函数
def risk_management_initialize():
    # 策略风控信号
    g.risk_management_signal = True

    # 策略当日触发风控清仓信号
    g.daily_risk_management = True

    # 单只最大买入股数或金额
    g.max_buy_value = None
    g.max_buy_amount = None


## 卖出未卖出成功的股票
def sell_every_day(context):
    if get_last_price(context, '000001.XSHG') < 0.0:
        return
    
    g.open_sell_securities = list(set(g.open_sell_securities))
    open_sell_securities = [s for s in context.portfolio.positions.keys() if s in g.open_sell_securities]
    if len(open_sell_securities)>0:
        for stock in open_sell_securities:
            order_target_value(stock, 0)
            
            vclose = get_last_price(context, stock)
            write_signal('123653,done,%s,%s,%s,%s,%.2f,%.2f,%d,%s,%s' % (stock, datetime_to_string(context.current_dt), 'sell', 'long', vclose,
                0.1, 1, 'XYZ', 'XYZ'))
            
    g.open_sell_securities = [s for s in g.open_sell_securities if s in context.portfolio.positions.keys()]
    return


# 股票筛选
def check_stocks(context):
    #g.check_out_lists = get_held_order(context)
    
    # 计数器归一
    g.check_stocks_days = 1
    return

## 交易函数
def trade(context):
    if get_last_price(context, '000001.XSHG') < 0.0:
        return
    
   # 初始化买入列表
    buy_lists = []

    # 买入股票筛选
    if g.buy_trade_days%g.buy_refresh_rate == 0:
        # 获取 buy_lists 列表
        buy_lists = g.check_out_lists
            

    # 卖出操作
    if g.sell_trade_days%g.sell_refresh_rate != 0:
        # 计数器加一
        g.sell_trade_days += 1
    else:
        # 卖出股票
        sell(context, buy_lists)
        # 计数器归一
        g.sell_trade_days = 1


    # 买入操作
    if g.buy_trade_days%g.buy_refresh_rate != 0:
        # 计数器加一
        g.buy_trade_days += 1
    else:
        # 卖出股票
        buy(context, buy_lists)
        # 计数器归一
        g.buy_trade_days = 1
        
    
    g.is_first_realtime_event = False
    
    
def adjust_position(context):
    if get_last_price(context, '000001.XSHG') < 0.0:
        return
    
    buy_lists = g.check_out_lists
    sell(context, buy_lists)
    buy(context, buy_lists)

    g.is_first_realtime_event = False
    

## 卖出股票日期计数
def selled_security_list_count(context):
    g.daily_risk_management = True
    if len(g.selled_security_list)>0:
        for stock in g.selled_security_list.keys():
            g.selled_security_list[stock] += 1
            
            
## 是否可重复买入
def holded_filter(context, security_list):
    if not g.filter_holded:
        security_list = [stock for stock in security_list if stock not in context.portfolio.positions.keys()]
    # 返回结果
    return security_list            
            
            
## 卖出股票加入dict
def selled_security_list_dict(context, security_list):
    selled_sl = [s for s in security_list if s not in context.portfolio.positions.keys()]
    if len(selled_sl)>0:
        for stock in selled_sl:
            g.selled_security_list[stock] = 0            


# 交易函数 - 出场
def sell(context, buy_lists):
    # 获取 sell_lists 列表
    init_sl = context.portfolio.positions.keys()
    sell_lists = context.portfolio.positions.keys()

    # TODO modify
    # 判断是否卖出buy_lists中的股票
    if not g.sell_will_buy:
        sell_lists = [security for security in sell_lists if security not in buy_lists]


    # 卖出股票
    if len(sell_lists)>0:
        for stock in sell_lists:
            sell_by_amount_or_percent_or_none(context, stock, g.sell_by_amount, g.sell_by_percent, g.open_sell_securities)
            
            vclose = get_last_price(context, stock)
            write_signal('123653,done,%s,%s,%s,%s,%.2f,%.2f,%d,%s,%s' % (stock, datetime_to_string(context.current_dt), 'sell', 'long', vclose,
                0.1, 1, 'XYZ', 'XYZ'))

    # 获取卖出的股票, 并加入到 g.selled_security_list中
    selled_security_list_dict(context,init_sl)

    return

# 交易函数 - 入场
def buy(context, buy_lists):
    # 判断是否可重复买入
    buy_lists = holded_filter(context, buy_lists)
    
    # 买入股票
    for stock in buy_lists:
        if len(context.portfolio.positions) < g.max_hold_stocknum:
            # 获取资金
            value = context.portfolio.total_value / g.max_hold_stocknum
            # 判断单只最大买入股数或金额
            amount = max_buy_value_or_amount(stock, value * g_max_turnover, g.max_buy_value, g.max_buy_amount)
            #order(stock, amount, MarketOrderStyle())
            order_target(stock, amount, MarketOrderStyle())
                


#----------+----------+----------+----------+----------+----------+----------+----------+----------+  
class ReqOrderInfo(object):
    def __init__(self):
        self.action = ''
        self.auth = ''
        self.cookie = ''
        self.time = ''
        
        
class RspOrderInfo(object):
    def __init__(self):
        self.symbol = ''
        self.price_buy = 0.0
        self.price_sell = 0.0
        self.time = ''
        self.contract = ''
        self.strategy_id = ''
        self.order_id = ''
        self.type = 'buy'
        self.side = 'long'
        self.amount = 0
        self.turnover = 0.0
        self.enddate = datetime.date(1990, 1, 1)    
        
#----------+----------+----------+----------+----------+----------+----------+----------+----------+  
def get_order_from_http_server(context, request, url):
    httpclient = requests.session()
    url = '%s?action=get_order' % url
    vheaders = {'Content-Type': 'application/json'}
    vbody = json.dumps(request.__dict__)
    
    try:
        response = httpclient.post(url, headers=vheaders, data=vbody)
        response_dict_array = response.json()
    except:
        return []
    
    if response_dict_array is None:
        return []

    orders = []
    for response_dict in response_dict_array:
        vorder = RspOrderInfo()
        vorder.__dict__ = response_dict
        orders.append(vorder)
        
    return orders
    

def get_held_order_from_http_server(context, request, url):
    httpclient = requests.session()
    url = '%s?action=get_held_order' % url
    vheaders = {'Content-Type': 'application/json'}
    vbody = json.dumps(request.__dict__)
    
    try:
        response = httpclient.post(url, headers=vheaders, data=vbody)
        response_dict_array = response.json()
    except:
        return []
    
    if response_dict_array is None:
        return []

    orders = []
    for response_dict in response_dict_array:
        vorder = RspOrderInfo()
        vorder.__dict__ = response_dict
        orders.append(vorder)
        
    return orders        
    
#----------+----------+----------+----------+----------+----------+----------+----------+----------+  
def get_last_price(context, symbol):
    begintime = context.current_dt
    endtime = begintime
    hst = get_price(symbol, begintime, endtime, '1m', fields=None, skip_paused=True, fq='post', count=None)
    close_list = hst['close']
    if close_list is not None and len(close_list) >= 1:
        return close_list[-1]
    else:
        return -1.0
        
def get_historical_price(context, symbol, vdatetime):
    hst = get_price(symbol, vdatetime, vdatetime, '1m', fields=None, skip_paused=True, fq='post', count=None)
    close_list = hst['close']
    if close_list is not None and len(close_list) >= 1:
        return close_list[-1]
    else:
        return -1.0

#----------+----------+----------+----------+----------+
# 公共函数.写日志
def write_log(text, is_append=True):
    write_file(g_log_path, text + '\n', append=is_append)
    
def write_signal(text, is_append=True):
    write_file(g_signal_path, text + '\n', append=is_append)
    
def write_original_signal(text, is_append=True):
    write_file(g_original_signal_path, text + '\n', append=is_append)    
    
def update_last_signal(context, buy_symbol_list):
    text = 'time,symbol,type\n'
    write_file(g_last_signal_path, text, append=False)
    
    for symbol in buy_symbol_list:
        text = '%s,%s,buy\n' % (datetime_to_string(context.current_dt), symbol)
        write_file(g_last_signal_path, text, append=True)    

#----------+----------+----------+----------+----------+
# 公共函数.CSV文件读写
def get_remote_file_content(url):
    rsp = urllib.request.urlopen(url)
    text = rsp.read()
    return text

def read_csv(filename):
    try:    
        vdata = read_file(filename)
    except:
        return []

    buffer = StringIO()
    if version_info.major < 3:
        buffer.write(vdata)
    else:
        buffer.write(vdata.decode())

    buffer.seek(0)
    return list(DictReader(buffer))
    
def read_remote_csv(url):
    try:
        vdata = get_remote_file_content(url)
    except:
        return []

    buffer = StringIO()
    if version_info.major < 3:
        buffer.write(vdata)
    else:
        buffer.write(vdata.decode())

    buffer.seek(0)
    return list(DictReader(buffer))    
    
#----------+----------+----------+----------+----------+
# 公共函数
def datetime_to_string(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def datetime_to_datestring(dt):
    return dt.strftime("%Y-%m-%d")

def string_to_datetime(st):
    return datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")

def string_to_timestamp(st):
    return time.mktime(time.strptime(st, "%Y-%m-%d %H:%M:%S"))

def timestamp_to_string(sp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sp))

def datetime_to_timestamp(dt):
    return time.mktime(dt.timetuple())

def timestamp_to_struct(sp):
    return time.localtime(sp)
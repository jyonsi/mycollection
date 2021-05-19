# 克隆自聚宽文章：https://www.joinquant.com/post/29633
# 标题：穿越牛熊的价值蓝筹策略（霍华.罗斯曼投资法）
# 作者：ethology

# 克隆自聚宽文章：https://www.joinquant.com/post/13382
# 标题：穿越牛熊基业长青的价值精选策略
# 作者：拉姆达投资

# 克隆自聚宽文章：https://www.joinquant.com/post/29173
# 标题：价值投资策略-大盘择时
# 作者：叶松

# 克隆自聚宽文章：https://www.joinquant.com/post/29038
# 标题：价值投资改进版-5年9.5倍
# 作者：叶松

# 克隆自聚宽文章：https://www.joinquant.com/view/community/detail/94473000bdd77eb9efe2d8830540b3b5
# 标题：霍华．罗斯曼 大盘蓝筹成长股研究
# 作者：Gyro

# 20201120调整了持仓股票数5只；采用了资金列表差异法调仓；比较了每周调仓法与每月调仓+每日均衡法，效益接近；）
# 20201121 对比月调仓、周调仓以及是否均衡持仓，周调仓收益
#霍华．罗斯曼投资法如下：
#http://www.tej.com.tw/twsite/tejweb/tw/product/explain/T0308.htm

'''
投资程序：
霍华．罗斯曼强调其投资风格在于为投资大众建立均衡、且以成长为导向的投资组合。选股方式偏好大型股，
管理良好且为领导产业趋势，以及产生实际报酬率的公司；不仅重视公司产生现金的能力，也强调有稳定成长能力的重要。
总市值大于等于50亿美元。
良好的财务结构。
较高的股东权益报酬。
拥有良好且持续的自由现金流量。
稳定持续的营收成长率。
优于比较指数的盈余报酬率。
'''


# 导入函数库
from jqdata import *
from kuanke.wizard import *
import numpy as np
import pandas as pd
import datetime as dt



# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    #防治未来函数
    set_option("avoid_future_data", True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'debug')
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001,
                             open_commission=0.0003,
                             close_commission=0.0003,
                             min_commission=5),
                   type='stock')

    # 模式一：每月调仓，每日平衡
    # 开盘前运行
    run_monthly(before_market_open,1,time='before_open', reference_security='000300.XSHG')
    # 定时运行
    run_monthly(trade,1,'open', reference_security='000300.XSHG')
    # 持仓平衡
    run_daily (adjust_position,'14:30')
    # 每天结束后统计交易情况
    run_daily(after_market_close,'close')
    # 模式二： 每周调仓
    #run_weekly(before_market_open,1,time='before_open', reference_security='000300.XSHG')
    #run_weekly(trade,1,'open', reference_security='000300.XSHG')
    # 每周统计交易情况
    #run_weekly(after_market_close,1,'close')
def after_code_changed(context):
    # 模式一：每月调仓，每日平衡
    # 开盘前运行
    run_monthly(before_market_open,1,time='before_open', reference_security='000300.XSHG')
    # 定时运行
    run_monthly(trade,1,'open', reference_security='000300.XSHG')
    # 持仓平衡
    run_daily (adjust_position,'14:30')
    # 每天结束后统计交易情况
    run_daily(after_market_close,'close')
    
    
## 开盘前运行函数     
def before_market_open(context):
    #获取满足条件的股票列表
    check_out_lists = get_stock_list(context)
    # 过滤: 三停（停牌、涨停、跌停）及st,*st,退市
    check_out_lists = filter_st_stock(check_out_lists)
    check_out_lists = filter_limitup_stock(context, check_out_lists)
    check_out_lists = filter_paused_stock(check_out_lists)
    # 取需要的只数
    g.stock_list = get_check_stocks_sort(context,check_out_lists)
        
## 开盘时运行函数
def trade(context):
    # 买卖
    buy(context, g.stock_list)
    
# 交易
def buy(context, stocks):
    # 交易函数 - 出场
    current_data = get_current_data()
    # 获取 sell_lists 列表
    final = list(stocks)
    #log.info('北上资金持股排序%s',s_change_rank)
    current_hold_funds_set = set(context.portfolio.positions.keys())
    if set(final) != current_hold_funds_set:
        need_buy= set(final).difference(current_hold_funds_set)
        need_sell= current_hold_funds_set.difference(final)
        for stock in need_sell:
            order_target(stock, 0)
            log.info('调仓卖出%s', stock)
        cash_per_fund = context.portfolio.available_cash/len(need_buy)
        for stock in need_buy:
            if current_data[stock].last_price * 101 < cash_per_fund:
                order_value(stock,cash_per_fund)
                log.info('调仓买入%s', stock)
    

# 平衡持仓
def adjust_position(context):
    cur_data = get_current_data()
    current_hold_funds_set = set(context.portfolio.positions.keys())
    position = context.portfolio.total_value/len(current_hold_funds_set) # 每份头寸大小
    # buy or rebalance
    for s in current_hold_funds_set:
        delta = position - context.portfolio.positions[s].value
        if abs(delta) > max(0.1*position, 100*cur_data[s].last_price) and\
            context.portfolio.available_cash > delta and not cur_data[s].paused:
            log.info('buy', s, cur_data[s].name, int(delta))
            order_value(s, delta) # order_value(s,delta)
            #order_target_value(s,position)



def get_check_stocks_sort(context,check_out_lists):
    df = get_fundamentals(query(valuation.circulating_market_cap,valuation.pe_ratio,valuation.code,indicator.inc_return).filter(valuation.code.in_(check_out_lists)),date=context.previous_date)
    #按流通市值排序，asc值为0，从大到小
    #df = df.sort ('circulating_market_cap',ascending=0) #python2 
    df = df.sort_values ('circulating_market_cap',ascending=0) #python3
    #按净资产收益率率排序，asc值为0，从大到小
    #df = df.sort_values ('inc_return',ascending=0)
    #按动态PE排序，asc值为1，从小到大
    #df = df.sort_values ('pe_ratio',ascending=1)
    out_lists = list(df['code'].values)
    out_lists=out_lists[1:6]
    return out_lists

def get_stock_list(context):
    dt_now = context.previous_date #dt.datetime.now().date() - dt.timedelta(days=1)
    index = '000906.XSHG'
    stocks = get_index_stocks(index)
    df = get_fundamentals(query(
        valuation.code,
        valuation.market_cap,
        valuation.pb_ratio,
        valuation.pe_ratio,
        valuation.pcf_ratio,
        balance.total_current_assets,
        balance.total_current_liability,
        indicator.inc_return,
        indicator.inc_revenue_year_on_year,
        indicator.inc_net_profit_year_on_year,
    ).filter(
        valuation.code.in_(stocks),
    ), date=dt_now).dropna().set_index('code')
    df['current_ratio'] = df.total_current_assets / (df.total_current_liability + 1)
    mkt_current_ratio = df.current_ratio.median()
    #df['roe'] = 100 * df.pb_ratio / df.pe_ratio
    mkt_roe = df.inc_return.median()
    df = df[
    (df.current_ratio > mkt_current_ratio) & \
    (df.inc_return > mkt_roe) & \
    (5 < df.inc_revenue_year_on_year) & (df.inc_revenue_year_on_year < 50) & \
    (5 < df.inc_net_profit_year_on_year) & (df.inc_net_profit_year_on_year < 50) &\
    (df.pcf_ratio > 0)]
    stocks = list(df.index)
    dlist = [dt_now - dt.timedelta(days=i*365) for i in [2,1,0]] #取近3年自由现金流为正
    for d in dlist:
        _df = get_fundamentals(query(
        valuation.code,
        valuation.pcf_ratio,
        ).filter(
        valuation.code.in_(stocks),
        valuation.pcf_ratio > 0,
        ),date=d).dropna().set_index('code')
        stocks = list(_df.index) #此处缩进对回测影响结果影响巨大
    df = df.reindex(stocks)
    df = df[['market_cap', 'inc_return', 'pcf_ratio']]
    df['name'] = [get_security_info(s).display_name for s in df.index]
    
    stocks = list (df.index)
    
    return stocks


## 收盘后运行函数
def after_market_close(context):
    #得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：'+str(_trade))
    #log.info('一周结束')
    log.info('****'*15)

# 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]
# 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [
        stock for stock in stock_list if not current_data[stock].is_st and 'ST'
        not in current_data[stock].name and '*' not in current_data[stock].name
        and '退' not in current_data[stock].name
    ]
# 过滤涨停\跌停的股票
def filter_limitup_stock(context, stock_list):
    last_prices = history(1,
                          unit='1m',
                          field='close',
                          security_list=stock_list)
    current_data = get_current_data()
    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    return [
        stock for stock in stock_list
        if stock in context.portfolio.positions.keys()
        or last_prices[stock][-1] <= current_data[stock].high_limit
        or last_prices[stock][-1] >= current_data[stock].low_limit
        ]


'''
#均线 实测效果不佳，不用。
def judge_More_average(security):
    close_data = attribute_history(security, 5, '1d', ['close'])
    MA5 = close_data['close'].mean()
    close_data = attribute_history(security, 10, '1d', ['close'])
    MA10 = close_data['close'].mean()
    close_data = attribute_history(security, 20, '1d', ['close'])
    MA20 = close_data['close'].mean()
    close_data = attribute_history(security, 30, '1d', ['close'])
    MA30 = close_data['close'].mean()
    if MA5 < MA20 and MA10 < MA30:  #and MA20>MA30 :
        return True
    return False    
'''


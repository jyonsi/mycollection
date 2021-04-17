from jqdata import *
import numpy as np
from scipy.stats import linregress


# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    g.benchmark = '000300.XSHG'
    # 设定基准
    set_benchmark('000300.XSHG')
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True)
    set_option("avoid_future_data", True)
    # 关闭提示
    log.set_level('order', 'error')

    # 最大建仓数量
    g.max_hold_stock_nums = 2
    # 选出来的股票
    g.buylist = []
    g.selllist = []

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
    # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')
    # 收盘后选股
    run_daily(after_close_brain, time='21:00')
    

## 开盘前运行函数
def before_market_open(context):
    # 输出运行时间
    log.info('函数运行时间(before_market_open)：'+str(context.current_dt.time()))
    today_date = context.current_dt.date()
    # 获取最后一个交易日
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    pass

## 开盘时运行函数
def market_open(context):
    print('今天开始的buylist'+str(g.buylist))
    log.info('函数运行时间(market_open):'+str(context.current_dt.time()))
    current_data = get_current_data()
    today_date = context.current_dt.date()
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    
    sell(context,current_data,today_date)

    ##接着买的操作
    if context.portfolio.available_cash <100: #钱不够了
        return
    
    #分配资金，全仓只持两只
    # 如果不持有股票平均分配两只
    if context.portfolio.available_cash == context.portfolio.total_value:
        cash_perstk = context.portfolio.available_cash/3
    elif len(context.portfolio.positions) == 1:
        cash_perstk = context.portfolio.available_cash/2
    else:
        cash_perstk = context.portfolio.available_cash
            
    for stockcode in g.buylist:
        if current_data[stockcode].paused == True:
            continue
        if order_target_value(stockcode,cash_perstk) != None:
            log.info(str('%s,开盘买入,%s,%s\n' % (context.current_dt.time(),stockcode,current_data[stockcode].last_price)))
            # write_file('follow_log.csv', str('%s,开盘买入,%s,%s\n' % (context.current_dt.time(),stockcode,current_data[stockcode].last_price)),append = True)
    g.buylist=[]
    g.selllist=[]

def after_close_brain(context):
    log.info(str('函数运行时间(after_close_brain):'+str(context.current_dt.time())))
    ##0，预设阶段
    #得到今天的日期和数据
    today_date = context.current_dt.date()
    #前天
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    all_data = get_current_data()
    
    ##1，收集bench指数的股票列表，三除，按市值排序
    #三除
    stocklist = get_index_stocks(g.benchmark)
    stocklist = filter(context,stocklist,all_data)
    
    
    ##2，循环遍历指数列表，去除百日次新，通用条件过滤，行业条件过滤后形成买入信号，直接记录到brain和log中
    buysign(context)
    print('收盘的buylist'+str(g.buylist))
    sellsign(context)
    print('收盘的selllist'+str(g.selllist))
        
    return    


## 收盘后运行函数log
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    today_date = context.current_dt.date()
    
    for stk in context.portfolio.positions:
        cost = context.portfolio.positions[stk].avg_cost
        price = context.portfolio.positions[stk].price
        value = context.portfolio.positions[stk].value
        intime= context.portfolio.positions[stk].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        
        log.info('股票(%s)共有:%s,入时:%s,周期:%s,成本:%s,现价:%s,收益:%s' % (stk,value,intime,duration,cost,price,ret))
        # write_file('follow_log.csv', str('股票:%s,共有:%s,入时:%s,周期:%s,成本:%s,现价:%s,收益:%s\n' % (stk,value,intime,duration,cost,price,ret)),append = True)
        
    log.info('总资产:%s,持仓:%s' %(context.portfolio.total_value,context.portfolio.positions_value))
    # write_file('follow_log.csv', str('%s,总资产:%s,持仓:%s\n' %(context.current_dt.date(),context.portfolio.total_value,context.portfolio.positions_value)),append = True)

##########################################################################

# 买入逻辑
def buysign(context):
    # type: (Context) -> None
    current_data = get_current_data()

    # 沪深300成分股
    check_out_lists = get_index_stocks("000300.XSHG", date=context.previous_date)

    # 未停牌、未涨跌停、非科创板
    check_out_lists = [stock for stock in check_out_lists if
                       (not current_data[stock].paused) and
                       (current_data[stock].low_limit < current_data[stock].day_open < current_data[stock].high_limit) and
                       (not stock.startswith('688')
                        )]

    # 昨收盘价不高于500元/股
    s_close_1 = history(1, '1d', 'close', check_out_lists).iloc[-1]
    check_out_lists = list(s_close_1[s_close_1 <= 500].index)

    # 近30个交易日的最高价 / 昨收盘价 <=1.1, 即HHV(HIGH,30)/C[-1] <= 1.1
    high_max_30 = history(30, '1d', 'high', check_out_lists).max()
    s_fall = high_max_30 / s_close_1
    check_out_lists = list(s_fall[s_fall <= 1.1].index)

    # 近7个交易日的交易量均值 与 近180给交易日的成交量均值 相比，放大不超过1.5倍  MA(VOL,7)/MA(VOL,180) <=1.5
    df_vol = history(180, '1d', 'volume', check_out_lists)
    s_vol_ratio = df_vol.iloc[-7:].mean() / df_vol.mean()
    check_out_lists = list(s_vol_ratio[s_vol_ratio <= 1.5].index)

    # TODO

    g.buylist = list(df_score.index[:g.max_hold_stock_nums])

# 卖出信号
def sellsign(context):
    #得到今天的日期和数据
    today_date = context.current_dt.date()
    df_bench_price = get_price(g.benchmark, count = 2, end_date=today_date, frequency='daily', fields=['close'])
    # 个股基线收益
    rise_bench_today = (df_bench_price['close'].values[-1] - df_bench_price['close'].values[-2])/df_bench_price['close'].values[-2]  
    #大盘暴跌卧倒
    if rise_bench_today < -0.07:
        return

    #大盘暴跌卧倒
    if rise_bench_today < -0.07:
        return
    ##3，遍历持仓，给出卖信号
    for stockcode in context.portfolio.positions:
        cost = context.portfolio.positions[stockcode].avg_cost
        price = context.portfolio.positions[stockcode].price
        value = context.portfolio.positions[stockcode].value
        intime= context.portfolio.positions[stockcode].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        rise_ratio = ret/duration
    
        # 天数股票数据
        df_price = get_price(stockcode, count = 10, end_date=today_date, frequency='daily', fields=['close'])
        close_max = df_price['close'].max()
        last_price = df_price['close'].values[-1]
        # 最新比最高跌幅10%并且买入大于8天
        if last_price/close_max < 0.9 and duration >8:
            g.selllist.append(stockcode)
            log.info(str('%s,动损信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)))
            # write_file('follow_brain.csv',str('%s,sell,%s,DS\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            # write_file('follow_log.csv', str('%s,动损信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
        #大于5天收益为负
        if duration >=5 and ret <0:
            # 最后一天开始上涨
            if df_price['close'].values[-1]> df_price['close'].values[-2]:  #当天收阳过
                continue
            g.selllist.append(stockcode)
            log.info(str('%s,短亏信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)))
            # write_file('follow_brain.csv',str('%s,sell,%s,DK\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            # write_file('follow_log.csv', str('%s,短亏信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
        
        if duration >=10 and rise_ratio <0.0085:
            g.selllist.append(stockcode)
            log.info(str('%s,到期信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)))
            # write_file('follow_brain.csv',str('%s,sell,%s,DQ\n' % (today_date,stockcode)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            # write_file('follow_log.csv', str('%s,到期信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue

# 卖出逻辑
def sell(context,current_data,today_date):
    ##先是卖的操作，止盈，止损，卖信
    for stockcode in context.portfolio.positions:
        # 如果停牌跳过处理
        if current_data[stockcode].paused == True:
            continue
        # 持仓成本
        cost = context.portfolio.positions[stockcode].avg_cost
        # 最新价
        price = current_data[stockcode].last_price
        value = context.portfolio.positions[stockcode].value
        intime= context.portfolio.positions[stockcode].init_time
        # 增长率
        ret = price/cost - 1
        # 获取建仓到现在所有交易日
        duration=len(get_trade_days(intime,today_date))
        # 平均每个交易日增长率
        rise_ratio = ret/duration
        
        #创板股提高盈速要求
        # '688' '300'开头的股
        if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
            rise_ratio = rise_ratio/2
            ret = ret/2
        # 增长大于100%止盈
        if ret > 1:
            if order_target(stockcode,0) != None:
                log.info(str('%s,开盘止盈,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)))
                # write_file('follow_log.csv', str('%s,开盘止盈,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)),append = True)
                continue
        # 清仓
        if stockcode in g.selllist:
            if order_target(stockcode,0) != None:
                log.info(str('%s,开盘清仓,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)))
                # write_file('follow_log.csv', str('%s,开盘清仓,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)),append = True)
                continue

# 三除函数
def filter(context,stocklist,all_data):
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].paused]
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].is_st]
    stocklist = [stockcode for stockcode in stocklist if'退' not in all_data[stockcode].name]
    return stocklist




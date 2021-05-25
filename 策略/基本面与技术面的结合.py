# 克隆自聚宽文章：https://www.joinquant.com/post/33274
# 标题：基本面+技术面=年化49.59%！
# 作者：HaoG

import pandas as pd

'''
================================================================================
总体回测前
================================================================================
'''
#总体回测前要做的事情
def initialize(context):
    set_params()                             # 设置策略常量
    set_variables()                          # 设置中间变量
    set_backtest()                           # 设置回测条件

#1 
#设置策略参数
def set_params():
    g.tc = 3            # 调仓天数
    g.num_stocks = 32

#2
#设置中间变量
def set_variables():
    g.t = 0                                  # 记录回测运行的天数
    g.if_trade = False                       # 当天是否交易

#3
#设置回测条件
def set_backtest():
    set_option('use_real_price',True)        # 用真实价格交易
    log.set_level('order','error')           # 设置报错等级




'''
================================================================================
每天开盘前
================================================================================
'''
#每天开盘前要做的事情
def before_trading_start(context):
    if g.t%g.tc==0:
        g.if_trade=True                          # 每g.tc天，调仓一次
        set_slip_fee(context)                    # 设置手续费与手续费
        g.stocks=get_index_stocks('000001.XSHG') # 设置沪深300为初始股票池
        # 设置可行股票池
        g.feasible_stocks = set_feasible_stocks(g.stocks,context)
    g.t+=1
    
    
#4
# 设置可行股票池：过滤掉当日停牌的股票
# 输入：initial_stocks为list类型,表示初始股票池； context（见API）
# 输出：unsuspened_stocks为list类型，表示当日未停牌的股票池，即：可行股票池
def set_feasible_stocks(initial_stocks,context):
    # 判断初始股票池的股票是否停牌，返回list
    paused_info = []
    current_data = get_current_data()
    for i in initial_stocks:
        paused_info.append(current_data[i].paused)
    df_paused_info = pd.DataFrame({'paused_info':paused_info},index = initial_stocks)
    unsuspened_stocks =list(df_paused_info.index[df_paused_info.paused_info == False])
    return unsuspened_stocks


#5
# 根据不同的时间段设置滑点与手续费
# 输入：context（见API）
# 输出：none
def set_slip_fee(context):
    # 将滑点设置为0
    set_slippage(FixedSlippage(0)) 
    # 根据不同的时间段设置手续费
    dt=context.current_dt
    if dt>datetime.datetime(2013,1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5)) 
        
    elif dt>datetime.datetime(2011,1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))
            
    elif dt>datetime.datetime(2009,1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))
    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))




'''
================================================================================
每天交易时
================================================================================
'''
# 每天回测时做的事情
def handle_data(context,data):
    if g.if_trade == True:
        # 待买入的g.num_stocks支股票，list类型
        list_to_buy = stocks_to_buy(context)
        # 待卖出的股票，list类型
        # list_to_sell = stocks_to_sell(context, list_to_buy)
        # 卖出操作
        sell_operation(context, list_to_buy)
        # 买入操作
        buy_operation(context, list_to_buy)
    g.if_trade = False
    
#6
# 计算股票的PEG值
# 输入：context(见API)；stock_list为list类型，表示股票池
# 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
def get_PEG(context, stock_list): 
    # 查询股票池里股票的市盈率，收益增长率
    q_PE_G = query(valuation.code, valuation.pe_ratio, indicator.inc_net_profit_year_on_year
                 ).filter(valuation.code.in_(stock_list)) 
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G)
    # 筛选出成长股：删除市盈率或收益增长率为负值的股票
    df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio >0)&(df_PE_G.inc_net_profit_year_on_year >0)]
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()
    # 得到一个Series：存放股票的市盈率TTM，即PE值
    Series_PE = df_Growth_PE_G.ix[:,'pe_ratio']
    # 得到一个Series：存放股票的收益增长率，即G值
    Series_G = df_Growth_PE_G.ix[:,'inc_net_profit_year_on_year']
    # 得到一个Series：存放股票的PEG值
    Series_PEG = Series_PE/Series_G
    # 将股票与其PEG值对应
    Series_PEG.index = df_Growth_PE_G.ix[:,0]
    # 将Series类型转换成dataframe类型
    df_PEG = pd.DataFrame(Series_PEG)
    return df_PEG
    
#7
# 获得买入信号
# 输入：context(见API)
# 输出：list_to_buy为list类型,表示待买入的g.num_stocks支股票
def stocks_to_buy(context):
    list_to_buy = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG(context, g.feasible_stocks)
    # 将股票按PEG升序排列，返回daraframe类型
    try:
        df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    except AttributeError:
        df_sort_PEG = df_PEG.sort_values(by=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
    for i in range(g.num_stocks):
        if df_sort_PEG.ix[i,0] < 0.5:
            list_to_buy.append(df_sort_PEG.index[i])

    return list_to_buy
    
    
#8
# 获得卖出信号
# 输入：context（见API文档）, list_to_buy为list类型，代表待买入的股票
# 输出：list_to_sell为list类型，表示待卖出的股票
# def stocks_to_sell(context, list_to_buy):
#     list_to_sell=[]
#     # 对于不需要持仓的股票，全仓卖出
#     for stock_sell in context.portfolio.positions:
#         if stock_sell not in list_to_buy:
#             list_to_sell.append(stock_sell)
#     return list_to_sell


#9
# 执行卖出操作
# 输入：list_to_sell为list类型，表示待卖出的股票
# 输出：none
def sell_operation(context, list_to_buy):
    stocks_list = boll_volume(list_to_buy)
    stock_filtered = stocks_list[stocks_list[0]<stocks_list[1]].index.tolist()
    for stock_sell in stock_filtered:
        order_target_value(stock_sell, 0)


#10
# 执行买入操作
# 输入：context(见API)；list_to_buy为list类型，表示待买入的股票
# 输出：none
def buy_operation(context, list_to_buy):
    
    stocks_list = boll_volume(list_to_buy)
    stock_filtered = stocks_list[stocks_list[0]>stocks_list[2]].index.tolist()
    
    for stock_sell in stock_filtered:
        # 为每个持仓股票分配资金
        g.capital_unit=context.portfolio.portfolio_value/len(stock_filtered)
        # 买入在“待买股票列表”的股票
        for stock_buy in stock_filtered:
            order_target_value(stock_buy, g.capital_unit)

'''
================================================================================
布林通道筛选
================================================================================
'''
def boll_volume(list_to_buy):
    
    g.risk=0
    ######布林30日均线上下轨计算
    # clo30=history(list_to_buy,10'1d',('close'),skip_paused=True)
    clo30 = history(100,'1d',('close'),list_to_buy,skip_paused=True)
    clo_basic = clo30[list_to_buy].mean(axis=0)
    clo = 100*clo30[list_to_buy][-3:].mean()/clo_basic
    mid=100*round(np.mean(clo30[list_to_buy])/clo_basic,2)
    std=100*round(np.std(clo30[list_to_buy])/clo_basic,2)
    bollUp=mid+2*std
    bollDown=mid-2*std
    stocks_list = pd.concat([clo, bollDown,bollUp], axis=1)

    return stocks_list 






'''
================================================================================
每天收盘后
================================================================================
'''
# 每天收盘后做的事情
# 进行长运算（本策略中不需要）
def after_trading_end(context):
    return
    
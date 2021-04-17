# 克隆自聚宽文章：https://www.joinquant.com/post/16343
# 标题：基于机器学习的多因子选股策略
# 作者：quant.show

# 标题：基于机器学习的多因子选股的量化投资策略
# 作者：ChenXuan

import pandas as pd
import numpy as np
import math
import json
import jqdata
from jqfactor import standardlize
from jqfactor import winsorize_med
from jqdata import *
from sklearn.model_selection import KFold


def initialize(context):
    set_params()
    set_backtest()

##
def set_params():
    # 记录回测运行的天数
    g.days = 0
    # 当天是否交易
    g.if_trade = False                       

    ## 可变参数
    # 股票池
    g.secCode = '000985.XSHG'  #中证全指
    #g.secCode = '000300.XSHG'
    #g.secCode = '000905.XSHG' #中证500
    # 调仓天数
    g.refresh_rate = 30 
    ## 机器学习算法
    # 线性回归：lr
    # 岭回归：ridge
    # 线性向量机：svr
    # 随机森林：rf
    g.method = 'svr'
    
    ## 分组测试之用 ####
    # True:开启分组测试（g.stocknum失效,g.group有效，g.quantile有效）
    # False:关闭分组测试（g.stocknum有效，g.group有效，g.quantile失效）
    g.invest_by_group = False
    # 每组（占所有股票中的）百分比
    # g.group（MAX）* g.quantile = 1， 即包含全部分组
    g.quantile = 0.1
    # 分组
    # 第1组：1
    # 第2组：2
    # ... ...
    # 第n组：n
    g.group = 1
    # 持仓数（分组时失效）
    g.stocknum = 5

##
def set_backtest():
    set_benchmark('000985.XSHG')   #中证全指
    set_option('use_real_price', True)
    log.set_level('order', 'error')
    
## 保存不能被序列化的对象, 进程每次重启都初始化,
def process_initialize(context):
    
    # 交易次数记录
    g.__tradeCount = 0
    # 删除建仓日或者重新建仓日停牌的股票后剩余的可选股
    g.__feasible_stocks = [] 
    # 网格搜索是否开启
    g.__gridserach = False

    ## 机器学习验证集及测试集评分记录之用（实际交易策略中不需要，请设定为False）#####
    # True：开启（写了到研究模块，文件名：score.json）
    # False：关闭
    g.__scoreWrite = False
    g.__valscoreSum = 0
    g.__testscoreSum = 0
    ## 机器学习验证集及测试集评分记录之用（实际交易策略中不需要，请设定为False）#####

    # 训练集长度
    g.__trainlength = 4
    # 训练集合成间隔周期（交易日）
    g.__intervals = 21
    
    # 离散值处理列表
    g.__winsorizeList = ['log_NC', 'LEV', 'NI_p', 'NI_n', 'g', 'RD',
                            'EP','BP','G_p','PEG','DP',
                               'ROE','ROA','OPTP','GPM','FACR']
    
    # 标准化处理列表
    g.__standardizeList = ['log_mcap',
                        'log_NC', 
                        'LEV', 
                        'NI_p', 'NI_n', 
                        'g', 
                        'RD',
                        'EP',
                        'BP',
                        'G_p',
                        'PEG',
                        'DP',
                        'CMV',
                        'ROE',
                        'ROA',
                        'OPTP',
                        'GPM',
                        'FACR',
                        'CFP',
                        'PS']
                        
    # 聚宽一级行业
    g.__industry_set = ['HY001', 'HY002', 'HY003', 'HY004', 'HY005', 'HY006', 'HY007', 'HY008', 'HY009', 
          'HY010', 'HY011']
    
    '''
    # 因子列表（因子组合1）
    g.__factorList = [#估值
                    'EP',
                    #'BP',
                    #'PS',
                    #'DP',
                    'RD',
                    #'CFP',
                    #资本结构
                    'log_NC', 
                    'LEV', 
                    #'CMV',
                    #'FACR',
                    #盈利
                    'NI_p', 
                    'NI_n', 
                    #'GPM',
                    #'ROE',
                    #'ROA',
                    #'OPTP',
                    #成长
                    'PEG',
                    #'g', 
                    #'G_p',
                    #行业哑变量
                    'HY001', 'HY002', 'HY003', 'HY004', 'HY005', 'HY006', 'HY007', 'HY008', 'HY009', 'HY010', 'HY011']
    '''
    '''
    # 因子列表(因子组合2)
    g.__factorList = [#估值
                    'EP',
                    'BP',
                    #'PS',
                    #'DP',
                    'RD',
                    #'CFP',
                    #资本结构
                    'log_NC', 
                    'LEV', 
                    'CMV',
                    #'FACR',
                    #盈利
                    'NI_p', 
                    'NI_n', 
                    'GPM',
                    'ROE',
                    #'ROA',
                    #'OPTP',
                    #成长
                    'PEG',
                    #'g', 
                    #'G_p',
                    #行业哑变量
                    'HY001', 'HY002', 'HY003', 'HY004', 'HY005', 'HY006', 'HY007', 'HY008', 'HY009', 'HY010', 'HY011']
    '''
    #'''
    # 因子列表(因子组合3)
    g.__factorList = [#估值
                    'EP',
                    'BP',
                    'PS',
                    'DP',
                    'RD',
                    'CFP',
                    #资本结构
                    'log_NC', 
                    'LEV', 
                    'CMV',
                    'FACR',
                    #盈利
                    'NI_p', 
                    'NI_n', 
                    'GPM',
                    'ROE',
                    'ROA',
                    'OPTP',
                    #成长
                    'PEG',
                    'g', 
                    'G_p',
                    #行业哑变量
                    'HY001', 'HY002', 'HY003', 'HY004', 'HY005', 'HY006', 'HY007', 'HY008', 'HY009', 'HY010', 'HY011']
    #'''
'''
================================================================================
每天开盘前
================================================================================
'''
#每天开盘前要做的事情
def before_trading_start(context):
    # 当天是否交易
    g.if_trade = False                       
    # 每g.refresh_rate天，调仓一次
    if g.days % g.refresh_rate == 0:
        g.if_trade = True                           
        # 设置手续费与手续费
        set_slip_fee(context)                       
        # 设置初始股票池
        sample = get_index_stocks(g.secCode)
        # 设置可交易股票池
        #g.feasible_stocks = set_feasible_stocks(sample,context)
        g.__feasible_stocks = set_feasible_stocks(sample,context)
        # 因子获取Query
        g.__q = get_q_Factor(g.__feasible_stocks)
    g.days+=1

#5
# 根据不同的时间段设置滑点与手续费
# 输入：context（见API）
# 输出：none
def set_slip_fee(context):
    # 将滑点设置为0
    set_slippage(FixedSlippage(0)) 
    # 根据不同的时间段设置手续费
    dt = context.current_dt
    if dt > datetime.datetime(2013,1, 1):
        set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
    elif dt > datetime.datetime(2008,9, 18):
        set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.001, close_commission=0.002, close_today_commission=0, min_commission=5), type='stock')
    elif dt > datetime.datetime(2008,4, 24):
        set_order_cost(OrderCost(open_tax=0.001, close_tax=0.001, open_commission=0.002, close_commission=0.003, close_today_commission=0, min_commission=5), type='stock')
    else:
        set_order_cost(OrderCost(open_tax=0.003, close_tax=0.003, open_commission=0.003, close_commission=0.004, close_today_commission=0, min_commission=5), type='stock')

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

'''
================================================================================
每天交易时
================================================================================
'''
# 每天回测时做的事情
def handle_data(context,data):
    if g.if_trade == True:
        # 记录交易次数
        g.__tradeCount = g.__tradeCount + 1

        # 训练集合成
        yesterday = context.previous_date

        df_train = get_df_train(g.__q,yesterday,g.__trainlength,g.__intervals)
        df_train = initialize_df(df_train)

        # T日截面数据（测试集）
        df = get_fundamentals(g.__q, date = None)
        df = initialize_df(df)
    
        # 离散值处理
        for fac in g.__winsorizeList:
            df_train[fac] = winsorize_med(df_train[fac], scale=5, inclusive=True, inf2nan=True, axis=0)    
            df[fac] = winsorize_med(df[fac], scale=5, inclusive=True, inf2nan=True, axis=0)    
        
        # 标准化处理        
        for fac in g.__standardizeList:
            df_train[fac] = standardlize(df_train[fac], inf2nan=True, axis=0)
            df[fac] = standardlize(df[fac], inf2nan=True, axis=0)

        # 中性化处理（行业中性化）
        df_train = neutralize(df_train,g.__industry_set)
        df = neutralize(df,g.__industry_set)

        #训练集（包括验证集）
        X_trainval = df_train[g.__factorList]
        X_trainval = X_trainval.fillna(0)
        
        #定义机器学习训练集输出
        y_trainval = df_train[['log_mcap']]
        y_trainval = y_trainval.fillna(0)
 
        #测试集
        X = df[g.__factorList]
        X = X.fillna(0)
        
        #定义机器学习测试集输出
        y = df[['log_mcap']]
        y.index = df['code']
        y = y.fillna(0)
 
        kfold = KFold(n_splits=4)
        if g.__gridserach == False:
            #不带网格搜索的机器学习
            if g.method == 'svr': #SVR
                from sklearn.svm import SVR
                model = SVR(C=100, gamma=1)
            elif g.method == 'lr':
                from sklearn.linear_model import LinearRegression
                model = LinearRegression()
            elif g.method == 'ridge': #岭回归
                from sklearn.linear_model import Ridge
                model = Ridge(random_state=42,alpha=100)
            elif g.method == 'rf': #随机森林
                from sklearn.ensemble import RandomForestRegressor
                model = RandomForestRegressor(random_state=42,n_estimators=500,n_jobs=-1)
            else:
                g.__scoreWrite = False
        else:
            # 带网格搜索的机器学习
            para_grid = {}
            if g.method == 'svr':
                from sklearn.svm import SVR  
                para_grid = {'C':[10,100],'gamma':[0.1,1,10]}
                grid_search_model = SVR()
            elif g.method == 'lr':
                from sklearn.linear_model import LinearRegression
                grid_search_model = LinearRegression()
            elif g.method == 'ridge':
                from sklearn.linear_model import Ridge
                para_grid = {'alpha':[1,10,100]}
                grid_search_model = Ridge()
            elif g.method == 'rf':
                from sklearn.ensemble import RandomForestRegressor
                para_grid = {'n_estimators':[100,500,1000]}
                grid_search_model = RandomForestRegressor()
            else:
                g.__scoreWrite = False
    
            from sklearn.model_selection import GridSearchCV
            model = GridSearchCV(grid_search_model,para_grid,cv=kfold,n_jobs=-1)
        
        # 拟合训练集，生成模型
        model.fit(X_trainval,y_trainval)
        # 预测值
        y_pred = model.predict(X)

        # 新的因子：实际值与预测值之差    
        factor = y - pd.DataFrame(y_pred, index = y.index, columns = ['log_mcap'])
        
        #对新的因子，即残差进行排序（按照从小到大）
        factor = factor.sort_index(by = 'log_mcap')
        
        ###  分组测试用 ##############
        if g.invest_by_group == True:
            len_secCodeList = len(list(factor.index))
            g.stocknum = int(len_secCodeList * g.quantile)
        ###  分组测试用 ##############

        start = g.stocknum * (g.group-1)
        end = g.stocknum * g.group
        stockset = list(factor.index[start:end])

        current_data = get_current_data()

        #卖出
        sell_list = list(context.portfolio.positions.keys())
        for stock in sell_list:
            if stock not in stockset:
                if stock in g.__feasible_stocks:
                    if current_data[stock].last_price == current_data[stock].high_limit:
                        pass
                    else:
                        stock_sell = stock
                        order_target_value(stock_sell, 0)

        #分配买入资金    
        if len(context.portfolio.positions) < g.stocknum:
            num = g.stocknum - len(context.portfolio.positions)
            cash = context.portfolio.cash/num
        else:
            cash = 0
            num = 0
            
        #买入
        for stock in stockset[:g.stocknum]:
            if stock in sell_list:
                pass
            else:
                if current_data[stock].last_price == current_data[stock].low_limit:
                    pass
                else:
                    stock_buy = stock
                    order_target_value(stock_buy, cash)
                    num = num - 1
                    if num == 0:
                        break
# 获取初始特征值
def get_q_Factor(feasible_stocks):
    q = query(valuation.code, 
          valuation.market_cap,#市值
          valuation.circulating_market_cap,
          balance.total_assets - balance.total_liability,#净资产
          balance.total_assets / balance.total_liability, 
          indicator.net_profit_to_total_revenue, #净利润/营业总收入
          indicator.inc_revenue_year_on_year,  #营业收入增长率（同比）
          balance.development_expenditure, #RD
          valuation.pe_ratio, #市盈率（TTM）
          valuation.pb_ratio, #市净率（TTM）
          indicator.inc_net_profit_year_on_year,#净利润增长率（同比）
          balance.dividend_payable,
          indicator.roe,
          indicator.roa,
          income.operating_profit / income.total_profit, #OPTP
          indicator.gross_profit_margin, #销售毛利率GPM
          balance.fixed_assets / balance.total_assets, #FACR
          valuation.pcf_ratio, #CFP
          valuation.ps_ratio #PS
        ).filter(
            valuation.code.in_(feasible_stocks)
        )
    return q
    
# 训练集长度设置
def get_df_train(q,d,trainlength,interval):
    
    #'''
    date1 = shift_trading_day(d,interval)
    date2 = shift_trading_day(d,interval*2)
    date3 = shift_trading_day(d,interval*3)

    d1 = get_fundamentals(q, date = date1)
    d2 = get_fundamentals(q, date = date2)
    d3 = get_fundamentals(q, date = date3)

    if trainlength == 1:
        df_train = d1
    elif trainlength == 3:
        # 3个周期作为训练集    
        df_train = pd.concat([d1, d2, d3],ignore_index=True)
    elif trainlength == 4:
        date4 = shift_trading_day(d,interval*4)
        d4 = get_fundamentals(q, date = date4)
        # 4个周期作为训练集    
        df_train = pd.concat([d1, d2, d3, d4],ignore_index=True)
    elif trainlength == 6:
        date4 = shift_trading_day(d,interval*4)
        date5 = shift_trading_day(d,interval*5)
        date6 = shift_trading_day(d,interval*6)

        d4 = get_fundamentals(q, date = date4)
        d5 = get_fundamentals(q, date = date5)
        d6 = get_fundamentals(q, date = date6)

        # 6个周期作为训练集
        df_train = pd.concat([d1,d2,d3,d4,d5,d6],ignore_index=True)
    elif trainlength == 9:
        date4 = shift_trading_day(d,interval*4)
        date5 = shift_trading_day(d,interval*5)
        date6 = shift_trading_day(d,interval*6)
        date7 = shift_trading_day(d,interval*7)
        date8 = shift_trading_day(d,interval*8)
        date9 = shift_trading_day(d,interval*9)

        d4 = get_fundamentals(q, date = date4)
        d5 = get_fundamentals(q, date = date5)
        d6 = get_fundamentals(q, date = date6)
        d7 = get_fundamentals(q, date = date7)
        d8 = get_fundamentals(q, date = date8)
        d9 = get_fundamentals(q, date = date9)
    
        # 9个周期作为训练集
        df_train = pd.concat([d1,d2,d3,d4,d5,d6,d7,d8,d9],ignore_index=True)
    else:
        pass
    
    return df_train

# 某一日的前shift个交易日日期 
# 输入：date为datetime.date对象(是一个date，而不是datetime)；shift为int类型
# 输出：datetime.date对象(是一个date，而不是datetime)
def shift_trading_day(date,shift):
    # 获取所有的交易日，返回一个包含所有交易日的 list,元素值为 datetime.date 类型.
    tradingday = get_all_trade_days()
    # 得到date之后shift天那一天在列表中的行标号 返回一个数
    shiftday_index = list(tradingday).index(date) - shift
    # 根据行号返回该日日期 为datetime.date类型
    return tradingday[shiftday_index]

# 特征值提取
def initialize_df(df):
    #定义列名
    df.columns = ['code', 
                'mcap', 
                'CMV',
                'log_NC', 
                'LEV', 
                'NI_p', 
                'g', 
                'development_expenditure',
                'pe',
                'BP',
                'G_p',
                'dividend_payable',
                'ROE',
                'ROA',
                'OPTP',
                'GPM',
                'FACR',
                'pcf_ratio',
                'PS'
                ]
    
    #标签：对数市值
    df['log_mcap'] = np.log(df['mcap'])
    
    #因子：
    df['EP'] = df['pe'].apply(lambda x: 1/x)
    
    df['BP'] = df['BP'].apply(lambda x: 1/x)
    df['DP'] = df['dividend_payable']/(df['mcap']*100000000)
    #因子：
    df['RD'] = df['development_expenditure']/(df['mcap']*100000000)
    # 因子：现金收益率
    df['CFP'] = df['pcf_ratio'].apply(lambda x: 1/x)

    df['log_NC'] = np.log(df['log_NC'])
    
    df['CMV'] = np.log(df['CMV'])
    
    #因子：净利润率
    df['NI_p'] = np.abs(df['NI_p'])
    #因子：
    df['NI_n'] = np.abs(df['NI_p'][df['NI_p']<0])   

    df['PEG'] = df['pe'] / (df['G_p']*100)
    
    del df['mcap']
    del df['pe']
    del df['dividend_payable']
    del df['pcf_ratio']
    del df['development_expenditure']
    
    df = df.fillna(0)
    
    return df
    
# 中性化
def neutralize(df,industry_set):
    for i in range(len(industry_set)):
        s = pd.Series([0]*len(df), index=df.index)
        df[industry_set[i]] = s

        industry = get_industry_stocks(industry_set[i])
        for j in range(len(df)):
            if df.iloc[j,0] in industry:
                df.iloc[j,i+8] = 1
                
    return df    
    
'''
================================================================================
每天收盘后
================================================================================
'''
# 每天收盘后做的事情
# 进行长运算（本策略中不需要）
def after_trading_end(context):
    return
            
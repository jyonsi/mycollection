import statsmodels.api as sm
from statsmodels import regression
import numpy as np
import pandas as pd
from pandas import DataFrame,Series
import scipy.stats as st
from scipy.stats import norm
import time 
import datetime
from datetime import date
from jqdata import *
import math
import jqdata
from jqdata import finance
from jqfactor import get_factor_values
'''
================================================================================
总体回测前
================================================================================
'''
#总体回测前要做的事情
def initialize(context):
    set_params()        #1设置策参数
    set_variables()     #2设置中间变量
    set_backtest()      #3设置回测条件
#1
#设置策参数
def set_params():
    g.tc=20  # 调仓频率
    g.yb=2  # 样本长度,过滤前g.yb天未停牌的股票
    g.N=30  # 持仓数目
    g.days = 0
#2
#设置中间变量
def set_variables():
    g.t=0               #记录连续回测天数
    g.if_trade=False    #当天是否交易
    
    today=date.today()     #取当日时间xxxx-xx-xx
    a=get_all_trade_days() #取所有交易日:[datetime.date(2005, 1, 4)到datetime.date(2016, 12, 30)]
    g.ATD=['']*len(a)      #获得len(a)维的单位向量
    for i in range(0,len(a)):
        g.ATD[i]=a[i].isoformat() #转换所有交易日为iso格式:2005-01-04到2016-12-30
        #列表会取到2016-12-30，现在需要将大于今天的列表全部砍掉
        if today<=a[i]:
            break
    g.ATD=g.ATD[:i]        #iso格式的交易日：2005-01-04至今
#3
#设置回测条件
def set_backtest():
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    log.set_level('order', 'error')
'''
================================================================================
每天开盘前
================================================================================
'''
#每天开盘前要做的事情
def before_trading_start(context):
    if g.t%g.tc==0:
        #每g.tc天，交易一次行
        g.if_trade=True 
        # 设置手续费与手续费
        set_slip_fee(context) 
        # 设置可行股票池：获得当前开盘的沪深300股票池并剔除当前或者计算样本期间停牌的股票
        #SZ1=get_index_stocks('399008.XSHE')  #中小板指
        SZ2=get_index_stocks('399905.XSHE')
        SH=get_index_stocks('399300.XSHE')
        SH2=get_index_stocks('000852.XSHG')#中证红利
        tem_index=SH+SZ2
        #tem_index=SH
        tem_index=set(tem_index)
        #print len(tem_index)
        g.all_stocks = set_feasible_stocks(tem_index,g.yb,context)
        g.all_stocks = sorted(g.all_stocks)#股票代码排序
    g.t+=1

#4 根据不同的时间段设置滑点与手续费
def set_slip_fee(context):
    # 将滑点设置为0.00246
    set_slippage(PriceRelatedSlippage(0.002))
    # 根据不同的时间段设置手续费
    dt=context.current_dt
    log.info(type(context.current_dt))
    
    if dt>datetime.datetime(2013,1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0015, min_cost=5)) 
        
    elif dt>datetime.datetime(2011,1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))
            
    elif dt>datetime.datetime(2009,1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))
                
    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))

#5
# 设置可行股票池：
# 过滤掉当日停牌的股票,且筛选出前days天未停牌股票
# 输入：stock_list-list类型,样本天数days-int类型，context（见API）
# 输出：颗星股票池-list类型
def set_feasible_stocks(stock_list,days,context):
    # 得到是否停牌信息的dataframe，停牌的1，未停牌得0
    suspened_info_df = get_price(list(stock_list), start_date=context.current_dt, end_date=context.current_dt, frequency='daily', fields='paused')['paused'].T
    # 过滤停牌股票 返回dataframe
    unsuspened_index = suspened_info_df.iloc[:,0]<1
    # 得到当日未停牌股票的代码list:
    unsuspened_stocks = suspened_info_df[unsuspened_index].index
    # 进一步，筛选出前days天未曾停牌的股票list:
    feasible_stocks=[]
    current_data=get_current_data()
    for stock in unsuspened_stocks:
        if sum(attribute_history(stock, days, unit='1d',fields=('paused'),skip_paused=False))[0]==0:
            feasible_stocks.append(stock)
    # 过滤涨停的股票        
    last_prices = history(1, unit='1m', field='close', security_list=feasible_stocks)
    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    feasible_stocks = [stock for stock in feasible_stocks if stock in context.portfolio.positions.keys() 
        or last_prices[stock][-1] < current_data[stock].high_limit]
    # 过滤跌停的股票
    feasible_stocks = [stock for stock in feasible_stocks if stock in context.portfolio.positions.keys() 
        or last_prices[stock][-1] > current_data[stock].low_limit]
    # 过滤创业版股票
    #feasible_stocks = [stock for stock in feasible_stocks if stock[0:3] != '300']
    # 过滤ST及其他具有退市标签的股票
    feasible_stocks = [stock for stock in feasible_stocks 
        if not current_data[stock].is_st 
        and 'ST' not in current_data[stock].name 
        and '*' not in current_data[stock].name 
        and '退' not in current_data[stock].name]
    #上市时间大于3年，考虑财务数据
    feasible_stocks=[stock for stock in feasible_stocks if ((context.current_dt.date() - get_security_info(stock).start_date).days)>1825]
    return feasible_stocks
'''
================================================================================
每天交易时
================================================================================
'''

#每天交易时要做的事情
def handle_data(context, data):
    if g.if_trade==True:
        # 获得调仓日的日期字符串
        end=context.previous_date
        ais=get_df(context,end)#当前因子值
        ais['f_sum'] = ais.apply(lambda x: x.sum(), axis=1)
        # 依打分排序，当前需要持仓的
        factor = ais.sort_index(by='f_sum',ascending=False)
        #每个行业选一只股
        f_sum= factor.f_sum  #28个行业
        industry_list = ['801011','801012','801013','801014','801015','801016','801017',
        '801018','801021','801022','801023','801024','801031','801032','801033','801034',
        '801035','801036','801037','801041','801051','801053',
        '801054','801055','801061','801062','801071','801072','801073','801074',
        '801075','801076','801081','801082','801083','801084','801085','801092',
        '801093','801094','801101','801102','801111','801112','801121','801123',
        '801124','801131','801132','801141','801142','801143','801144',
        '801151','801152','801153','801154','801155','801156','801161','801162',
        '801163','801164','801171','801172','801173','801174','801175','801176',
        '801177','801178','801181','801182','801191','801192','801193','801194','801202',
        '801203','801204','801205','801211','801212','801213','801214','801215','801222','801223',
        '801231','801711','801712','801713','801721','801722','801723','801724',
        '801725','801731','801732','801733','801734','801741','801742','801743',
        '801744','801751','801752','801761','801881',]
        #industry_list = ['801010','801020','801030','801040','801050','801080','801110','801120','801130','801140','801150','801160','801170','801180','801200','801210','801230','801710','801720','801730','801740','801750','801760','801770','801780','801790','801880','801890']
        #industry_list = ['HY001','HY002','HY003','HY004','HY005','HY006','HY007','HY008','HY009','HY010','HY011']
        '''
        stk=pd.Series()
        for i in industry_list:
            industry_stocks=pd.DataFrame(index =get_industry_stocks(i),columns=[0])
            industry_stocks=industry_stocks.fillna(0)
            #industry_stocks=get_industry_stocks(i)
            #f_sum=factor.filter(industry_stocks)
            #print f_sum
            x = pd.merge(industry_stocks,factor,left_index=True, right_index=True, how='inner')
            x = x.sort_index(by='f_sum',ascending=False)
            #x=x.f_sum
            if x.index.values != []:
                stk[i]=x.index.values[0]
        print stk
        stock_sort= stk.values
        '''
        '''
        #单个行业最大持仓数
        max_num=1
        for industry in industry_list:
            i = 0
            stocks = get_industry_stocks(industry)
            for stock in factor.index:
                if stock in stocks:
                    i += 1
                    if i > max_num:
                        factor=factor.drop([stock],axis = 0)
        print factor
        factor = factor.sort_index(by='f_sum',ascending=False)
        '''
        stock_sort=factor.index[:g.N]
        
        print stock_sort
        g.everyStock=context.portfolio.portfolio_value/len(stock_sort)
        order_stock_sell(context,data,stock_sort)
        order_stock_buy(context,data,stock_sort)       
    g.if_trade=False

#6
#获得卖出信号，并执行卖出操作
#输入：context, data，已排序股票列表stock_sort-list类型
def order_stock_sell(context,data,stock_sort):
    # 对于不需要持仓的股票，全仓卖出
    for stock in context.portfolio.positions:
        #除去排名前g.N个股票（选股！）
        if stock not in stock_sort[:g.N]:
            stock_sell = stock
            order_target_value(stock_sell, 0)
#7
#获得买入信号，并执行买入操作
#输入：context, data，已排序股票列表stock_sort-list类型
def order_stock_buy(context,data,stock_sort):
    # 对于需要持仓的股票，按分配到的份额买入
    for stock in stock_sort:
        stock_buy = stock
        order_target_value(stock_buy, g.everyStock)

#数据预处理
#3、标准化
def stand(series): #原始值法
  std = series.std()
  mean = series.mean()
  return (series-mean)/std

#获取n年前的今天
def get_n_year_date(context,n):
    now = context.previous_date  #context.current_dt
    last_one_year = int(now.year) - n
    now_date = now.strftime("%Y-%m-%d")[-6:]
    last_year_date =  str(last_one_year) + now_date
    return last_year_date
# 给定日期获取因子数据，并处理
def get_df(context,end):
    year1=get_n_year_date(context,1)
    year2=get_n_year_date(context,2)
    year3=get_n_year_date(context,3)
    year4=get_n_year_date(context,4)
    q=query(valuation.code,
            1/valuation.pb_ratio,
            1/valuation.pe_ratio,
            ).filter(valuation.code.in_(g.all_stocks))
    df = get_fundamentals(q, date = end)
    df.columns = ['code','BP','EP']
    df.index = df.code.values
    del df['code']
    #一、财务欺诈、困境
    df['COMB']=get_COMB_PMAN(g.all_stocks,end)
    df['PMAN']=get_PMAN(g.all_stocks,end,year1)
    df['LPFD']=get_LPFD(g.all_stocks,end,year1)
    #二、廉价股
    EB = get_factor_values(securities=g.all_stocks, factors=['EBITDA','cfo_to_ev','net_operate_cash_flow_ttm','operating_revenue_growth_rate'],
        end_date=end,count=1)
    df['EBITDA/EV']=EB['EBITDA'].iloc[0] * EB['cfo_to_ev'].iloc[0] / EB['net_operate_cash_flow_ttm'].iloc[0]
    #三、质量
    FP=get_FP(g.all_stocks,end,year1,year2,year3,year4)
    #p_fs财务实力指标
    #p_fs = pd.DataFrame(index=df.index,columns=[])
    CP=get_CP(g.all_stocks,end)
    ROI=get_ROI(g.all_stocks,end,year1)
    STAB=get_STAB(g.all_stocks,end,year1)
    p_fs=pd.concat([CP,ROI,STAB],axis=1)
    p_fs = p_fs.fillna(0)
    p_fs['fs']=p_fs.mean(axis=1)
    p_fs['p_fs']=p_fs['fs'].rank(ascending = True,method='dense')
    p_fs['p_fs']=p_fs['p_fs']/max(p_fs['p_fs'])
    df['quality']=0.5*FP+0.5*p_fs['p_fs']
    
    #技术指标
    df['Vola']=get_df_VAR(g.all_stocks,end)#12个月波动率

    #数据清洗
    df = df.fillna(0)  #nan值替换为0
    #取得所有因子后，删除不合要求的
    #三个 0.96 30只 120%          144
    df = df.sort('COMB',ascending=True)#108%
    df=df.head(int(len(df)*0.95))
    df = df.sort('PMAN',ascending=True)#111%
    df=df.head(int(len(df)*0.95))
    df = df.sort('LPFD',ascending=True) #无技术中证800 30只股 110%
    df=df.head(int(len(df)*0.95))
    
    #低估值筛选中证800   20160101-20190222   21.83%  -5.65%  波动放前面
    #  17.08%  -5.65%
    #低估值筛选中证800   20160101-20190222   25.32%  -5.65%  MTM90%,Vola80%
    #20.58   vola0.8-136%   vola0.5-152%  vola0.3-129% 
    #无技术  10只 139%    vola0.9-126%
    #无技术  中证800 30只股 105%
    #df = df.sort('Vola',ascending=False)
    #df=df.head(int(len(df)*0.5))
    df = df.sort('EBITDA/EV',ascending=False)
    df=df.head(int(len(df)*0.3))

    #标准化处理
    #df['BP']=stand(df['BP'])#,df['quality']
    del df['BP'],df['EP'],df['Vola'],df['EBITDA/EV']
    del df['COMB'],df['PMAN'],df['LPFD']
    print df.head(3)
    return df
#第一步：避免持有存在永久性资本损失的股票
#1识别潜在的欺诈和操控
#1.1应记项目筛选
def get_COMB_PMAN(stock_list,end):
    #流动资产、现金及其等价物、流动负债、
    #长期债务、应交所得税、折旧与摊销
    #balance.code, balance.total_current_assets,
    #cash_flow.cash_and_equivalents_at_end, balance.total_current_liability,
    #balance.total_non_current_liability, income.income_tax_expense,
    q = query(income.code,income.net_profit, cash_flow.net_operate_cash_flow, 
        balance.total_sheet_owner_equities
        ).filter(income.code.in_(g.all_stocks))
    p1 = get_fundamentals(q, date=end)
    pa = pd.DataFrame(index=p1.code.values,columns=['STA','SNOA'])
    pa['STA'] = (p1['net_profit'].values - p1['net_operate_cash_flow'].values) / p1['total_sheet_owner_equities'].values
    #营业资产、营业负债、总资产
    #总资产、现金及等价物、短期负债、长期负债、(少数权益、优先股、账面普通股)
    q2 = query(income.code, balance.total_sheet_owner_equities,
    cash_flow.cash_and_equivalents_at_end, balance.total_current_liability,
    balance.total_non_current_liability, balance.total_owner_equities
        ).filter(income.code.in_(g.all_stocks))    
    p2 = get_fundamentals(q2, date=end)
    pa['SNOA']=(p2['total_sheet_owner_equities'].values - p2['cash_and_equivalents_at_end'].values + (p2['total_current_liability'].values
    +p2['total_non_current_liability'].values+p2['total_owner_equities'].values)
    ) / p2['total_sheet_owner_equities'].values
    pa['p_STA']=pa['STA'].rank(ascending = True,method='dense')/len(stock_list)
    pa['p_SNOA']=pa['SNOA'].rank(ascending = True,method='dense')/len(stock_list)
    pa = pa.fillna(0)
    pa['COMB']=(pa['p_STA']+pa['p_SNOA'])/2.0
    return pa['COMB']
#1.2欺诈和操控筛选
def get_PMAN(stock_list,end,year1):
    #应收账款周转天数、销售毛利率、类无形资产比率(无形资产+研发支出+商誉)
    #营业收入TTM、资产减值损失TTM、销售费用TTM、财务费用TTM、管理费用TTM
    a0=get_factor_values(securities=stock_list, factors=['account_receivable_turnover_days','gross_income_ratio',
    'intangible_asset_ratio','total_operating_revenue_ttm','asset_impairment_loss_ttm',
    'sale_expense_ttm','financial_expense_ttm','administration_expense_ttm'], end_date=end,count=1)
    a1=get_factor_values(securities=stock_list, factors=['account_receivable_turnover_days','gross_income_ratio',
    'intangible_asset_ratio','total_operating_revenue_ttm','asset_impairment_loss_ttm',
    'sale_expense_ttm','financial_expense_ttm','administration_expense_ttm'], end_date=year1,count=1)
    #负债、总资产、应收账款、其他应收款、应收票据
    q = query(income.code,balance.total_liability, balance.total_sheet_owner_equities,
    balance.account_receivable,balance.other_receivable,balance.bill_receivable
        ).filter(income.code.in_(g.all_stocks))
    p0 = get_fundamentals(q, date=end)
    p1 = get_fundamentals(q, date=year1)
    p0.index = p0.code.values
    p1.index = p1.code.values
    #创建公式表
    pa = pd.DataFrame(index=p0.code.values,columns=['DSRI','GMI','AQI','SGI','DEPI','SGAI','LVGI','TATA','PROBM','CDF'])
    pa['DSRI']=(a0['account_receivable_turnover_days']/a1['account_receivable_turnover_days'].values).T
    pa['GMI']=(a1['gross_income_ratio']/a0['gross_income_ratio'].values).T
    pa['AQI']=(a0['intangible_asset_ratio']).T
    pa['SGI']=(a0['total_operating_revenue_ttm']/a1['total_operating_revenue_ttm'].values).T
    pa['DEPI']=(a1['asset_impairment_loss_ttm']/a0['asset_impairment_loss_ttm'].values).T
    pa['SGAI']=((a0['sale_expense_ttm']+a0['financial_expense_ttm']+a0['administration_expense_ttm']
    )/(a1['sale_expense_ttm']+a1['financial_expense_ttm']+a1['administration_expense_ttm']).values).T
    pa['LVGI']=(p0['total_liability']/p0['total_sheet_owner_equities'])/(p1['total_liability']/p1['total_sheet_owner_equities'])
    pa['TATA']=p0['account_receivable']+p0['other_receivable']+p0['bill_receivable']
    pa['PROBM'] = -4.84+0.92*pa['DSRI']+0.528*pa['GMI']+0.404*pa['AQI']
    +0.892*pa['SGI']+0.115*pa['DEPI']-0.172*pa['SGAI']+4.679*pa['TATA']+0.327*pa['LVGI']
    pa['CDF'] = norm.cdf(pa['PROBM'])
    pa = pa.fillna(0)
    #print pa
    return pa['CDF']
#2识别具有搞财务困境风险的股票
def get_LPFD(stock_list,end,year1):
    #负债、市值(临时，以后按股本乘股价重写)、总资产、股东权益、现金及其等价物
    q = query(income.code,balance.total_liability,valuation.market_cap,balance.total_sheet_owner_equities,
        balance.total_owner_equities,cash_flow.cash_and_equivalents_at_end
        ).filter(income.code.in_(g.all_stocks))
    p0 = get_fundamentals(q, date=end)
    p1 = get_fundamentals(q, date=year1)
    p0.index = p0.code.values
    p1.index = p1.code.values
    pa = pd.DataFrame(index=p1.code.values,columns=['MTA','TLMTA','CASHMTA','SIGMA','RSIZE','MB','LPFD','PFD'])
    pa['MTA']=p0['total_liability']/100000000+p0['market_cap']#单位问题，数值太大
    #NIMTAAVG
    q_ni = query(income.code,income.net_profit,balance.total_liability,valuation.market_cap
        ).filter(income.code.in_(g.all_stocks))
    p_ni1 = get_fundamentals(q_ni, date=end)#1季度
    p_ni1.index = p_ni1.code.values
    qua2=shift_trading_day(end,shift=61)
    p_ni2 = get_fundamentals(q_ni, date=qua2)#2季度
    p_ni2.index = p_ni2.code.values
    qua3=shift_trading_day(end,shift=122)
    p_ni3 = get_fundamentals(q_ni, date=qua3)#3季度
    p_ni3.index = p_ni3.code.values
    qua4=shift_trading_day(end,shift=183)
    p_ni4 = get_fundamentals(q_ni, date=qua4)#4季度
    p_ni4.index = p_ni4.code.values
    pa['NIMTAAVG']=(0.5333*p_ni1['net_profit']/(p_ni1['total_liability']+p_ni1['market_cap'])
    +0.2666*p_ni2['net_profit']/(p_ni2['total_liability']+p_ni2['market_cap'])
    +0.1333*p_ni3['net_profit']/(p_ni3['total_liability']+p_ni3['market_cap'])
    +0.0666*p_ni4['net_profit']/(p_ni4['total_liability']+p_ni4['market_cap']))
    pa['TLMTA']=p0['total_liability']/100000000.0/pa['MTA']
    pa['CASHMTA']=p0['cash_and_equivalents_at_end']/100000000/pa['MTA']
    #计算EXRETAVG
    quar1=np.log(1+get_df_MTM3(g.all_stocks,end,0,61))-np.log(1+get_index_MTM('000300.XSHG',end,0))
    quar2=np.log(1+get_df_MTM3(g.all_stocks,end,61,61))-np.log(1+get_index_MTM('000300.XSHG',end,61))
    quar3=np.log(1+get_df_MTM3(g.all_stocks,end,122,61))-np.log(1+get_index_MTM('000300.XSHG',end,122))
    quar4=np.log(1+get_df_MTM3(g.all_stocks,end,183,61))-np.log(1+get_index_MTM('000300.XSHG',end,183))
    pa['EXRETAVG']=0.5333*quar1+0.2666*quar2+0.1333*quar3+0.0666*quar4
    #计算SIGMA
    df_price_info_between_1shift = get_price(list(stock_list),count = 62,
    end_date = end,frequency = 'daily', fields = 'close')['close']
    x =[]
    for i in range(0,61):
        x.append(df_price_info_between_1shift.iloc[ i + 1 ]
                 / df_price_info_between_1shift.iloc[ i ] - 1.000)
    df_VAR_info = pd.DataFrame(x).T
    df_VAR_info=df_VAR_info.fillna(1)
    pa['SIGMA']=df_VAR_info.std(axis = 1, skipna = True)*sqrt(243)
    #RSIZE   市值改进
    HS300=get_index_stocks('399300.XSHE')
    q_300 = query(income.code,valuation.market_cap).filter(income.code.in_(HS300))
    p_300 = get_fundamentals(q_300, date=end)
    p_300.index = p_300.code.values
    pa['RSIZE']=np.log(p0['market_cap']/sum(p_300['market_cap']))
    #MB
    pa['MB']=(p0['total_owner_equities']/100000000+0.1*(p0['market_cap'] - p0['total_owner_equities']/100000000))/100
    #PR
    df_price=(get_price(list(stock_list),count=1,end_date=end,frequency='daily',fields='close')['close']).T
    df_price[df_price>15]=15
    pa['PRICE']=np.log(df_price)
    pa['LPFD']=(-20.26*pa['NIMTAAVG']+1.42*pa['TLMTA']-7.13*pa['EXRETAVG']+1.41
    *pa['SIGMA']-0.045*pa['RSIZE']-2.13*pa['CASHMTA']+0.075*pa['MB']-0.058*pa['PRICE']-9.16)
    pa['PFD']=1.0/(1+exp(-pa['LPFD']))
    #print pa
    return pa['PFD']
#第二步 找出最廉价的股票

#第三步 找出质量最高的股票
#1经济特许权
def get_FP(stocks,end,year1,year2,year3,year4):
    q = query(valuation.code,cash_flow.fix_intan_other_asset_acqui_cash,
        balance.total_sheet_owner_equities
            ).filter(valuation.code.in_(g.all_stocks))
    p0 = get_fundamentals(q, date=end)
    p1 = get_fundamentals(q, date=year1)
    p2 = get_fundamentals(q, date=year2)
    p3 = get_fundamentals(q, date=year3)
    p4 = get_fundamentals(q, date=year4)
    p0.index = p0.code.values
    p1.index = p1.code.values
    p2.index = p2.code.values
    p3.index = p3.code.values
    p4.index = p4.code.values
    pa = pd.DataFrame(index=p0.code.values,columns=['ROAEBITTTM'])
    #ROA、EBIT息税前利润、净运营资本、固定资产比率、总资产现金回收率=经营活动产生的现金流量净额(ttm) / 总资产、经营活动现金流量净额TTM
    f0 = get_factor_values(securities=stocks, factors=['ROAEBITTTM','EBIT','net_working_capital','fixed_asset_ratio','net_operate_cash_flow_to_asset','net_operate_cash_flow_ttm'
    ,'net_operate_cash_flow_ttm','DEGM','gross_income_ratio'],end_date=end,count=1)
    f1 = get_factor_values(securities=stocks, factors=['ROAEBITTTM','EBIT','net_working_capital','fixed_asset_ratio','net_operate_cash_flow_to_asset','net_operate_cash_flow_ttm'
    ,'net_operate_cash_flow_ttm','DEGM','gross_income_ratio'],end_date=year1,count=1)
    f2 = get_factor_values(securities=stocks, factors=['ROAEBITTTM','EBIT','net_working_capital','fixed_asset_ratio','net_operate_cash_flow_to_asset','net_operate_cash_flow_ttm'
    ,'net_operate_cash_flow_ttm','DEGM','gross_income_ratio'],end_date=year2,count=1)
    f3 = get_factor_values(securities=stocks, factors=['ROAEBITTTM','EBIT','net_working_capital','fixed_asset_ratio','net_operate_cash_flow_to_asset','net_operate_cash_flow_ttm'
    ,'net_operate_cash_flow_ttm','DEGM','gross_income_ratio'],end_date=year3,count=1)
    f4 = get_factor_values(securities=stocks, factors=['ROAEBITTTM','EBIT','net_working_capital','fixed_asset_ratio','net_operate_cash_flow_to_asset','net_operate_cash_flow_ttm'
    ,'net_operate_cash_flow_ttm','DEGM','gross_income_ratio'],end_date=year4,count=1)
    pa['ROAEBITTTM']=((f0['ROAEBITTTM'].T+1)*(f1['ROAEBITTTM'].T.values+1)*(f2['ROAEBITTTM'].T.values+1)*(f3['ROAEBITTTM'].T.values+1)*(f4['ROAEBITTTM'].T.values+1))**(1.0/5)-1
    p0['ROC0']=f0['EBIT'].T/(f0['net_working_capital'].T+f0['fixed_asset_ratio'].T
              *f0['net_operate_cash_flow_to_asset'].T/f0['net_operate_cash_flow_ttm'].T)
    p0['ROC1']=f1['EBIT'].T/(f1['net_working_capital'].T+f1['fixed_asset_ratio'].T
              *f1['net_operate_cash_flow_to_asset'].T/f1['net_operate_cash_flow_ttm'].T)    
    p0['ROC2']=f2['EBIT'].T/(f2['net_working_capital'].T+f2['fixed_asset_ratio'].T
              *f2['net_operate_cash_flow_to_asset'].T/f2['net_operate_cash_flow_ttm'].T)
    p0['ROC3']=f3['EBIT'].T/(f3['net_working_capital'].T+f3['fixed_asset_ratio'].T
              *f3['net_operate_cash_flow_to_asset'].T/f3['net_operate_cash_flow_ttm'].T)
    p0['ROC4']=f4['EBIT'].T/(f4['net_working_capital'].T+f4['fixed_asset_ratio'].T
              *f4['net_operate_cash_flow_to_asset'].T/f4['net_operate_cash_flow_ttm'].T)
    pa['ROC']=((p0['ROC0']+1)*(p0['ROC1']+1)*(p0['ROC2']+1)*(p0['ROC3']+1)*(p0['ROC4']+1))**(1.0/5)-1
    #FCFA现金流，.T.iloc[:,0]
    #聚宽因子的重要用法，f0['net_operate_cash_flow_ttm'].T.iloc[:,0]
    p0['FCFA']=(f0['net_operate_cash_flow_ttm'].T.iloc[:,0]-p0['fix_intan_other_asset_acqui_cash'])
    p1['FCFA']=(f1['net_operate_cash_flow_ttm'].T.iloc[:,0]-p1['fix_intan_other_asset_acqui_cash'])
    p2['FCFA']=(f2['net_operate_cash_flow_ttm'].T.iloc[:,0]-p2['fix_intan_other_asset_acqui_cash'])
    p3['FCFA']=(f3['net_operate_cash_flow_ttm'].T.iloc[:,0]-p3['fix_intan_other_asset_acqui_cash'])
    p4['FCFA']=(f4['net_operate_cash_flow_ttm'].T.iloc[:,0]-p4['fix_intan_other_asset_acqui_cash'])
    pa['FCFA']=(p0['FCFA']+p1['FCFA']+p2['FCFA']+p3['FCFA']+p4['FCFA'])/p0['total_sheet_owner_equities']
    #MG利润增长率
    pa['MG']=((f0['DEGM'].T+2)*(f1['DEGM'].T.values+2)*(f2['DEGM'].T.values+2)*(f3['DEGM'].T.values+2)*(f4['DEGM'].T.values+2))**(1.0/5)-1
    #MS毛利稳定性
    #data = {f0['gross_income_ratio'].T.iloc[:,0],f2['gross_income_ratio'].T.iloc[:,0]}
    #dfd = pd.DataFrame(data)  #简单的pandas创建方法
    #print dfd
    G_A = get_fundamentals(q, date=end)
    G_A.index = G_A.code.values
    G_A['gross_income_ratio0']=f0['gross_income_ratio'].T.iloc[:,0]
    G_A['gross_income_ratio1']=f1['gross_income_ratio'].T.iloc[:,0]
    G_A['gross_income_ratio2']=f2['gross_income_ratio'].T.iloc[:,0]
    G_A['gross_income_ratio3']=f3['gross_income_ratio'].T.iloc[:,0]
    G_A['gross_income_ratio4']=f4['gross_income_ratio'].T.iloc[:,0]
    del G_A['code'],G_A['fix_intan_other_asset_acqui_cash'],G_A['total_sheet_owner_equities']
    pa['MS']=((f0['gross_income_ratio'].T.iloc[:,0]+f1['gross_income_ratio'].T.iloc[:,0]+f2['gross_income_ratio'].T.iloc[:,0]+f3['gross_income_ratio'].T.iloc[:,0]+f0['gross_income_ratio'].T.iloc[:,0])/5.0
            /np.std(G_A,axis=1))#按行求标准差
    #计算排名
    p_pa=pd.DataFrame(index=pa.index, columns = [])
    p_pa['p_ROAEBITTTM']=pa['ROAEBITTTM'].rank(ascending = True,method='dense')/len(stocks)
    p_pa['p_ROC']=pa['ROC'].rank(ascending = True,method='dense')/len(stocks)
    p_pa['p_FCFA']=pa['FCFA'].rank(ascending = True,method='dense')/len(stocks)
    p_pa['p_MS']=pa['MS'].rank(ascending = True,method='dense')/len(stocks)
    p_pa['p_MG']=pa['MG'].rank(ascending = True,method='dense')/len(stocks)
    MG_MS=pd.DataFrame(p_pa, columns = ['p_MS', 'p_MG'])
    p_pa['p_MM']=MG_MS.max(axis=1)
    del p_pa['p_MM'],p_pa['p_MS']#MG毛利增速更好,比MS毛利稳定性好
    p_pa['p_fp']=p_pa.mean(axis=1)
    return p_pa['p_fp']
#2财务实力
#2.1当前盈利能力
def get_CP(stocks,end):
    #暂用进度ROA
    q = query(valuation.code,cash_flow.fix_intan_other_asset_acqui_cash,
        balance.total_sheet_owner_equities
            ).filter(valuation.code.in_(g.all_stocks))
    p1 = get_fundamentals(q, date=end)
    p1.index = p1.code.values
    del p1['code']
    #暂用经营现金流代替自由现金流
    f0 = get_factor_values(securities=stocks, factors=['roa_ttm','net_operate_cash_flow_ttm'],
         end_date=end,count=1)
    p1['FCFTA']=(f0['net_operate_cash_flow_ttm']).T
    p1['FCFTA']=(p1['FCFTA']-p1['fix_intan_other_asset_acqui_cash'])/p1['total_sheet_owner_equities']
    p1['roa_ttm']=(f0['roa_ttm']).T
    p1['ACCRUAL']=p1['FCFTA']-p1['roa_ttm']
    p1.roa_ttm[p1['roa_ttm']>0]=1
    p1.roa_ttm[p1['roa_ttm']<0]=0
    p1.FCFTA[p1['FCFTA']>0]=1
    p1.FCFTA[p1['FCFTA']<0]=0
    p1.ACCRUAL[p1['ACCRUAL']>0]=1
    p1.ACCRUAL[p1['ACCRUAL']<0]=0
    del p1['fix_intan_other_asset_acqui_cash'],p1['total_sheet_owner_equities']
    #print p1
    return p1
#2.2稳定指标
def get_STAB(stocks,end,year1):
    q = query(valuation.code,balance.total_non_current_assets,
    balance.total_assets,balance.total_current_assets,
    balance.total_current_liability
            ).filter(valuation.code.in_(g.all_stocks))
    p0 = get_fundamentals(q, date=end)
    p1 = get_fundamentals(q, date=year1)
    p0.index = p0.code.values
    p1.index = p1.code.values
    p0['LEVER']=p1['total_non_current_assets']/p1['total_assets']-p0['total_non_current_assets']/p0['total_assets']
    p0.LEVER[p0['LEVER']>0]=1
    p0.LEVER[p0['LEVER']<0]=0
    p0['LIQUID']=p0['total_current_assets']/p0['total_current_liability']-p1['total_current_assets']/p1['total_current_liability']
    p0.LIQUID[p0['LIQUID']>0]=1
    p0.LIQUID[p0['LIQUID']<0]=0
    #NEQISS股份净发行
    
    del p0['code'],p0['total_non_current_assets'],p0['total_assets'] 
    del p0['total_current_assets'],p0['total_current_liability']  
    #print p0
    return p0
#2.3近期运营改进指标
def get_ROI(stocks,end,year1):
    q = query(valuation.code,cash_flow.fix_intan_other_asset_acqui_cash,
        balance.total_sheet_owner_equities
            ).filter(valuation.code.in_(g.all_stocks))
    p0 = get_fundamentals(q, date=end)
    p1 = get_fundamentals(q, date=year1)    
    p0.index = p0.code.values
    p1.index = p1.code.values
    pa = pd.DataFrame(index=p0.code.values,columns=['D_roa_ttm'])
    f0 = get_factor_values(securities=stocks, factors=['roa_ttm','net_operate_cash_flow_ttm',
        'gross_income_ratio','total_asset_turnover_rate'],end_date=end,count=1)
    f1 = get_factor_values(securities=stocks, factors=['roa_ttm','net_operate_cash_flow_ttm',
        'gross_income_ratio','total_asset_turnover_rate'],end_date=year1,count=1)
    p0['roa_ttm']=f0['roa_ttm'].T
    p1['roa_ttm']=f1['roa_ttm'].T
    pa['D_roa_ttm']=p0['roa_ttm']-p1['roa_ttm']
    pa.D_roa_ttm[pa['D_roa_ttm']>0]=1
    pa.D_roa_ttm[pa['D_roa_ttm']<0]=0
    p0['FCFTA']=(f0['net_operate_cash_flow_ttm']).T
    p0['FCFTA']=(p0['FCFTA']-p0['fix_intan_other_asset_acqui_cash'])/p0['total_sheet_owner_equities']
    p1['FCFTA']=(f1['net_operate_cash_flow_ttm']).T
    p1['FCFTA']=(p1['FCFTA']-p1['fix_intan_other_asset_acqui_cash'])/p1['total_sheet_owner_equities']
    pa['D_FCFTA']=p0['FCFTA']-p1['FCFTA']
    pa.D_FCFTA[pa['D_FCFTA']>0]=1
    pa.D_FCFTA[pa['D_FCFTA']<0]=0
    p0['gross_income_ratio']=f0['gross_income_ratio'].T
    p1['gross_income_ratio']=f1['gross_income_ratio'].T
    pa['D_gross_income_ratio']=p0['gross_income_ratio']-p1['gross_income_ratio']
    pa.D_gross_income_ratio[pa['D_gross_income_ratio']>0]=1
    pa.D_gross_income_ratio[pa['D_gross_income_ratio']<0]=0
    p0['total_asset_turnover_rate']=f0['total_asset_turnover_rate'].T
    p1['total_asset_turnover_rate']=f1['total_asset_turnover_rate'].T
    pa['D_total_asset_turnover_rate']=p0['total_asset_turnover_rate']-p1['total_asset_turnover_rate']
    pa.D_total_asset_turnover_rate[pa['D_total_asset_turnover_rate']>0]=1
    pa.D_total_asset_turnover_rate[pa['D_total_asset_turnover_rate']<0]=0
    #print pa
    return pa
#3综合

#近3年ROE函数
def get_3y(stocks,end,year1,year2):
    #f = pd.DataFrame(index=[], data=0, columns=[])#创建一个DataFrame
    f = get_factor_values(securities=g.all_stocks, factors=['roe_ttm'],
             end_date=end,count=1)['roe_ttm']
    f=f.append(get_factor_values(securities=g.all_stocks, factors=['roe_ttm'],
             end_date=year1,count=1)['roe_ttm'])
    f=f.append(get_factor_values(securities=g.all_stocks, factors=['roe_ttm'],
             end_date=year2,count=1)['roe_ttm'])
    f.loc['ave'] = f.apply(lambda x: x.sum(),axis=0) / 3
    f=f*100
    return f

#因子
#资产收益率变化（△ROA）
def get_ROA(stocks, day1):
    q = query(valuation.code,indicator.roa
            ).filter(valuation.code.in_(g.all_stocks))
    p1 = get_fundamentals(q, date=day1)
    day2 = day1 - datetime.timedelta(91)#91天不太准确,多组数据合并
    p2 = get_fundamentals(q, date=day2)
    pa = pd.DataFrame(index=p1.code.values,columns=['ROAC'])
    pa['ROAC'] = p1['roa'].values - p2['roa'].values
    #print pa
    return pa

#营收同比增长率变化（△inc)
def get_OR(stocks,end,year1):
    #get_factor_values(securities=stocks,factors=['net_profit_ttm','non_recurring_gain_loss'],
    #start_date='2017-01-01', end_date=day1)
    q = query(valuation.code,income.total_operating_revenue
            ).filter(valuation.code.in_(g.all_stocks))
    p1 = get_fundamentals(q, date=end)
    p2 = get_fundamentals(q, date=year1)
    p1=pd.merge(p1,p2,on='code',how='outer').set_index('code')
    p1 = p1.fillna(0)
    p1['OR'] = p1['total_operating_revenue_x'] / p1['total_operating_revenue_y']
    return p1['OR']
# 26VAR
# 波动，股票收盘价方差
def get_df_VAR(stock_list, end):
    # 获取价格数据,当前到21天前一共22行，与之前get_price不同，没有使用转置，行为股票代码
    # 列为日期，上边为较早最后为较晚
    df_price_info_between_1shift = get_price(list(stock_list), 
                       count = 251, 
                       end_date = end, 
                       frequency = 'daily', 
                       fields = 'close')['close']
    # 生成一个空得列
    x =[]
    # 计算日回报率为前一天收盘价/当天收盘价 - 1
    for i in range(0, 250):
        x.append(df_price_info_between_1shift.iloc[ i + 1 ]
                 / df_price_info_between_1shift.iloc[ i ] - 1.000)
    # 进行转置
    df_VAR_info = pd.DataFrame(x).T
    df_VAR_info=df_VAR_info.fillna(1)
    #print df_VAR_info
    # 生成方差
    df_VAR_info['VAR'] = 1/df_VAR_info.std(axis = 1, skipna = True) 
    # 生成新的DataFrame
    df_VAR = pd.DataFrame(df_VAR_info['VAR'])
    return df_VAR
#4
# 某一日的前shift个交易日日期 
# 输入：date为datetime.date对象(是一个date，而不是datetime)；shift为int类型
# 输出：datetime.date对象(是一个date，而不是datetime)
def shift_trading_day(date,shift):
    # 获取所有的交易日，返回一个包含所有交易日的 list,元素值为 datetime.date 类型.
    tradingday = get_all_trade_days()
    # 得到date之后shift天那一天在列表中的行标号 返回一个数
    shiftday_index = list(tradingday).index(date)+shift
    # 根据行号返回该日日期 为datetime.date类型
    return tradingday[shiftday_index]
# 21MTM3
# 三个月动能，输入stock_list, context, asc = True/False
# m为m天时间间隔
def get_df_MTM3(stock_list,end,n,m):
    #(n+61)天前日期
    days_before= shift_trading_day(end, shift = -(n+m))
    #(n+61)天前价格
    days_before_price = get_price(list(stock_list), start_date=days_before, 
            end_date=days_before,frequency='daily',fields='close')['close'].T
    #n天前的日期(较新的日期)
    days= shift_trading_day(end, shift = -n)
    # # n天前的价格
    days_price=get_price(list(stock_list), start_date=days, 
            end_date=days, frequency='daily', fields='close')['close'].T
    #n天这段时间的收益率,Series
    Series_mtm3=(days_price.iloc[:,0]-days_before_price.iloc[:,0])/days_before_price.iloc[:,0]
    # 生成dataframe格式
    df_MTM3 = pd.DataFrame({'MTM3':Series_mtm3})
    df_MTM3=df_MTM3.fillna(0)
    #排序打分
    #df_MTM3['MTM3'] = df_MTM3['MTM3'].rank(ascending = True, method = 'dense')
    # 排序给出排序打分，MTM3
    return df_MTM3
def get_index_MTM(index,end,n):
    #(n+61)天前日期
    days_before= shift_trading_day(end, shift = -(n+61))
    #(n+61)天前价格
    days_before_price=get_price(index, start_date=days_before, 
            end_date=days_before,frequency='daily',fields='close')['close'].T
    #n天前的日期(较新的日期)
    days = shift_trading_day(end, shift = -n)
    #n天前的价格
    days_price=get_price(index,start_date=days, end_date=days, 
                       frequency='daily', fields='close')['close'].T
    #n天这段时间的收益率,Series
    Series_mtm3=(days_price.values-days_before_price.values)/days_before_price.values
    return Series_mtm3
'''
================================================================================
每天收盘后
================================================================================
'''
#每天收盘后要做的事情
def after_trading_end(context):
    return

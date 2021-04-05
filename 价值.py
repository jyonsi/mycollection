# 导入函数库
from jqdata import *
import pandas as pd
import datetime
#from datetime import datetime
import time
import math
import datetime
import copy

import pandas as pd
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0001, close_commission=0.0003, min_commission=5), type='stock')
    
    #设定全局变量
    g.year_num = 3
    g.revenue_thre = 19
    g.profit_thre = 10
    g.buylist = None
    g.buylist_this_month = None
    g.buylist_ss = None
    g.buylist_sum = []
    g.max_total_value = 0
    
    #设定年化预期收益率，计算每月预期收益率 
    #当实际上每月收益率高于设定收益率x倍时，认为市场过热，减少股票池总市值至预期值
    #当实际上每月收益率低于设定收益率1/x倍时，认为市场过冷，增加股票池总市值至预期值
    g.last_month_total_value = context.portfolio.total_value
    g.set_return_rate = 18 
    g.set_return_rate_month = math.pow((100+g.set_return_rate)/100, 1/12 )-1
    
    
    
    #寻找股票
    #   第一步：找股票
    #   第二部：鉴别股票
    run_monthly(find_stocks, monthday=1, time='10:10')
    
    
     #择时卖出
    run_monthly(select_right_time_sell,monthday=3,time='10:10')
    
    
    #择时买入
   #1、一个月内最低点上涨10%买入
    run_monthly(select_right_time_buy,monthday=6,time='10:10')
    
    #市场过冷过热矫正
    
    run_monthly(hot_cold_correct_frame,monthday=9,time='10:10')
    
    #进行资产平衡
    run_monthly(assets_balance_frame,monthday=12,time='10:10')
    
    #run_daily(assets_balance_frame,time='every_bar')
    
    # 测试区域
    #select_stocks_list =  select_stocks(context)
    # print(select_stocks_list['code'])
    # print(select_stocks_list['name'])
    # print(len(select_stocks_list))
    
    #run_daily(period,time='every_bar')
    #run_monthly(find_stocks, monthday=1, time='10:30')
    #stock_code = '000603.XSHG'
    #check_stock(context, stock_code)
    #profit_margin = get_enterprise_gross_profit_margin_single_enter(context, stock_code)
    #print(profit_margin)
    #find_stocks(context)
    #select_right_time_buy(context)
    #print(g.buylist)
   # run_monthly(func, monthday, time='9:30', reference_security, force=False)

#获取年份列表
def get_recent_year_list(context, num):
    currentYear = context.current_dt.year
    year_list = list(range(currentYear-1, currentYear-num-1, -1))
    return year_list

#获取当前时间的股票列表
def get_stocks_list(context):
    date = context.current_dt.strftime("%Y-%m-%d")
    stocks_list = get_all_securities(['stock'], date=date)
    stocks_list = stocks_list[~stocks_list.display_name.str.contains('\*|ST|退', regex=True)]
    stock_list = list(stocks_list.index)
    return list(stocks_list.index)
    
def get_stocks_name(context):
    date =context.current_dt.strftime("%Y-%m-%d")
    stocks_list = get_all_securities(['stock'], date=date)
    stock_name = stocks_list[['display_name']]
    stock_name =stock_name.reset_index(drop = False)
    stock_name = stock_name.rename(columns = {"index": "code"})
    return stock_name
    #print(stocks_list.columns)
    
#获取最近一年的毛利率
def get_enterprise_gross_profit_margin(stock_list, statDateYear, gross_profit_margin_thre=20):
    df = get_fundamentals(query(
          indicator.code,  indicator.gross_profit_margin
      ).filter(
          indicator.gross_profit_margin > gross_profit_margin_thre,
          indicator.code.in_(stock_list)    
      ).order_by(
          # 按市值降序排列
          indicator.gross_profit_margin.desc()
      ).limit(
          # 最多返回100个
          4000
      ),
     statDate= statDateYear)
    return df
    
    
#获取某只股票最近一个季度的毛利率
def get_enterprise_gross_profit_margin_single_enter(context, stock_code):
    current_day = context.current_dt.strftime("%Y-%m-%d")
    #current_day = "2008-08-04"
    #print(current_day)
    #print(stock_code)
    df = get_history_fundamentals(stock_code, fields=[indicator.gross_profit_margin], 
        watch_date=current_day,  count=1, interval='1q', stat_by_year=False)
    #print(df)
    if len(df)>0:
        gross_profit_margin_single_enter = list(df['gross_profit_margin'])[0]
        #print(stock_code+"在"+current_day+"的毛利:"+str(gross_profit_margin_single_enter))
    else:
        gross_profit_margin_single_enter = 40
        #print("未找到"+stock_code+"在"+current_day+"的毛利,设为默认值40")
        
    
    return gross_profit_margin_single_enter
    
def check_stocks_gross_profit_margin(context, stock_code,gross_profit_margin=10):
    gross_profit_margin_single_enter = get_enterprise_gross_profit_margin_single_enter(context, stock_code)
    if gross_profit_margin_single_enter > gross_profit_margin:
        return True
    else:
        return False
    
    
#获取股票的列表的营收增长率
def  get_enterprise_revenue(stock_list, statDateYear,  revenue_thre, profit_thre=-0.1):
    df = get_fundamentals(query(
          indicator.code,  indicator.inc_total_revenue_year_on_year
      ).filter(
          indicator.inc_total_revenue_year_on_year > revenue_thre,
          indicator.inc_net_profit_year_on_year > profit_thre,
          indicator.code.in_(stock_list)    
      ).order_by(
          # 按市值降序排列
          indicator.inc_total_revenue_year_on_year.desc()
      ).limit(
          # 最多返回100个
          4000
      ),
     statDate= statDateYear)
    return df

#获取最近几年营收增长大于阈值的企业
def get_enterprice_revenue_recent_year(context, year_num, revenue_thre, profit_thre):
    year_list = get_recent_year_list(context, year_num)
    #输入研究的当前节点
    stock_list = get_stocks_list(context)
    df_sum = None
    for year in year_list:
        df_current_year = get_enterprise_revenue(stock_list, year,  revenue_thre, profit_thre)
        #print(year, "年数据量大小:", len(df_current_year) )
        new_revenue_name = str(year) + "_inc_total_revenue_year_on_year" 
        df_current_year = df_current_year.rename(columns = {"inc_total_revenue_year_on_year": new_revenue_name})
        if df_sum is None:
            df_sum = df_current_year
        else:
            df_sum = pd.merge(df_sum, df_current_year)
        #print("合并数据量大小：",len(df_sum) )
    
    #获取最近一年的毛利率
    gross_profit_margin = get_enterprise_gross_profit_margin(stock_list, year_list[0], gross_profit_margin_thre=20)
    df_sum = pd.merge(df_sum, gross_profit_margin)
    #print("毛利率筛选之后数据量大小：",len(df_sum) )
    #df_sum.to_csv('fundamentaldfsum.csv',encoding = 'utf_8_sig')

    #增加公司名称
    stocks_name = get_stocks_name(context)
    #print(stocks_name)
    df_sum = pd.merge(df_sum, stocks_name)
    stock_name_tmp = df_sum.display_name 
    df_sum = df_sum.drop('display_name',axis=1) 
    df_sum.insert(1,'name',stock_name_tmp)
    return df_sum

#选择股票
def select_stocks(context):
    df = get_enterprice_revenue_recent_year(context, g.year_num, g.revenue_thre, g.profit_thre)
    return df
    
#检验多股票
def check_stocks(context, buylist):
    #检验历年的财报， 营收
    current_day = context.current_dt.strftime("%Y-%m-%d")
    i = 0
    buylist_check = []
    for stock_code in buylist:
        i = i + 1
        #print('进度:'+str(i)+'/'+str(len(buylist)))
        check_flag = check_stock(context, stock_code)
        check_margin = check_stocks_gross_profit_margin(context, stock_code,gross_profit_margin=15)
        if check_flag > 0:
            #print('未增长季度个数为：'+str(check_flag)+'，移除：'+stock_code)
            pass
        elif check_margin == False:
            #print('最近一季财报毛利率低于阈值：'+str(check_flag)+'，移除：'+stock_code)
            pass
        else:
            buylist_check.append(stock_code)
    return buylist_check
    
def check_stock(context, stock_code):
    current_day = context.current_dt.strftime("%Y-%m-%d")
    stock_info  = get_security_info(stock_code)
    stock_start_year = stock_info.start_date.year
    current_year = datetime.datetime.strptime(current_day, "%Y-%m-%d").year
    df = get_fundamentals(query(indicator).filter(indicator.code == stock_code),date=current_day)
    last_mom = list(df['statDate'])[0]
    current_loc = datetime.datetime.strptime(last_mom, "%Y-%m-%d").month/3
    current_loc = int(current_loc)
    df_fundamentals = None
    for year in range(current_year, stock_start_year-1,-1):
        for i in range(current_loc, 0,-1):
            fundamental_date = str(year)+'q'+str(i)
            #print(fundamental_date)
            df = get_fundamentals(query(indicator).filter(indicator.code == stock_code),
                 statDate= fundamental_date)
            #df
            if df_fundamentals is None:
                df_fundamentals = df
            else:
                df_fundamentals = df_fundamentals.append(df)
        current_loc = 4
    #df_fundamentals
    df_fundamentals['revenue_year_flag'] = df_fundamentals['inc_total_revenue_year_on_year'].apply(lambda x: -1 if x <0 else 0)
    df_fun_reve_list = list(df_fundamentals['revenue_year_flag'])
    #print(df_fundamentals[['statDate.1','inc_total_revenue_year_on_year', 'revenue_year_flag']])
    df_fun_reve_list = list(df_fundamentals['revenue_year_flag'])
    stock_reve_count = df_fun_reve_list.count(-1)
    #print(stock_code+'营收未增长季度计数：', stock_reve_count)
    return stock_reve_count

#择时买入
   #1、一个月内最低点上涨10%买入

#择时卖出
   #1、持有收益率小于-10%卖出
   #2、 比最近的一段时间的最高点下降-10卖出
   #

def find_stocks(context):
    # 代码：找出市值排名最小的前stocksnum只股票作为要买入的股票
    # 获取上证指数和深证综指的成分股代码并连接，即为全A股市场所有股票的股票代码
    # 用加号可以连接两个list
    stock_start_month =context.current_dt.month
    print("月份：",stock_start_month)
    if stock_start_month != 8:
        print("跳过")
        return
    else:
        print("是八月")
    
    df =  select_stocks(context)
    # 选取股票代码并转为list
    g.buylist_ss = list(df['code'])

    
    #计算应该买的股票总和，历史的股票+加上当期的寻找到的
    #将不合格的筛选出去
    g.buylist_sum =  g.buylist_sum + g.buylist_ss
    print("筛选前股票数量："+str(len(g.buylist_sum)))
    print(g.buylist_sum)
    g.buylist_sum = check_stocks(context, g.buylist_sum)
    print("筛选后股票数量："+str(len(g.buylist_sum)))
    print(g.buylist_sum)
    return  
    
#择时买入，对于每只股票在一个月内的最低点上涨10买入
#3.0版本 直接买 不等待
def select_right_time_buy(context):
    print("开始买入")
    g.buylist_this_month = [i for i in g.buylist_sum if i not in context.portfolio.positions.keys()]
    g.buylist_this_month = check_stocks(context, g.buylist_this_month)
    print("本月要买"+str(len(g.buylist_this_month)))
    print(g.buylist_this_month )
    stock_num = len(g.buylist_this_month)
    if stock_num < 1:
        g.buylist_this_month = copy.deepcopy(g.buylist_sum)
        stock_num = len(g.buylist_this_month)
        if stock_num <1:
            return
    position_per_stk = context.portfolio.available_cash / stock_num
    position_per_stk_the = context.portfolio.total_value / 10
    position_per_stk = position_per_stk  if position_per_stk < position_per_stk_the else position_per_stk_the
    for stock_code in g.buylist_this_month:
        close_price_30 = attribute_history(stock_code, 30, '1d', ['close'])
        #print(close_price_30)
        min_price_30 = close_price_30['close'].min()
        current_price  = close_price_30['close'][-1]
        if current_price/ min_price_30 - 1 >0.1 :
            position_per_stk = position_per_stk if position_per_stk/current_price > 100 else current_price*200
            order_bak = order_value(stock_code, position_per_stk)
            if order_bak is not None:
                g.buylist_this_month.remove(stock_code) 
                #print('买入'+str(position_per_stk)+'的'+stock_code)
            else:
                #print('买入'+str(position_per_stk)+'的'+stock_code+'失败')
                pass
#择时卖出
   #1、3.0版本 卖一家公司的理由就是公司的基本面发生了变化
def select_right_time_sell(context):
    g.buylist_sum = check_stocks(context, g.buylist_sum)
    for stock_code in context.portfolio.positions:
        if stock_code not in g.buylist_sum:
            order_bak = order_target_value(stock_code, 0)
            if order_bak is not None:
                #print('基本面发生变化全部卖出：'+stock_code)
                pass
    return 


#资产再平衡
def assets_balance(context):
    stock_num = len(context.portfolio.positions)
    position_per_stk_target = context.portfolio.positions_value  /(stock_num+1)   # 1代表现金份额
    print("进行资产平衡")
    for stock_code in context.portfolio.positions:
        order_bak = order_target_value(stock_code, position_per_stk_target)
        if order_bak is not None:
            print(stock_code + "调整至"+ str(position_per_stk_target))
    return 
  


#资产再平衡框架
def assets_balance_frame(context):
    value_list = [0]
    for stock_code in context.portfolio.positions:
        value_list.append( context.portfolio.positions[stock_code].value)
    value_max = max(value_list)
    #如果某类资产超过总资产的20%，则进行资产再平衡
    if value_max > context.portfolio.total_value*0.2:
            assets_balance(context)
    
    # g.max_total_value = context.portfolio.total_value if context.portfolio.total_value > g.max_total_value else  g.max_total_value          
    # #如果产生了一次10%的回撤，进行一次资产再平衡
    # if context.portfolio.total_value / g.max_total_value < 0.9:
    #     assets_balance(context)
    #     g.max_total_value = context.portfolio.total_value
    return 



def hot_cold_correct(context, coefficient):
    for stock_code in context.portfolio.positions:
        stk_target =  context.portfolio.positions[stock_code].value * coefficient
        #stk_target = round(stk_target/100)*100
        order_bak = order_target_value(stock_code, stk_target)
        if order_bak is not None:
            print(stock_code + "调整至"+ str(stk_target))
    return 

#市场过冷过热调节
def hot_cold_correct_frame(context):
    print("设定月收益率"+str(g.set_return_rate_month))
    print("资产增值情况"+str(context.portfolio.total_value / g.last_month_total_value))
    if context.portfolio.total_value / g.last_month_total_value  -1 > g.set_return_rate_month * 4:
        print("市场过热")
        coe = g.last_month_total_value / context.portfolio.total_value * (1+g.set_return_rate_month)
        hot_cold_correct(context, coe)
    elif context.portfolio.total_value / g.last_month_total_value  -1  < g.set_return_rate_month * 1/ 4:
        print("市场过冷")
        coe = g.last_month_total_value / context.portfolio.total_value * (1+g.set_return_rate_month)
        hot_cold_correct(context, coe)
    else:
        print("市场冷热正常不进行调节")
        pass
    g.last_month_total_value = context.portfolio.total_value
            
        
    
    

        
    

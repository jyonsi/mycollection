# 克隆自聚宽文章：https://www.joinquant.com/post/29173
# 标题：价值投资策略-大盘择时
# 作者：叶松

# 克隆自聚宽文章：https://www.joinquant.com/post/29173
# 标题：价值投资策略-大盘择时
# 作者：叶松

# 克隆自聚宽文章：https://www.joinquant.com/post/25496
# 标题：收益狂飙，年化收益100%，11年1700倍，绝无未来函数
# 作者：jqz1226

# 导入函数库
from jqdata import *
from kuanke.wizard import *


# 初始化函数，设定基准等等
def initialize(context):

    # 设定沪深300作为基准
    g.base = '000300.XSHG'
    set_benchmark(g.base)
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    # set_option("avoid_future_data", True)  # 避免未来数据

    # 股票池
    # 中小板 "399101.XSHE"
    g.security_universe_index = "000300.XSHG"
    g.buy_stock_count = 5

    g.risk_control = RiskControl(g.base)

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001,
                             open_commission=0.0003,	
                             close_commission=0.0003,
                             min_commission=5),
                   type='stock')
    # before_market_open(context)
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 开盘前运行
    run_monthly(before_market_open,1,time='before_open', reference_security='000300.XSHG')
    # 定时运行
    run_weekly(trade,1, time='14:40', reference_security=g.base)
    run_weekly(stop_loss,3,time='open', reference_security='000300.XSHG')
    #止盈止损
    run_weekly(stop_loss,3,time='open', reference_security='000300.XSHG')

## 开盘前运行函数     
def before_market_open(context):

    #获取满足条件的股票列表
    g.stock_list = get_stock_list(context)
    check_out_lists = g.stock_list
    # 过滤: 三停（停牌、涨停、跌停）及st,*st,退市
    check_out_lists = filter_st_stock(check_out_lists)
    check_out_lists = filter_limitup_stock(context, check_out_lists)
    check_out_lists = filter_paused_stock(check_out_lists)
    check_out_lists = filter_kc_stock(check_out_lists)
    check_out_lists = filter_blacklist_stock(context,check_out_lists)
    # 取需要的只数
    g.stock_list = get_check_stocks_sort(context,check_out_lists)
    
    print((g.stock_list))
        
## 开盘时运行函数
def trade(context):
    # 买卖
    adjust_position(context, g.stock_list)

    log.info('__'*15)


# 交易
def adjust_position(context, buy_stocks):
    # 交易函数 - 出场
    current_data = get_current_data()
    # 获取 sell_lists 列表
    hold_stock = list(context.portfolio.positions.keys())
    for stock in hold_stock:
        #卖出不在买入列表中的股票
        if stock not in buy_stocks:
            order_target_value(stock,0)   
            log.info('卖出：',current_data[stock].name,stock)
    #买入
    if check_for_benchmark(context):
        Num = g.buy_stock_count - len(context.portfolio.positions)
        buy_lists = buy_stocks[:Num]
        if len(buy_lists) > 0:
            #分配资金
            cash = context.portfolio.available_cash / (len(buy_lists))
            # 进行买入操作
            for stock in buy_lists:
                close_data = attribute_history(stock, 5, '1d', ['close'])
                e_5 = (close_data['close'][-1] -
                       close_data['close'][0]) / close_data['close'][0]
                if current_data[
                        stock].last_price * 120 < cash and not judge_More_average(
                            stock):
                    if not e_5 < -0.1 and stock in g.stock_list:
                        result = order_value(stock, cash)
                        if not result == None:
                            log.info("买入：%s %s" %
                                     (current_data[stock].name, stock))
#止盈止损
def stop_loss(context):
    current_data = get_current_data()
    close_index = attribute_history('000300.XSHG', 5, '1d', ['close'])
    index_5 = (close_index['close'][-1]-close_index['close'][0])/close_index['close'][0]
    
    for security in context.portfolio.positions:
        closeable_amount= context.portfolio.positions[security].closeable_amount
        if closeable_amount:
            close_data = attribute_history(security, 5, '1d', ['close'])
            e_5 = (close_data['close'][-1]-close_data['close'][0])/close_data['close'][0]
            earn = (current_data[security].last_price-context.portfolio.positions[security].avg_cost)/context.portfolio.positions[security].avg_cost
            if earn>.35:
                result = order_target(security, 0)  
                if not result == None:
                    log.info('止赢：%s %s %.2f'%(current_data[security].name,security,earn))
            if e_5<-0.1:
                result = order_target(security, 0)  
                if not result == None:
                    log.info('回撤：%s %s %.2f'%(current_data[security].name,security,earn))
            if earn<-0.10 :
                result = order_target(security, 0) 
                if not result == None:
                    log.info('个股止损0.10 卖出：',security,current_data[security].name,earn)

# 过滤科创
def filter_kc_stock(stock_list):

    return [stock for stock in stock_list if not (stock.startswith('300') or stock.startswith('688'))]
            
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
        if stock in list(context.portfolio.positions.keys())
        or last_prices[stock][-1] <= current_data[stock].high_limit
        or last_prices[stock][-1] >= current_data[stock].low_limit
    ]

    return [
        stock for stock in stock_list
        if stock in list(context.portfolio.positions.keys())
        or last_prices[stock][-1] > current_data[stock].low_limit
    ]


#自定义函数
def check_for_benchmark(context):

    return g.risk_control.check_for_benchmark(context)


#============================================================================================
class RiskControlStatus(Enum):
    RISK_WARNING = 1
    RISK_NORMAL = 2
class RiskControl(object):
    def __init__(self, symbol):
        self.symbol = symbol
        self.status = RiskControlStatus.RISK_NORMAL

    def check_for_ma_rate(self, period, ma_rate_min, ma_rate_max,
                          show_ma_rate):
        ma_rate = self.compute_ma_rate(period, show_ma_rate)
        return (ma_rate_min < ma_rate < ma_rate_max)

    def compute_ma_rate(self, period, show_ma_rate):
        hst = get_bars(self.symbol, period, '1d', ['close'])
        close_list = hst['close']
        if (len(close_list) == 0):
            return -1.0

        if (math.isnan(close_list[0]) or math.isnan(close_list[-1])):
            return -1.0

        period = min(period, len(close_list))
        if (period < 2):
            return -1.0

        #ma = close_list.sum() / len(close_list)
        ma = talib.MA(close_list, timeperiod=period)[-1]
        ma_rate = hst['close'][-1] / ma
        if (show_ma_rate):
            record(mar=ma_rate)

        return ma_rate

    def check_for_rsi(self, period, rsi_min, rsi_max, show_rsi):
        hst = attribute_history(self.symbol, period + 1, '1d', ['close'])
        close = [float(x) for x in hst['close']]
        if (math.isnan(close[0]) or math.isnan(close[-1])):
            return False

        rsi = talib.RSI(np.array(close), timeperiod=period)[-1]
        if (show_rsi):
            record(RSI=max(0, (rsi - 50)))

        return (rsi_min < rsi < rsi_max)

    def check_for_benchmark_v1(self, context):
        could_trade_ma_rate = self.check_for_ma_rate(10000, 0.75, 1.50, True)

        could_trade = False
        if (could_trade_ma_rate):
            could_trade = self.check_for_rsi(90, 35, 99, False)
        else:
            could_trade = self.check_for_rsi(15, 50, 70, False)

        return could_trade

    def check_for_benchmark(self, context):
        ma_rate = self.compute_ma_rate(1000, False)
        if (ma_rate <= 0.0):
            return False

        if (self.status == RiskControlStatus.RISK_NORMAL):
            if ((ma_rate > 2.5) or (ma_rate < 0.30)):
                self.status = RiskControlStatus.RISK_WARNING
        elif (self.status == RiskControlStatus.RISK_WARNING):
            if (0.35 <= ma_rate <= 0.7):
                self.status = RiskControlStatus.RISK_NORMAL

        could_trade = False

        if (self.status == RiskControlStatus.RISK_WARNING):
            #if (self.status == RiskControlStatus.RISK_WARNING) or not(self.check_for_usa_intrest_rate(context)):
            could_trade = self.check_for_rsi(15, 55, 90, False) and self.check_for_rsi(90, 50, 90, False)
            # could_trade = self.check_for_rsi(60, 47, 99, False)
            #record(status=2.5)
        elif (self.status == RiskControlStatus.RISK_NORMAL):
            could_trade = self.check_for_rsi(60, 50, 99, False)
            # could_trade = True
            #record(status=0.7)

        return could_trade
#============================================================================================

def get_check_stocks_sort(context,check_out_lists):
    df = get_fundamentals(query(valuation.circulating_cap,valuation.pe_ratio,valuation.code).filter(valuation.code.in_(check_out_lists)),date=context.previous_date)
    #asc值为0，从大到小
    df = df.sort_values('circulating_cap',ascending=False)
    out_lists = list(df['code'].values)
    return out_lists

def get_stock_list(context):
    temp_list = list(get_all_securities(types=['stock']).index)    
    #剔除停牌股
    all_data = get_current_data()
    temp_list = [stock for stock in temp_list if not all_data[stock].paused]
    #获取多期财务数据
    panel = get_data(context, temp_list,4)
    
    #1.总市值≧市场平均值*1.0。
    # 获取所有股票最新季度的财务数据
    df_mkt = panel.loc[['circulating_market_cap'],3,:]
    df_mkt = df_mkt[df_mkt['circulating_market_cap']>df_mkt['circulating_market_cap'].mean()*1.1]
    l1 = set(df_mkt.index)
    
    #2.最近一季流动比率≧市场平均值（流动资产合计/流动负债合计）。
    df_cr = panel.loc[['total_current_assets','total_current_liability'],3,:]
    #替换零的数值
    df_cr = df_cr[df_cr['total_current_liability'] != 0]
    df_cr['cr'] = df_cr['total_current_assets']/df_cr['total_current_liability']
    df_cr_temp = df_cr[df_cr['cr']>df_cr['cr'].mean()]
    l2 = set(df_cr_temp.index)

    #3.近四季股东权益报酬率（roe）≧市场平均值。
    l3 = {}
    for i in range(4):
        roe_mean = panel.loc['roe',i,:].mean()
        df_3 = panel.iloc[:,i,:]
        df_temp_3 = df_3[df_3['roe']>roe_mean]
        if i == 0:    
            l3 = set(df_temp_3.index)
        else:
            l_temp = df_temp_3.index
            l3 = l3 & set(l_temp)
    l3 = set(l3)

    #4.近3年自由现金流量均为正值。（cash_flow.net_operate_cash_flow - cash_flow.net_invest_cash_flow）
    # #3年经营现金流净额>投资现金流净额1000000
    y = context.current_dt.year
    l4 = {}
    for i in range(1,4):
        log.info('year', str(y-i))
        df = get_fundamentals(query(cash_flow.code,cash_flow.statDate,cash_flow.net_operate_cash_flow , \
                                    cash_flow.net_invest_cash_flow),statDate=str(y-i))
        if len(df) != 0:
            df['FCF'] = df['net_operate_cash_flow']-df['net_invest_cash_flow']
            df = df[df['FCF']>1000000]
            l_temp = df['code'].values
            if len(l4) != 0:
                l4 = set(l4) & set(l_temp)
            else:
                l4 = l_temp
        else:
            continue
    l4 = set(l4)
    #5.近四季营收成长率介于6%至30%（）。    'IRYOY':indicator.inc_revenue_year_on_year, # 营业收入同比增长率(%)
    l5 = {}
    for i in range(4):
        df_5 = panel.iloc[:,i,:]
        df_temp_5 = df_5[(df_5['inc_revenue_year_on_year']>15) & (df_5['inc_revenue_year_on_year']<50)]
        if i == 0:    
            l5 = set(df_temp_5.index)
        else:
            l_temp = df_temp_5.index
            l5 = l5 & set(l_temp)
    l5 = set(l5)
    
    #6.近四季盈余成长率介于8%至50%。(eps比值)
    l6 = {}
    for i in range(4):
        df_6 = panel.iloc[:,i,:]
        df_temp = df_6[(df_6['eps']>0.08) & (df_6['eps']<0.5)]
        if i == 0:    
            l6 = set(df_temp.index)
        else:
            l_temp = df_temp.index
            l6 = l6 & set(l_temp)
    l6 = set(l6)
    
    #7.获取最近一季度PEG，从小到大排序，PEG<1.2,
    PEG_list = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    temp_set = l2 & l3 & l4 & l5 & l6
    df_PEG = get_PEG(context, list(temp_set))
    # 将股票按PEG降序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort_values('peg', ascending=False)
    log.info('PGE列表：'+str(df_sort_PEG))
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
    for i in range(len(df_sort_PEG.index)):
        
        #获取上市时间
        start_date =get_security_info(df_sort_PEG.index[i]).start_date
        end_date=context.previous_date
        
        log.info("开始时间：" +str(start_date))
        log.info("结束时间：" +str(end_date))
        
        #获取PE历史分位
        df = get_PE_HistoryPercentile(df_sort_PEG.index[i],start_date,end_date)
        log.info("PE历史分位：" +str(df))
        
        
        if df_sort_PEG.ix[i,'peg'] < 1.2:
            # if df<75:
            #     PEG_list.append(df_sort_PEG.index[i])
            PEG_list.append(df_sort_PEG.index[i])
    l7 = set(PEG_list)
    log.info('最近一季度PEG<1.2：'+str(len(temp_list)))

    return list(l7)
    # return list(l2 & l3 & l4 & l5 & l6)
    
#去极值（分位数法）  
def winsorize(se):
    q = se.quantile([0.025, 0.975])
    if isinstance(q, pd.Series) and len(q) == 2:
        se[se < q.iloc[0]] = q.iloc[0]
        se[se > q.iloc[1]] = q.iloc[1]
    return se
    
#获取多期财务数据内容
def get_data(context, pool, periods):
    q = query(valuation.code, income.statDate, income.pubDate).filter(valuation.code.in_(pool))
    df = get_fundamentals(q)
    df.index = df.code
    stat_dates = set(df.statDate)
    stat_date_stocks = { sd:[stock for stock in df.index if df['statDate'][stock]==sd] for sd in stat_dates }

    def quarter_push(quarter):
        if quarter[-1]!='1':
            return quarter[:-1]+str(int(quarter[-1])-1)
        else:
            return str(int(quarter[:4])-1)+'q4'

    q = query(valuation.code,valuation.code,valuation.circulating_market_cap,balance.total_current_assets,balance.total_current_liability,\
    indicator.roe,cash_flow.net_operate_cash_flow,cash_flow.net_invest_cash_flow,indicator.inc_revenue_year_on_year,indicator.eps
              )

    stat_date_panels = { sd:None for sd in stat_dates }

    for sd in stat_dates:
        quarters = [sd[:4]+'q'+str(int(int(sd[5:7]) / 3))]
        for i in range(periods-1):
            quarters.append(quarter_push(quarters[-1]))
        nq = q.filter(valuation.code.in_(stat_date_stocks[sd]))
        
        quarters.reverse()
        pre_panel = { quarter:get_fundamentals(nq, statDate=quarter) for quarter in quarters }
        for thing in list(pre_panel.values()):
            thing.index = thing.code.values
        panel = pd.Panel(pre_panel)
        panel.items = list(range(len(quarters)))
        stat_date_panels[sd] = panel.transpose(2,0,1)

    final = pd.concat(list(stat_date_panels.values()), axis=2)
    final = final.dropna(axis=2)

    return final
    
#均线
def judge_More_average(security):
    close_data = attribute_history(security, 5, '1d', ['close'])
    MA5 = close_data['close'].mean()
    close_data = attribute_history(security, 10, '1d', ['close'])
    MA10 = close_data['close'].mean()
    close_data = attribute_history(security, 15, '1d', ['close'])
    MA20 = close_data['close'].mean()
    close_data = attribute_history(security, 25, '1d', ['close'])
    MA30 = close_data['close'].mean()
    if MA5 < MA20 and MA10 < MA30:  #and MA20>MA30 :
        return True
    return False

def shift_trading_day(date, shift):
    '''
    # 某一日的前shift个交易日日期
    # 输入：date为datetime.date对象(是一个date，而不是datetime)；shift为int类型
    # 输出：datetime.date对象(是一个date，而不是datetime)
    '''

    if type(date) is str:
        date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    # 获取所有的交易日，返回一个包含所有交易日的 list,元素值为 datetime.date 类型.
    tradingday = list(get_all_trade_days())

    # 如果找不到，则找最接近的一天
    if not date in tradingday:
        date = [d for d in tradingday if d < date][-1]
    
    # 得到date之后shift天那一天在列表中的行标号 返回一个数
    shiftday_index = tradingday.index(date) - shift

    # 根据行号返回该日日期 为datetime.date类型
    return tradingday[shiftday_index]

## 收盘后运行函数
def after_market_close(context):
    # log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    #得到当天所有成交记录
    trades = get_trades()
    for _trade in list(trades.values()):
        log.info('成交记录：'+str(_trade))
    # log.info('一天结束')
    log.info('————'*15)

#过滤黑名单    
def filter_blacklist_stock(context, stock_list):
    blacklist = get_blacklist()
    return [stock for stock in stock_list if stock not in blacklist]  
    
#获取黑名单    
def get_blacklist():
    blacklist = [
        #业绩造假
        "300269.XSHE","600598.XSHG","002763.XSHE","300117.XSHE","601519.XSHG","000691.XSHE","000922.XSHE","601777.XSHG","300267.XSHE","000636.XSHE",
        "300056.XSHE","300043.XSHE","002181.XSHE","002490.XSHE","000798.XSHE","600710.XSHG","600812.XSHG","300072.XSHE","601717.XSHG","000898.XSHE",
        "600010.XSHG","600983.XSHG","002715.XSHE","300208.XSHE","002569.XSHE","600281.XSHG","600155.XSHG","600250.XSHG","600807.XSHG","002426.XSHE",
        "002667.XSHE","600076.XSHG","300277.XSHE","300268.XSHE","002323.XSHE","000972.XSHE","603777.XSHG","600399.XSHG","300156.XSHE","000820.XSHE",
        "002366.XSHE","002069.XSHE","000806.XSHE","300426.XSHE","002477.XSHE","002147.XSHE","002131.XSHE","600290.XSHG","600614.XSHG","603568.XSHG",
        "300676.XSHE","002239.XSHE","002143.XSHE","002571.XSHE","600666.XSHG","002118.XSHE","002512.XSHE","000018.XSHE","600518.XSHG","600771.XSHG",
        "002751.XSHE","002418.XSHE","600371.XSHG","000533.XSHE","300216.XSHE","002694.XSHE","002183.XSHE","000587.XSHE","300090.XSHE","002450.XSHE",
        "000519.XSHE","002291.XSHE","000792.XSHE","000939.XSHE","002509.XSHE","002018.XSHE","000408.XSHE","600177.XSHG","600485.XSHG","002473.XSHE",
        "600319.XSHG","300238.XSHE","002585.XSHE","000752.XSHE","002045.XSHE","002420.XSHE","002665.XSHE","300392.XSHE","300292.XSHE","603016.XSHG",
        "600745.XSHG","000534.XSHE","300442.XSHE","300256.XSHE","600393.XSHG","600234.XSHG","000995.XSHE","601258.XSHG","002219.XSHE","002622.XSHE",
        "300111.XSHE","002501.XSHE","300518.XSHE","002356.XSHE","002766.XSHE","002147.XSHE","000567.XSHE","002657.XSHE","300086.XSHE","002005.XSHE",
        "600645.XSHG","300069.XSHE","600355.XSHG","300332.XSHE","000056.XSHE","603718.XSHG","000711.XSHE","002693.XSHE","000760.XSHE","300647.XSHE",
        "600358.XSHG","002761.XSHE","300134.XSHE","300601.XSHE","002739.XSHE","600084.XSHG","600381.XSHG","000506.XSHE","000790.XSHE","002568.XSHE",
        "600601.XSHG","603555.XSHG","002112.XSHE","300578.XSHE","300221.XSHE","300077.XSHE","002656.XSHE","300313.XSHE","300215.XSHE","300076.XSHE",
        "002625.XSHE","300055.XSHE","600703.XSHG","600080.XSHG","002650.XSHE","002575.XSHE","002278.XSHE","300664.XSHE","603080.XSHG","600175.XSHG",
        "002502.XSHE","000782.XSHE","300413.XSHE","002021.XSHE","300399.XSHE","600385.XSHG",
        #仿制药
        #"300452.XSHE","000908.XSHE","600420.XSHG","300630.XSHE","002020.XSHE","002653.XSHE","002262.XSHE","600062.XSHG","600380.XSHG","600521.XSHG",
        #"600566.XSHG","601607.XSHG","300003.XSHE",
        #商誉过高
        "600594.XSHG","002464.XSHE","002759.XSHE","000971.XSHE","002354.XSHE","600721.XSHG","002247.XSHE","002113.XSHE","000662.XSHE","002619.XSHE",
        "300143.XSHE","300299.XSHE","600242.XSHG","000976.XSHE","002072.XSHE","000835.XSHE","600898.XSHG","600682.XSHG","002071.XSHE","002647.XSHE",
        "002437.XSHE","000697.XSHE","300431.XSHE","600418.XSHG","002076.XSHE","600079.XSHG","000545.XSHE","000980.XSHE","002621.XSHE","600256.XSHG",
        "600226.XSHG","600146.XSHG","002292.XSHE","300364.XSHE","002445.XSHE","002576.XSHE","600240.XSHG","002602.XSHE","300027.XSHE","002735.XSHE",
        "300050.XSHE","300296.XSHE","300310.XSHE","002359.XSHE","300182.XSHE","600754.XSHG","002382.XSHE","000526.XSHE","000606.XSHE","002316.XSHE",
        "603598.XSHG","300459.XSHE","600136.XSHG","300071.XSHE","601919.XSHG","000981.XSHE","603603.XSHG","300312.XSHE","300344.XSHE",
        #大股东质押
        "000732.XSHE","600225.XSHG","002721.XSHE","002011.XSHE","002226.XSHE","603988.XSHG","300166.XSHE","000662.XSHE","002002.XSHE","000518.XSHE",
        "300266.XSHE","000673.XSHE","000576.XSHE","603032.XSHG","600868.XSHG","002617.XSHE","300116.XSHE","000802.XSHE","002442.XSHE","300688.XSHE",
        "002584.XSHE","002413.XSHE","000040.XSHG","000413.XSHE","002486.XSHE","000793.XSHE","002519.XSHE","300004.XSHE","002700.XSHE","002740.XSHE",
        "300682.XSHE","002240.XSHE","300526.XSHE","600260.XSHG","002708.XSHE","603001.XSHG","300138.XSHE","300432.XSHE","600069.XSHG","000593.XSHE"
        ]
    return blacklist 

#获取PE历史分位
def get_PE_HistoryPercentile(code, start_date, end_date):
    pelist=[]
    
    #获取历史日期
    time_list = pd.date_range(start_date, end_date,freq='M')         # 频率为月
    #遍历日期获取历史PE数据
    for i, d in enumerate(time_list): 
        num=_get_stock_valuation_date([code], d)
        if num>0:
            pelist.append(num)
        
    #获取当前PE
    pe=_get_stock_valuation_date([code],end_date)
    log.info("当前PE" +str(pe))
    
    #pe为负，亏损，实际是很大，因此转化为大的正值且需对应大小关系
    pelist.append(pe)
    pelist.sort()
    cnpe=pelist.index(pe)
    cppe=int((1.0*cnpe)/(1.0*len(pelist))*100)
    log.info("当前pe: ",pe,"；有",cppe,"%  的交易日的估值比当前低。")
    
    return cppe

#PE数据
def _get_stock_valuation_date(stock, date):
    q = query(valuation).filter(valuation.code.in_(stock))
    df = get_fundamentals(q, date)
    if len(df)>0:
        return df['pe_ratio'][0]
    else:
        return -1
    
# 计算股票的PEG值
# 输入：context(见API)；stock_list为list类型，表示股票池
# 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
def get_PEG(context, stock_list): 
    # 查询股票池里股票的市盈率，净利润增长率
    q_PE_G = query(
        valuation.code,
        valuation.pe_ratio,
        indicator.inc_net_profit_year_on_year,
        # indicator.day
        ).filter(valuation.code.in_(stock_list))
    
    df_PE_G = get_fundamentals(q_PE_G)

    # 筛选出成长股：删除市盈率或净利润增长率为负值的股票
    df_Growth_PE_G = pd.DataFrame(df_PE_G[(df_PE_G.pe_ratio > 0) & (df_PE_G.pe_ratio < 88)\
                & (df_PE_G.inc_net_profit_year_on_year > 10) & (df_PE_G.inc_net_profit_year_on_year < 200)])

    # 去除无效数据，以及使用code来索引
    df_Growth_PE_G.dropna()
    df_Growth_PE_G.set_index(['code'], 1, inplace=True)
    
    # PEG值 = 市盈率TTM(PE) / 收益增长率(G)
    df_Growth_PE_G['peg'] = df_Growth_PE_G['pe_ratio'] / df_Growth_PE_G['inc_net_profit_year_on_year']
    return pd.DataFrame(df_Growth_PE_G['peg'])  

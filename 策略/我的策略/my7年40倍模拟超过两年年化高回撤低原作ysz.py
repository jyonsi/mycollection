# 克隆自聚宽文章：https://www.joinquant.com/post/30481
# 标题：7年40倍，模拟超过两年，年化高回撤低
# 作者：quakecat

from __future__ import division
# from bwlist import *
import math

def initialize(context):
    set_option('use_real_price', True)
    set_option('avoid_future_data', True)
    # 交易设置
    g.stocknum = 3 # 理想持股数量
    g.bearpercent = 0.3 # 熊市仓位
    g.bearposition = True # 熊市是否持仓
    g.sellrank = 10 # 排名多少位之后(不含)卖出
    g.buyrank = 9 # 排名多少位之前(含)可以买入

    # 初始筛选
    g.tradeday = 300 # 上市天数
    g.increase1d = 0.087 # 前一日涨幅
    g.tradevaluemin = 0.01 # 最小流通市值 单位（亿）
    g.tradevaluemax = 1000 # 最大流通市值 单位（亿）
    g.pbmin = 0.01 # 最小市净率
    g.pbmax = 30 # 最大市净率

    # 排名条件及权重，正数代表从小到大，负数表示从大到小
    # 各因子权重：总市值，流通市值，最新价格，5日平均成交量，60日涨幅
    g.weights = [5,5,8,4,10]
    
    # 配置择时
    g.MA = ['000001.XSHG', 10] # 均线择时
    g.choose_time_signal = True # 启用择时信号
    g.threshold = 0.003 # 牛熊切换阈值
    g.buyagain = 20 # 再次买入的间隔时间

    g.isbull = False # 是否牛市
    g.chosen_stock_list = [] # 存储选出来的股票
    g.nohold = True # 空仓专用信号
    g.sold_stock = {} # 近期卖出的股票及卖出天数
    # g.indus_list = ['801010','801080','801120','801140','801150','801210','801710','801750','801760','801780','801790','801880']
    g.inv_value_max = context.portfolio.total_value #市值峰值

    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    run_daily(gogogo, '14:50')
    run_daily(mybuy, '14:55')
    # run_daily(stop_loss,'09:30', reference_security='000300.XSHG')
 
def before_market_open(context):
    tmp_sold_stock = {}
    for stock in g.sold_stock:
        if g.sold_stock[stock] + 1 < g.buyagain:
            tmp_sold_stock[stock] = g.sold_stock[stock] + 1
    #
    g.sold_stock = tmp_sold_stock
    g.chosen_stock_list = get_stock_list(context)

def get_stock_list(context):
    # 过滤掉新股
    check_date = context.previous_date - datetime.timedelta(days=g.tradeday+1)
    stock_list = list(get_all_securities(date=check_date).index)

    q = query(
        valuation.code
    ).filter(
        valuation.pb_ratio.between(g.pbmin, g.pbmax),
        valuation.code.in_(stock_list)
    ).order_by(
        valuation.circulating_market_cap.asc()
    ).limit(
        1000
    )
    df = get_fundamentals(q).dropna()
    stock_list = list(df['code'])

    # 过滤创业板、ST、停牌、当日涨停、次新股、昨日涨幅过高
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if not (
            # (curr_data[stock].day_open == curr_data[stock].high_limit) or  # 涨停开盘
            # (curr_data[stock].day_open == curr_data[stock].low_limit) or  # 跌停开盘
            curr_data[stock].paused or  # 停牌
            curr_data[stock].is_st or  # ST
            ('ST' in curr_data[stock].name) or
            ('*' in curr_data[stock].name) or
            ('退' in curr_data[stock].name) or
            (stock.startswith('300')) or  # 创业
            (stock.startswith('688')) or  # 科创
            (stock in g.sold_stock)  # 在近一段时间内卖出过的股票
    )]
    # 过滤开盘涨停除非持仓
    stock_list =  [stock for stock in stock_list if stock in context.portfolio.positions.keys() 
        or curr_data[stock].day_open < curr_data[stock].high_limit]
    # 过滤涨幅过高
    h = history(2, '1d', 'close', stock_list)
    s_pct = h.pct_change().iloc[-1]
    stock_list = list(s_pct[s_pct < g.increase1d].index)
    # 过滤卖出时间过短
    stock_list = [stock for stock in stock_list if stock not in g.sold_stock.keys()]

    # 行业选择
    # stock_list = [stock for stock in stock_list if get_industry(stock, date=None)[stock]['sw_l1']['industry_code'] in g.indus_list]

    return stock_list


def gogogo(context):
    get_bull_bear_signal_minute()
    if g.isbull:
        log.info("当前市场判断为：牛市")
    else:
        log.info("当前市场判断为：熊市")
    if g.choose_time_signal and (not g.isbull) and (not g.bearposition) or len(g.chosen_stock_list) < 10:
        clear_position(context)
        g.nohold = True
    else:
        g.chosen_stock_list = get_stock_rank_m_m(g.chosen_stock_list)
        log.info(g.chosen_stock_list)
        my_adjust_position(context, g.chosen_stock_list)
        g.nohold = False

def get_bull_bear_signal_minute():
    nowindex = get_bars(g.MA[0], g.MA[1], '1d', 'close', include_now=True)['close']
    ma_old = nowindex.mean()
    if g.isbull:
        if nowindex[-1] * (1 + g.threshold) <= ma_old:
            g.isbull = False
    else:
        if nowindex[-1] > ma_old * (1 + g.threshold):
            g.isbull = True

# 平仓，卖出指定持仓
def close_position(security):
    order = order_target_value(security, 0) # 可能会因停牌或跌停失败
    if order != None and order.status == OrderStatus.held:    
        print("订单状态"+str(order.status))
        g.sold_stock[security] = 0


# 清空卖出所有持仓
def clear_position(context):
    if context.portfolio.positions:
        log.info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions:
            close_position(stock)

def get_stock_rank_m_m(stock_list):
    q = query(
        valuation.code, valuation.market_cap, valuation.circulating_market_cap
    ).filter(
        valuation.code.in_(stock_list)
    ).order_by(
        valuation.circulating_market_cap.asc() # 流通市值排序
    ).limit(
        100
    )
    rank_stock_list = get_fundamentals(q)

    volume5d = [attribute_history(stock, 1200, '1m', 'volume', df=False)['volume'].sum() for stock in rank_stock_list['code']] # 过去1分钟，往回追朔1200分钟，即5天
    increase60d = [get_growth_rate60(stock) for stock in rank_stock_list['code']]
    current_price = [get_close_price(stock, 1, '1m') for stock in rank_stock_list['code']]

    min_price = min(current_price)
    min_increase60d = min(increase60d)
    min_circulating_market_cap = min(rank_stock_list['circulating_market_cap'])
    min_market_cap = min(rank_stock_list['market_cap'])
    min_volume = min(volume5d)

    totalcount = [[i, math.log(min_volume / volume5d[i]) * g.weights[3] + math.log(min_price / current_price[i]) * g.weights[2] + math.log(min_circulating_market_cap / rank_stock_list['circulating_market_cap'][i]) * g.weights[1] + math.log(min_market_cap / rank_stock_list['market_cap'][i]) * g.weights[0] + math.log(min_increase60d / increase60d[i]) * g.weights[4]] for i in rank_stock_list.index]
    totalcount.sort(key=lambda x:x[1])
    return [rank_stock_list['code'][totalcount[-1-i][0]] for i in range(min(g.sellrank, len(rank_stock_list)))]

# 获取股票n日以来涨幅，根据当前价计算
# n 默认20日
def get_growth_rate(security, n=20):
    lc = get_close_price(security, n)
    c = get_close_price(security, 1, '1m')
    
    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %(security, n, lc, c))
        return 0
        
def get_growth_rate60(security):
    price60d = attribute_history(security, 60, '1d', 'close', False)['close'][0]
    pricenow = get_close_price(security, 1, '1m')
    if not isnan(pricenow) and not isnan(price60d) and price60d != 0:
        return pricenow / price60d
    else:
        return 100

# 获取前n个单位时间当时的收盘价
def get_close_price(security, n, unit='1d'):
    print(security)
    return attribute_history(security, n, unit, 'close')['close'][0]

# 调仓策略：控制在设置的仓位比例附近，如果过多或过少则调整
# 熊市时按设置的总仓位比例控制
def my_adjust_position(context, hold_stocks):
    if g.choose_time_signal and (not g.isbull):
        free_value = context.portfolio.total_value * g.bearpercent
        maxpercent = 1.3 / g.stocknum * g.bearpercent
    else:
        free_value = context.portfolio.total_value
        maxpercent = 1.3 / g.stocknum
    buycash = free_value / g.stocknum

    for stock in context.portfolio.positions.keys():
        current_data = get_current_data()
        price1d = get_close_price(stock, 1)
        nosell_1 = context.portfolio.positions[stock].price >= current_data[stock].high_limit
        sell_2 = stock not in hold_stocks
        if sell_2 and not nosell_1:
            close_position(stock)
        else:
            current_percent = context.portfolio.positions[stock].value / context.portfolio.total_value
            if current_percent > maxpercent:order_target_value(stock, buycash)

# 过滤卖出不足buyagain日的股票
def filter_buyagain(stock_list):
    return [stock for stock in stock_list if stock not in g.sold_stock.keys()]

def mybuy(context):
    if not g.nohold:
        # 避免卖出的股票马上买入
        hold_stocks = filter_buyagain(g.chosen_stock_list)
        log.info("待买股票列表：%s" %(hold_stocks))
        if g.choose_time_signal and (not g.isbull):
            free_value = context.portfolio.total_value * g.bearpercent
            minpercent = 0.7 / g.stocknum * g.bearpercent
        else:
            free_value = context.portfolio.total_value
            minpercent = 0.7 / g.stocknum
        buycash = free_value / g.stocknum
    
        for i in range(min(g.buyrank, len(hold_stocks))):
            free_cash = free_value - context.portfolio.positions_value
            # if hold_stocks[i] not in get_blacklist() and free_cash > context.portfolio.total_value / (g.stocknum * 10): # 黑名单里的股票不买
            if free_cash > context.portfolio.total_value / (g.stocknum * 10):
                if hold_stocks[i] in context.portfolio.positions.keys():
                    log.info("已经持有股票：[%s]" %(hold_stocks[i]))
                    current_percent = context.portfolio.positions[hold_stocks[i]].value / context.portfolio.total_value
                    if  current_percent >= minpercent:continue
                    tobuy = min(free_cash, buycash - context.portfolio.positions[hold_stocks[i]].value)
                else:
                    tobuy = min(buycash, free_cash)
                order_value(hold_stocks[i], tobuy)
    g.inv_value_max = max(g.inv_value_max, context.portfolio.total_value)



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
            # if earn>.35:
            #     close_position(security)
            if e_5<-0.1:
                close_position(security)
            if earn<-0.10 :
                close_position(security)
    # if context.portfolio.total_value < 0.9 * g.inv_value_max:
    #     log.info("回撤超过10%平仓")
    #     clear_position(context)



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

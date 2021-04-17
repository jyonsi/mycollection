# 克隆自聚宽文章：https://www.joinquant.com/post/30481
# 标题：7年40倍，模拟超过两年，年化高回撤低
# 作者：quakecat

# 克隆自聚宽文章：https://www.joinquant.com/post/30481
# 标题：7年40倍，模拟超过两年，年化高回撤低
# 作者：quakecat

from jqdata import *
# from kuanke.user_space_api import *
import numpy as np
import pandas as pd


def initialize(context):
    set_option('use_real_price', True)
    set_option('avoid_future_data', True)

    g.is_bull = False  # 是否牛市
    g.chosen_stock_list = []  # 存储选出来的股票
    g.not_hold = True  # 空仓专用信号
    g.sold_stock = {}  # 近期卖出的股票及卖出天数
    # 交易设置
    g.stock_nums = 4  # 理想持股数量
    g.bear_pct = 0.3  # 熊市仓位
    g.bear_pos = True  # 熊市是否持仓
    g.sell_rank = 10  # 排名多少位之后(不含)卖出
    g.buy_rank = 9  # 排名多少位之前(含)可以买入

    # 初始筛选
    g.trade_days = 300  # 上市天数
    g.inc_1d = 0.087  # 前一日涨幅小于8.7%
    g.pb_min = 0.01  # 最小市净率
    g.pb_max = 30  # 最大市净率

    # 排名条件及权重，正数代表从小到大，负数表示从大到小
    # 各因子权重：总市值，流通市值，最新价格，5日平均成交量，60日涨幅
    g.weights = [5, 5, 8, 4, 10]

    # 配置择时
    g.MA = ['000001.XSHG', 10]  # 均线择时
    g.choose_time_signal = True  # 启用择时信号
    g.threshold = 0.003  # 牛熊切换阈值
    g.buy_again = 5  # 再次买入的间隔时间
    #
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    run_daily(my_trade, '14:50')
    run_daily(my_buy, '14:55')


def before_market_open(context):
    tmp_sold_stock = {}
    for stock in g.sold_stock:
        if g.sold_stock[stock] + 1 < g.buy_again:
            tmp_sold_stock[stock] = g.sold_stock[stock] + 1
    #
    g.sold_stock = tmp_sold_stock
    # 初步选股
    get_stock_list(context)


def get_stock_list(context):
    # 过滤掉新股
    check_date = context.previous_date - datetime.timedelta(days=g.trade_days)
    stock_list = list(get_all_securities(date=check_date).index)

    # pb符合要求的股票，按照流通市值升序排列，取前1000名
    q = query(
        valuation.code
    ).filter(
        valuation.code.in_(stock_list),
        valuation.pb_ratio.between(g.pb_min, g.pb_max)
    ).order_by(
        valuation.circulating_market_cap.asc()
    ).limit(
        1000
    )
    df = get_fundamentals(q).dropna()
    stock_list = list(df['code'])

    # 过滤创业板、ST、停牌、当日涨停、次新股、昨日涨幅过高
    #
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if not (
            (curr_data[stock].day_open == curr_data[stock].high_limit) or  # 涨停开盘
            (curr_data[stock].day_open == curr_data[stock].low_limit) or  # 跌停开盘
            curr_data[stock].paused or  # 停牌
            curr_data[stock].is_st or  # ST
            ('ST' in curr_data[stock].name) or
            ('*' in curr_data[stock].name) or
            ('退' in curr_data[stock].name) or
            (stock.startswith('300')) or  # 创业
            # (stock.startswith('688')) or  # 科创
            (stock in g.sold_stock)  # 在近一段时间内卖出过的股票
    )]

    # 过滤昨日涨幅过高的股票(涨幅不超过 g.inc_1d = 0.087, 即8.7%)
    h = history(2, '1d', 'close', stock_list)
    s_pct = h.pct_change().iloc[-1]
    stock_list = list(s_pct[s_pct < g.inc_1d].index)

    # 记录初步选股结果
    g.chosen_stock_list = stock_list


def my_trade(context):
    # type: (Context) -> None
    get_bull_bear_signal_minute()
    if g.is_bull:
        log.info("当前市场判断为：牛市")
    else:
        log.info("当前市场判断为：熊市")
    if (g.choose_time_signal and (not g.is_bull) and (not g.bear_pos)) or len(g.chosen_stock_list) < 10:
        clear_position(context)
        g.not_hold = True
    else:
        get_stock_rank_m_m()
        my_adjust_position(context)
        g.not_hold = False


def get_bull_bear_signal_minute():
    close_data = get_bars(g.MA[0], g.MA[1], '1d', 'close', include_now=True)['close']
    ma_old = close_data.mean()
    if g.is_bull:
        if close_data[-1] * (1 + g.threshold) <= ma_old:
            g.is_bull = False
    else:
        if close_data[-1] > ma_old * (1 + g.threshold):
            g.is_bull = True


def get_stock_rank_m_m():
    # 以开盘前初选结果为基础，继续精选
    stock_list = g.chosen_stock_list

    # valuation.circulating_market_cap, valuation.market_cap
    q = query(
        valuation.code, valuation.circulating_market_cap, valuation.market_cap
    ).filter(
        valuation.code.in_(stock_list)
    ).order_by(
        valuation.circulating_market_cap.asc()  # 流通市值排序
    ).limit(
        100  # 流通市值最小的100只
    )
    df = get_fundamentals(q).set_index('code')
    stock_list = list(df.index)

    # 过去“5日”成交量之和
    s_volume5d = history(1200, '1m', 'volume', stock_list).sum()  # 过去1分钟，往回追朔1200分钟，即5天

    # 过去“60日”的股价增长率 [-1]/[0]，过去1分钟的收盘价
    h = get_bars(stock_list, 61, '1d', ['close', ], include_now=True)
    d_inc60d = {}
    d_current = {}
    for stock in stock_list:
        close_data = h[stock]['close']
        d_inc60d[stock] = close_data[-1] / close_data[0]
        d_current[stock] = close_data[-1]
    #
    s_inc60d = pd.Series(d_inc60d)
    s_current = pd.Series(d_current)

    # 4. increase60d
    increase60d = np.log(s_inc60d.min()) - np.log(s_inc60d)

    # 3. volume5d
    volume5d = np.log(s_volume5d.min()) - np.log(s_volume5d)

    # 2. current_price
    current_price = np.log(s_current.min()) - np.log(s_current)

    # 1. circulating_market_cap
    circulating_market_cap = np.log(df['circulating_market_cap'].min()) - np.log(df['circulating_market_cap'])

    # 0. market_cap
    market_cap = np.log(df['market_cap'].min()) - np.log(df['market_cap'])

    # 打分
    s_total = increase60d * g.weights[4] + volume5d * g.weights[3] + current_price * g.weights[
        2] + circulating_market_cap * g.weights[1] + market_cap * g.weights[0]  # type: pd.Series

    # 选股结果：排序后,取前g.sell_rank名
    g.chosen_stock_list = list(s_total.sort_values(ascending=False).index)[:g.sell_rank]
    log.info(g.chosen_stock_list)


# 清空卖出所有持仓
def clear_position(context):
    if context.portfolio.positions:
        log.info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions:
            close_position(stock)


# 平仓，卖出指定持仓
def close_position(security):
    _order = order_target_value(security, 0)  # 可能会因停牌或跌停失败
    if _order is not None and _order.filled > 0:
        g.sold_stock[security] = 0


# 调仓策略：控制在设置的仓位比例附近，如果过多或过少则调整
# 熊市时按设置的总仓位比例控制
def my_adjust_position(context):
    if g.choose_time_signal and (not g.is_bull):
        free_value = context.portfolio.total_value * g.bear_pct
        max_percent = 1.3 / g.stock_nums * g.bear_pct
    else:
        free_value = context.portfolio.total_value
        max_percent = 1.3 / g.stock_nums
    buy_cash = free_value / g.stock_nums

    current_data = get_current_data()
    for stock in context.portfolio.positions:
        sell_1 = current_data[stock].last_price < current_data[stock].high_limit  # 未涨停
        sell_2 = stock not in g.chosen_stock_list  # 不在股票池内
        if sell_2 and sell_1:
            close_position(stock)
        else:
            current_percent = context.portfolio.positions[stock].value / context.portfolio.total_value
            if current_percent > max_percent:
                order_target_value(stock, buy_cash)


def my_buy(context):
    if g.not_hold:
        return 
        
    # 避免卖出的股票马上买入
    hold_stocks = [stock for stock in g.chosen_stock_list if stock not in g.sold_stock]
    log.info("待买股票列表：%s" % hold_stocks)
    #
    if g.choose_time_signal and (not g.is_bull):
        free_value = context.portfolio.total_value * g.bear_pct
        min_percent = 0.7 / g.stock_nums * g.bear_pct
    else:
        free_value = context.portfolio.total_value
        min_percent = 0.7 / g.stock_nums
    #
    buy_cash = free_value / g.stock_nums

    for stock in hold_stocks:
        if len(context.portfolio.positions) >= g.buy_rank:
            break
        #
        free_cash = free_value - context.portfolio.positions_value
        if free_cash > context.portfolio.total_value / (g.stock_nums * 10):
            if stock in context.portfolio.positions:
                log.info("已经持有股票：[%s]" % stock)
                current_percent = context.portfolio.positions[stock].value / context.portfolio.total_value
                if current_percent >= min_percent:
                    continue
                #
                to_buy = min(free_cash, buy_cash - context.portfolio.positions[stock].value)
                if to_buy < 0:
                    continue
            else:
                to_buy = min(buy_cash, free_cash)
            #
            order_value(stock, to_buy)

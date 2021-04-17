# 克隆自聚宽文章：https://www.joinquant.com/post/25183
# 标题：价值低波（下）--十年十倍（2020拜年）
# 作者：Gyro

import pandas as pd
# 系统设置&系统变量，不因代码更替而改变
def initialize(context):
    log.set_level('order', 'error')
    set_option('use_real_price', True)
    g.weight = pd.Series() #投资组合
    g.inv_value_max = context.portfolio.total_value #市值峰值

# 系统参数&系统变量，因代码更新而重新赋值
def after_code_changed(context):
    g.index = '000300.XSHG' #投资指数
    g.treasury = '000012.XSHG'
    g.stocks_num = 10 #最大持股数

# 开盘运行，准备数据
def before_trading_start(context):
    # 高价值
    stocks = high_value(context, g.index, g.stocks_num)
    # 波动率加权
    g.weight = volatility_weight(stocks, 241)
    # 风险控制
    risk_controller(context)
    # 输出信息
    cur_data = get_current_data()
    for stock in g.weight.index:
        log.info(stock, cur_data[stock].name, 100*g.weight[stock])

# 收盘后运行，记录数据
def after_trading_end(context):
    g.inv_value_max = max(g.inv_value_max, context.portfolio.total_value)
    log.info('...Trading end')

# 逐日运行
def handle_data(context, data):
    cur_data = get_current_data()
    # sell
    for stock in context.portfolio.positions:
        if stock not in g.weight.index and\
            not cur_data[stock].paused:
            log.info('sell out', stock, cur_data[stock].name)
            order_target(stock, 0);
    # buy & rebalance
    for stock in g.weight.index:
        position = g.weight[stock] * context.portfolio.total_value
        if stock not in context.portfolio.positions:
            delta = position
        else:
            delta = position - context.portfolio.positions[stock].value
        if context.portfolio.available_cash > delta and\
            not cur_data[stock].paused:
            log.info('rebalance', stock, cur_data[stock].name, int(position))
            order_value(stock, delta)

# 功能函数，选取品质价值股
def high_value(context, index, stocks_num):
    # 股票池
    stocks = get_index_stocks(index)
    # 取基本面数据，选择品质价值股
    df = get_fundamentals(query(
            valuation.code,
            valuation.pb_ratio,
            valuation.pe_ratio,
        ).filter(
            valuation.code.in_(stocks),
            valuation.pb_ratio > 0,
            valuation.pe_ratio > 0,
            valuation.pe_ratio < 20, # PE < 20
            valuation.pb_ratio / valuation.pe_ratio > 0.1, # ROE >10%
        ).order_by(valuation.pe_ratio.asc()
        ).limit(stocks_num)
        ).dropna()
    return list(df.code)

# 功能函数，波动率加权
def volatility_weight(stocks, days):
    # 历史数据
    h = history(days, '1d', 'close', stocks, df=True)
    r = h.pct_change()[1:]
    # 波动率加权
    w = 1.0 / r.std()
    weight = w / w.sum()
    return weight

# 功能函数，风险控制
def risk_controller(context):
    # 风险控制-- 趋势
    inv_stock = True
    h = history(61, '1d', 'close', [g.index], df=True)
    p = h[g.index]
    if p[-1] < p.mean():
        inv_stock = False #指数趋势空头，停止买进
    # 风险控制-- 回撤
    inv_stock_ratio = 1.0
    if context.portfolio.total_value < 0.9 * g.inv_value_max:
        inv_stock_ratio = 0.5 #资金曲线回撤大于10%，保守投资
    # 调整投资组合
    weight = pd.Series()
    for stock in g.weight.index:
        if inv_stock or stock in context.portfolio.positions:
            weight[stock] = inv_stock_ratio * g.weight[stock]
    weight[g.treasury] = 1.0 - weight.sum()
    # 保存结果
    g.weight = weight.sort_values(ascending=False)
 # end
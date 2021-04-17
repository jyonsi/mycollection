# 克隆自聚宽文章：https://www.joinquant.com/post/2054
# 标题：talib 的K线模式识别使用 用于识别晨星、乌鸦、三兵、锤线、碑线等等形态
# 作者：软猫克鲁

enable_profile()
import numpy as np
import pandas as pd
import talib as tl
from operator import methodcaller

def initialize(context):
    # 定义一个全局变量, 保存要操作的股票
    g.security = '510300.XSHG'
    # 初始化此策略
    # 设置我们要操作的股票池, 这里我们只操作一支股票
    set_universe([g.security])
    
    g.buyOrSell = None
    
    g.CDLFuncList = [
        'CDL2CROWS'
        , 'CDL3BLACKCROWS'
        , 'CDL3INSIDE'
        , 'CDL3LINESTRIKE'
        , 'CDL3OUTSIDE'
        , 'CDL3STARSINSOUTH'
        , 'CDL3WHITESOLDIERS'
        , 'CDLABANDONEDBABY'
        , 'CDLADVANCEBLOCK'
        , 'CDLBELTHOLD'
        , 'CDLBREAKAWAY'
        , 'CDLCLOSINGMARUBOZU'
        , 'CDLCONCEALBABYSWALL'
        , 'CDLCOUNTERATTACK'
        , 'CDLDARKCLOUDCOVER'
        , 'CDLDOJI'
        , 'CDLDOJISTAR'
        , 'CDLDRAGONFLYDOJI'
        , 'CDLENGULFING'
        , 'CDLEVENINGDOJISTAR'
        , 'CDLEVENINGSTAR'
        , 'CDLGAPSIDESIDEWHITE'
        , 'CDLGRAVESTONEDOJI'
        , 'CDLHAMMER'
        , 'CDLHANGINGMAN'
        , 'CDLHARAMI'
        , 'CDLHARAMICROSS'
        , 'CDLHIGHWAVE'
        , 'CDLHIKKAKE'
        , 'CDLHIKKAKEMOD'
        , 'CDLHOMINGPIGEON'
        , 'CDLIDENTICAL3CROWS'
        , 'CDLINNECK'
        , 'CDLINVERTEDHAMMER'
        , 'CDLKICKING'
        , 'CDLKICKINGBYLENGTH'
        , 'CDLLADDERBOTTOM'
        , 'CDLLONGLEGGEDDOJI'
        , 'CDLLONGLINE'
        , 'CDLMARUBOZU'
        , 'CDLMATCHINGLOW'
        , 'CDLMATHOLD'
        , 'CDLMORNINGDOJISTAR'
        , 'CDLMORNINGSTAR'
        , 'CDLONNECK'
        , 'CDLPIERCING'
        , 'CDLRICKSHAWMAN'
        , 'CDLRISEFALL3METHODS'
        , 'CDLSEPARATINGLINES'
        , 'CDLSHOOTINGSTAR'
        , 'CDLSHORTLINE'
        , 'CDLSPINNINGTOP'
        , 'CDLSTALLEDPATTERN'
        , 'CDLSTICKSANDWICH'
        , 'CDLTAKURI'
        , 'CDLTASUKIGAP'
        , 'CDLTHRUSTING'
        , 'CDLTRISTAR'
        , 'CDLUNIQUE3RIVER'
        , 'CDLUPSIDEGAP2CROWS'
        , 'CDLXSIDEGAP3METHODS'
        ]

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    security = g.security
    
    hData = attribute_history(security, 40, unit='1d'
                    , fields=('close', 'volume', 'open', 'high', 'low')
                    , skip_paused=False
                    , df=False)
    
    volume = hData['volume']
    close = hData['close']
    open = hData['open']
    high = hData['high']
    low = hData['low']
    
    g.buyOrSell = None
    
    g.CDLFuncList = [
         'CDLMORNINGDOJISTAR'
        ]
    for funcName in g.CDLFuncList :
        func = methodcaller(funcName, open, high, low, close)
        ret = func(tl)
        #log.debug([funcName, ret])
        if ret[-1] > 0 :
            g.buyOrSell = 'B'
        if ret[-1] < 0 :
            g.buyOrSell = 'S'
        if g.buyOrSell is not None :
            log.debug([funcName, g.buyOrSell])
            #break
    
    holdPosition = context.portfolio.positions[security].amount 
    # 取得过去五天的平均价格
    average_price = data[security].mavg(20)   
    # 取得上一时间点价格
    current_price = data[security].price
    # 取得当前的现金
    cash = context.portfolio.cash

    average_price = 0
    # 如果上一时间点价格高出五天平均价1%, 则全仓买入
    if holdPosition == 0 and g.buyOrSell == 'B' :
        # 计算可以买多少只股票
        number_of_shares = int(cash/current_price)
        # 购买量大于0时，下单
        if number_of_shares > 0:
            # 买入股票
            order(security, +number_of_shares)
            # 记录这次买入
            log.info("Buying %s" % (security))
    # 如果上一时间点价格低于五天平均价, 则空仓卖出
    elif (g.buyOrSell == 'S' or current_price < average_price) and holdPosition > 0:
        # 卖出所有股票,使这只股票的最终持有量为0
        order_target(security, 0)
        # 记录这次卖出
        log.info("Selling %s" % (security))

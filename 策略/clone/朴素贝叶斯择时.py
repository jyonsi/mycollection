# 克隆自聚宽文章：https://www.joinquant.com/post/29706
# 标题：高斯朴素贝叶斯择时2
# 作者：龙174

# 导入函数库
import jqlib.technical_analysis as tech
import datetime as dt
import pandas as pd
import statsmodels.api as sm
from jqdata import *
from sklearn.naive_bayes import GaussianNB

#ATR指标
def get_atr(codes,dates,timeperiod=14,unit='1d'):
    import jqlib.technical_analysis as tech
    res=[]
    for date in dates:
        MTR,ATR=tech.ATR(codes, date , timeperiod=timeperiod, unit = unit,include_now = True)
        tmp=pd.Series(ATR,name=date)
        res.append(tmp)
    atr=pd.concat(res,axis=1).T
    return atr

#macd指标
def get_macd(codes,dates, SHORT = 12, LONG = 26, MID = 9, unit='1d' ):
    res=[]
    for date in dates:
        dif, dea,macd=tech.MACD(codes, date, SHORT = SHORT, LONG = LONG, MID = MID, unit = unit, include_now = True)
        tmp=pd.Series(macd,name=date)
        res.append(tmp)
    macd=pd.concat(res,axis=1).T
    return macd

#移动均线指标
def get_emaDiff(codes,dates, fast = 12,slow=50,unit='1d'):
    res=[]
    for date in dates:
        EXPMA_fast = pd.Series(tech.EXPMA(codes,check_date=date, timeperiod=fast, unit = unit, include_now = True),name=date)
        EXPMA_slow = pd.Series(tech.EXPMA(codes,check_date=date, timeperiod=slow, unit = unit, include_now = True),name=date)
        diff=EXPMA_fast-EXPMA_slow
        res.append(diff)
    emaDiff=pd.concat(res,axis=1).T
    return emaDiff

#获取gain因子
def get_gain(codes,period,start,end,unit='1d'):
    res=[]
    for code in codes:
        tmp=get_price(code, start_date=start, end_date=end, frequency='daily', fields=None, skip_paused=False, fq='pre')
        close=tmp.close.rename(code)
        volume=tmp.volume.rename(code)
        perGain=pd.Series(index=close.index,name=code)
        
        for i in range(period,len(close)):
            perClose=close.iloc[i-period:i].copy()
            perClose=(close.iloc[i]-perClose)/close.iloc[i]
            
            perVolume=volume.iloc[i-period:i].copy()
            perVolume=perVolume/sum(perVolume)
            weight=pd.Series(1,index=perVolume.index)
            for j in range(len(perVolume)-1):
                tmp=1-perVolume.iloc[j+1:len(perVolume)].copy()
                weight.iloc[j]=tmp.prod()
            weight=weight*perVolume
            weight=weight/sum(weight)
            
            perGain.iloc[i]=sum(perClose*weight)
              
        res.append(perGain)
        
    gain=pd.concat(res,axis=1)
    return gain

#获取RSRS因子
def get_rsrs(codes,period,window,start,end,unit='1d'):
    res=[]
    for code in codes:
        tmp=get_price(code, start_date=start, end_date=end, frequency='daily', fields=None, skip_paused=False, fq='pre')
        beta=pd.Series(index=tmp.index,name=code)
        rsquare=pd.Series(index=tmp.index,name=code)
        high=tmp.high.copy()
        low=tmp.low.copy()
        
        for i in range(period,len(high)):
            tmpHigh=high.iloc[i-period+1:i+1].copy()
            tmpLow=low.iloc[i-period+1:i+1].copy()
            if (sum(pd.isnull(tmpHigh))+sum(pd.isnull(tmpLow)))>0:
                continue
            x=sm.add_constant(tmpLow)
            model=sm.OLS(tmpHigh,x)
            results=model.fit()
            beta.iloc[i]=results.params.low
            rsquare.iloc[i]=results.rsquared
            
        mean=beta.rolling(window=window).mean()
        std=beta.rolling(window=window).std()
        beta_std=(beta-mean)/std
        right=beta_std*beta*rsquare
        
        res.append(right)
    rsrs=pd.concat(res,axis=1)
    return rsrs

#获取交易日
def get_tradeDates(start,end,unit='1d'):
    tmp=get_price('000300.XSHG', start_date=start, end_date=end, frequency='daily', fields=None, skip_paused=False, fq='pre')['close']
    return list(pd.to_datetime(tmp.index))
# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
      # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
 
## 开盘前运行函数
def before_market_open(context):
    days=300#获取数据的天数
    end=context.current_dt
    start= end - dt.timedelta(days=days)
    unit='1d'
    codes=['000300.XSHG']
    
    ohlc=get_price(codes, start_date=start, end_date=end, frequency='daily', fields=None, skip_paused=False, fq='pre')
    ret=ohlc['close'].pct_change()
    volume=ohlc['volume'].pct_change()
    money=ohlc['money'].pct_change()
    atr=get_atr(codes=codes, dates=ret.index,unit=unit)
    macd=get_macd(codes=codes, dates=ret.index,unit=unit)
    emaDiff=get_emaDiff(codes=codes, dates=ret.index,unit=unit)
    gain=get_gain(codes,60,start,end,unit=unit)
    rsrs=get_rsrs(codes,18,60,start,end,unit=unit)
    
    for x in ['days','end','start','unit']:
        exec('del ' + x)
    
    
    tech_indexs=['ret','atr','macd','emaDiff','gain','rsrs','volume','money']
    
    index_data={}
    res=[]
    for name in tech_indexs:
        temp=eval(name)
        temp.index=pd.to_datetime(temp.index)
        index_data[name]=temp
        exec('del ' + name)
        
        temp=temp.stack().reset_index()
        temp.columns=['date','code',name]
        res.append(temp)
    del temp
        
        
    allData=res[0]
    for i in range(1,len(res)):
        allData=pd.merge(allData,res[i],on=['date','code'])
    allData.set_index('date',inplace=True)
    del res
    
    code_data={}
    for code in codes:
        code_data[code]=allData[allData.code==code].copy()
    del allData
    
    #训练模型
    train_window=60
    res=[]
    for code in codes:
        data=code_data[code].copy()
        n=len(data)
        x_train=data.iloc[n-train_window-1:n-1,1:]
        y_train=data.ret.iloc[n-train_window:n]>0
        
        clf=GaussianNB()
        clf.fit(x_train,y_train)
    x_test=np.array(data.iloc[n-1,1:]).reshape(1,-1)
    g.signal=clf.predict(x_test)[0]
    
    g.security = '510300.XSHG'

## 开盘时运行函数
def market_open(context):
    security = g.security
    
    cash = context.portfolio.available_cash
    
    if g.signal==True and cash>0:
        order_value(security, cash)
        log.info("Buying %s" % (security))
    elif g.signal==False and context.portfolio.positions[security].closeable_amount > 0:
        order_target(security, 0)
        # 记录这次卖出
        log.info("Selling %s" % (security))

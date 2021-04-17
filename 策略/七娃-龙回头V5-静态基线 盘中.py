# 克隆自聚宽文章：https://www.joinquant.com/post/32132
# 标题：复制+改进，龙回头策略，三年20倍
# 作者：韶华聚鑫

###12.9 EragonV5升版主题，结合JQ社区龙回头V5的思路，以龙虎榜和市值快速选股，然后结合Eragon量和踩线的过滤，隔日晚计算，次日买卖###
# 导入函数库
from jqdata import *
from jqlib.technical_analysis import *
import operator
import datetime
import numpy as np
import pandas as pd
import datetime as dt
from six import BytesIO

# 初始化函数，设定基准等等
def initialize(context):
    # 设定中证1000作为基准
    g.index = '000852.XSHG'
    set_benchmark(g.index)
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    set_option("avoid_future_data", True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    g.stocknum =2   #经凯利公式计算（128-37）/165=0.55
    g.roundratio =0.946 #和基准值核对时取个概数，非绝对，经验值

    
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    #run_daily(before_market_open, time='before_open')
      # 开盘时运行
      ##改用handledata
    run_daily(market_open, time='open')
    run_daily(market_run, time ='13:58')
    run_daily(market_close, time ='14:55')
      # 收盘后运行
    run_daily(after_market_close, time='20:00')
      # 收盘后运行--E5之眼
    run_daily(after_close_eye, time='21:00')
      # 收盘后运行--E5之脑
    run_daily(after_close_brain, time='22:00')

## 开盘前运行函数
def before_market_open(context):
    # 输出运行时间
    log.info('函数运行时间(before_market_open)：'+str(context.current_dt.time()))

## 开盘时运行函数
def market_open(context):
    log.info('函数运行时间(market_open):'+str(context.current_dt.time()))
    current_data = get_current_data()
    today_date = context.current_dt.date()
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    g.df_buy=None
    g.df_sell=None
    #1,读取brain文件中当天的买卖信号
    df_signal = pd.read_csv(BytesIO(read_file('Crouch_brain.csv')))
    df_signal['date'] = pd.to_datetime(df_signal['date']).dt.date
    df_signal= df_signal[(df_signal['date'] == lastd_date)]
    g.df_buy= df_signal[df_signal['flag'].str.contains('buy')]
    g.df_buy= g.df_buy.sort_values(['data'], ascending = False) #按'data'-人气降序排序
    g.df_sell= df_signal[df_signal['flag'].str.contains('sell')]
    
    ##先是卖的操作，止盈，卖信
    for stockcode in context.portfolio.positions:
        if current_data[stockcode].paused == True:
            continue
        cost = context.portfolio.positions[stockcode].avg_cost
        price = current_data[stockcode].last_price
        value = context.portfolio.positions[stockcode].value
        intime= context.portfolio.positions[stockcode].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        rise_ratio = ret/duration
        
        #创板股提高盈速要求
        """
        if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
            rise_ratio = rise_ratio/2
            ret = ret/2
        """
        if ret > 2:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,开盘止盈,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)),append = True)
                continue
            
        if stockcode in g.df_sell['code'].values:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,开盘清仓,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)),append = True)
                continue
            
    ##接着买的操作
    if context.portfolio.available_cash <10000: #钱不够了
        return
    
    #分配资金，全仓只持两只
    if context.portfolio.available_cash == context.portfolio.total_value:
        cash_perstk = context.portfolio.available_cash/2
    else:
        cash_perstk = context.portfolio.available_cash
    
    for i in range(len(g.df_buy)):
        stockcode = g.df_buy['code'].values[i]
        lastd_price = g.df_buy['price'].values[i]
        flag = g.df_buy['flag'].values[i]
        if current_data[stockcode].paused == True:
            continue
        if context.portfolio.available_cash <10000: #钱不够了
            continue

        if (current_data[stockcode].last_price >0.982*lastd_price) and (current_data[stockcode].last_price <1.05*lastd_price):
            if order_target_value(stockcode,cash_perstk) != None:
                write_file('Crouch_log.csv', str('%s,开盘买入,%s,%s\n' % (context.current_dt.time(),stockcode,current_data[stockcode].last_price)),append = True)
                
## 赌场运行函数
def market_run(context):
    log.info('函数运行时间(market_run):'+str(context.current_dt.time()))
    #设置全局参数
    today_date = context.current_dt.date()
    today_time = context.current_dt.time()
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    current_data = get_current_data()
    
    #1-盘中先遍历持仓进行卖出判断
    for stockcode in context.portfolio.positions:
        if current_data[stockcode].paused == True:
            continue
        cost = context.portfolio.positions[stockcode].avg_cost
        price = context.portfolio.positions[stockcode].price
        value = context.portfolio.positions[stockcode].value
        intime= context.portfolio.positions[stockcode].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        rise_ratio = ret/duration
        #创板股提高盈速要求
        """
        if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
            rise_ratio = rise_ratio/2
            ret = ret/2
            up_ratio =1.05
        else:
            up_ratio =1.02
        """
        df_ticks = get_ticks(stockcode, end_dt=context.current_dt, count=1 ,fields=['time','current', 'volume','money'],df=True)
        df_price = get_price(stockcode,count=2,end_date=lastd_date, frequency='daily', fields=['low_limit','high_limit','open','close','volume']) #先老后新
        if (current_data[stockcode].last_price < 1.02*current_data[stockcode].low_limit):
            #跌停中若当日量小于前两日，略过
            if (df_ticks['volume'].values[0] > df_price['volume'].values[0]) or (df_ticks['volume'].values[0] > df_price['volume'].values[1]):
                if order_target(stockcode,0) != None:
                    write_file('Crouch_log.csv', str('%s,盘中近停,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)),append = True)
                    continue
        if ret < -0.1:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,盘中止损,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue
        elif ret > 2:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,盘中止盈,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue
            
## 收盘时运行函数
def market_close(context):
    log.info('函数运行时间(market_close):'+str(context.current_dt.time()))
    #设置全局参数
    today_date = context.current_dt.date()
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    current_data = get_current_data()
    #1-盘中先遍历持仓进行卖出判断
    for stockcode in context.portfolio.positions:
        if current_data[stockcode].paused == True:
            continue
        cost = context.portfolio.positions[stockcode].avg_cost
        price = context.portfolio.positions[stockcode].price
        value = context.portfolio.positions[stockcode].value
        intime= context.portfolio.positions[stockcode].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        rise_ratio = ret/duration
        
        #创板股提高盈速要求
        """
        if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
            rise_ratio = rise_ratio/2
            ret = ret/2
            up_ratio =1.05
        else:
            up_ratio =1.02
        """    
        df_ticks = get_ticks(stockcode, end_dt=context.current_dt, count=1 ,fields=['time','current', 'volume','money'],df=True)
        df_price = get_price(stockcode,count=2,end_date=lastd_date, frequency='daily', fields=['low_limit','high_limit','open','close','volume']) #先老后新
        if (current_data[stockcode].last_price < 1.02*current_data[stockcode].low_limit):
            #跌停中若当日量小于前两日，略过
            if (df_ticks['volume'].values[0] > df_price['volume'].values[0]) or (df_ticks['volume'].values[0] > df_price['volume'].values[1]):
                if order_target(stockcode,0) != None:
                    write_file('Crouch_log.csv', str('%s,尾盘近停,%s,周期:%s,盈利:%s\n' % (context.current_dt.time(),stockcode,duration,ret)),append = True)
                    continue
        if ret < -0.1:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,尾盘止损,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue
        elif ret > 2:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,尾盘止盈,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue
        
        if duration >=3 and ret <(-0.05+0.015*(duration-3))/g.roundratio:
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,尾盘短清,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue
            
        if duration >=6 and g.roundratio*rise_ratio <(0.004*(2**(duration//4-1))):
            if order_target(stockcode,0) != None:
                write_file('Crouch_log.csv', str('%s,尾盘失速,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue        
    ##接着买的操作
    if context.portfolio.available_cash <10000: #钱不够了
        return
    #分配资金，全仓只持两只
    if context.portfolio.available_cash == context.portfolio.total_value:
        cash_perstk = context.portfolio.available_cash/2
    else:
        cash_perstk = context.portfolio.available_cash
    
    for i in range(len(g.df_buy)):
        stockcode = g.df_buy['code'].values[i]
        lastd_price = g.df_buy['price'].values[i]
        if stockcode in context.portfolio.positions or current_data[stockcode].paused == True:
            continue
        if context.portfolio.available_cash <10000: #钱不够了
            continue
        #每日的开盘价最优区间，不匹配就不补
        if (current_data[stockcode].day_open <0.982*lastd_price) or (current_data[stockcode].day_open >1.05*lastd_price):
            continue
        if current_data[stockcode].paused != True:
            if order_target_value(stockcode,cash_perstk) != None:
                write_file('Crouch_log.csv', str('%s,尾盘买入,%s,%s\n' % (context.current_dt.time(),stockcode,current_data[stockcode].last_price)),append = True)
                
## 收盘后运行函数
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    #总结当日持仓情况
    today_date = context.current_dt.date()
    
    for stk in context.portfolio.positions:
        cost = context.portfolio.positions[stk].avg_cost
        price = context.portfolio.positions[stk].price
        value = context.portfolio.positions[stk].value
        intime= context.portfolio.positions[stk].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        
        print('股票(%s)共有:%s,入时:%s,周期:%s,成本:%s,现价:%s,收益:%s' % (stk,value,intime,duration,cost,price,ret))
        write_file('Crouch_log.csv', str('股票:%s,共有:%s,入时:%s,周期:%s,成本:%s,现价:%s,收益:%s\n' % (stk,value,intime,duration,cost,price,ret)),append = True)
        
    print('总资产:%s,持仓:%s' %(context.portfolio.total_value,context.portfolio.positions_value))
    write_file('Crouch_log.csv', str('%s,总资产:%s,持仓:%s\n' %(context.current_dt.date(),context.portfolio.total_value,context.portfolio.positions_value)),append = True)

## 收盘后运行函数
def after_close_eye(context):
    log.info(str('函数运行时间(after_close_eye):'+str(context.current_dt.time())))
    today_date = context.current_dt.date()
    back3_date = get_trade_days(end_date=today_date,count=7)[0]
    all_data = get_current_data()
    ##抓股1，取A下和当天龙虎榜的交集，并去四
    stocklist = list(get_all_securities(['stock'],date=today_date).index)
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].paused]
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].is_st]
    stocklist = [stockcode for stockcode in stocklist if'退' not in all_data[stockcode].name]
    stocklist = [stockcode for stockcode in stocklist if (today_date-get_security_info(stockcode).start_date).days>100]
    num1 = len(stocklist)
    
    q=query(valuation.code).filter(valuation.code.in_(stocklist), valuation.market_cap<500).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q)
    stocklist = list(df['code'])
    
    #billboard_list = get_billboard_list(stock_list=stocklist, end_date = today_date, count =30)
    billboard_list = get_billboard_list(stock_list=stocklist, end_date = today_date, count=1) #为了正推
    stocklist = list(set(stocklist).intersection(set(billboard_list["code"])))
    num3 = len(billboard_list)
    
    ##读取eye文件中3天内的信号，重复判断的参考
    df_existing = pd.read_csv(BytesIO(read_file('Crouch_eye.csv')))
    df_existing['date'] = pd.to_datetime(df_existing['date']).dt.date
    df_existing= df_existing[(df_existing['date'] < today_date) & (df_existing['date'] >= back3_date)]

    for stockcode in stocklist:
    ##抓股2，，去除百日涨幅3倍以上的
        riselevel = is_falls(context,stockcode)
        if riselevel == 0:
            continue
    #去除一月内复牌的
        df_suspened = get_price(stockcode, count=20, end_date=today_date, frequency='daily', fields='paused')['paused']
        suspened_sum = sum(df_suspened)
        if suspened_sum >0:
            continue
    ##三天内的信号要去重
        if stockcode in df_existing['code'].values:  #不重复判断，不重复买卖
            continue
    ##将信号记录在eye中，Sign_date,code,riselevel
        write_file('Crouch_eye.csv', str('%s,%s,%s\n' % (today_date,stockcode,riselevel)),append = True)
        
## 收盘后运行函数,基于eye信号和持仓进行买入卖出判断
def after_close_brain(context):
    log.info(str('函数运行时间(after_close_brain):'+str(context.current_dt.time())))
    today_date = context.current_dt.date()
    back5_date = get_trade_days(end_date=today_date,count=5)[0]
    back30_date = get_trade_days(end_date=today_date,count=30)[0]
    all_data = get_current_data()
    ##选股1，读取eye文件30天内的信号
    df_waiting = pd.read_csv(BytesIO(read_file('Crouch_eye.csv')))
    df_waiting['date'] = pd.to_datetime(df_waiting['date']).dt.date
    df_waiting= df_waiting[(df_waiting['date'] < today_date) & (df_waiting['date'] >= back30_date)]
    
    ##读取brain文件中7天内的买信，做重复判断的参考
    df_existing = pd.read_csv(BytesIO(read_file('Crouch_brain.csv')))
    df_existing['date'] = pd.to_datetime(df_existing['date']).dt.date
    df_existing= df_existing[(df_existing['date'] < today_date) & (df_existing['date'] >= back5_date)]
    df_existing= df_existing[df_existing['flag'].str.contains('buy')]
    
    ##抓取基准涨幅作为参考特征
    index_rise = risk_bench(context)
    
    ##遍历信号，选股2，得到Sign_date及后一天，出现两连板的个股，记录price_T和T1_date
    for i in range(len(df_waiting)):
        stockcode = df_waiting['code'].values[i]
        Sign_date = df_waiting['date'].values[i]
        riselevel = df_waiting['riselevel'].values[i]
        if stockcode in df_existing['code'].values:  #不重复判断，不重复买卖
            continue
        #去掉TD间停复牌，以防数据异常
        df_suspened = get_price(stockcode, start_date=Sign_date, end_date=today_date, frequency='daily', fields='paused')['paused']
        suspened_sum = sum(df_suspened)
        if suspened_sum >0:
            continue
        S1_date = get_trade_days(start_date=Sign_date)[1]
        #刚到高点的暂不考虑
        if S1_date == today_date:
            continue
        df_price = get_price(stockcode, start_date=Sign_date, end_date=S1_date, frequency='daily', fields=['close','high_limit'])
        if (df_price['close'].values[0] < df_price['high_limit'].values[0]) or (df_price['close'].values[-1] < df_price['high_limit'].values[-1]):
            continue
        price_T = df_price['close'].values[-1]
        T1_date = get_trade_days(start_date=S1_date)[1]
        
    ##选股3，抓取T1_date到今天的价表，最高价不得过T日的20%，最低价低于0.8Max，且低于MA10和MA10;
        df_price = get_price(stockcode, start_date=T1_date, end_date=today_date, frequency='daily', fields=['high','low','close'])
        highmax_id, highmax_price = max(enumerate(df_price['high'][0:]), key=operator.itemgetter(1))
        lowmin_id, lowmin_price = min(enumerate(df_price['low'][0:]), key=operator.itemgetter(1))
        if highmax_price >1.2*price_T:
            continue
        if lowmin_price >0.8*highmax_price:
            continue
    ##得到极值的ID，便于取到日期，记录maxd_date和mind_date
        maxd_date = df_price.index[highmax_id].date() 
        price_maxd = df_price['close'].values[highmax_id]
        mind_date = df_price.index[lowmin_id].date()
        price_mind = df_price['close'].values[lowmin_id]
        MA5 = MA(stockcode, check_date=mind_date, timeperiod=5)
        MA10 = MA(stockcode, check_date=mind_date, timeperiod=10)
        if (price_mind > MA10[stockcode]) or (price_mind > MA5[stockcode]):
            continue
        
    ##选股4，自mind_date至今日，观察是否有出现站线或低点起收涨8点以上的信号，是为候选信号
    ##若自mind_date就是今日，则列入备关信号;不是则看今日的表现
        maxN_date = get_trade_days(start_date=maxd_date)[1]
        num_sellout =0
        df_price = get_price(stockcode, start_date=maxN_date, end_date=today_date, frequency='daily', fields=['pre_close','open','close','high'])
        for i in range(len(df_price)):
            if df_price['close'].values[i]/df_price['pre_close'].values[i] >1.05:
                num_sellout = num_sellout+1
                
        flag = None
        sink_date = mind_date
        while sink_date <=today_date:
            MA5 = MA(stockcode, check_date=sink_date, timeperiod=5)
            MA10 = MA(stockcode, check_date=sink_date, timeperiod=10)
            df_price = get_price(stockcode, count = 1, end_date=sink_date, frequency='daily', fields=['low', 'high', 'close'])
            up_level = df_price['close'].values[-1]/df_price['low'].values[-1]
            if (df_price['close'].values[-1] >= MA5[stockcode]) and (df_price['close'].values[-1] >= MA10[stockcode]):
                flag = 'prebuy'
                break
            elif up_level > 1.08:
                flag = 'prebuy'
                break
            sink_date = sink_date + datetime.timedelta(days=1)
            
    ##选股5，为信号们准备riselevel,PE,PB,POP,price,Du_TD,Du_MinD,num_sell等特征
        df_value = get_valuation(stockcode, end_date=today_date, count=1, fields=['circulating_market_cap','pe_ratio','pb_ratio'])
        if flag != 'prebuy' or sink_date != today_date:    #正式时应改为=='None'
            continue
        
        stock_name = get_security_info(stockcode).display_name
        cir_m = df_value['circulating_market_cap'].values[0]
        pe = df_value['pe_ratio'].values[0]
        pb = df_value['pb_ratio'].values[0]
        price_D = all_data[stockcode].last_price
        CYF_code = CYF(stockcode, check_date=today_date, N = 10, unit = '1d', include_now = True)
        pop = CYF_code[stockcode]
        dur_TD =len(get_trade_days(T1_date,today_date))
        dur_MinD =len(get_trade_days(mind_date,today_date))
            
    ##抓取S前，S-T，T-D的均换作为参考,构建对比to作为特征
        df_value = get_valuation(stockcode, end_date=Sign_date, count=20, fields=['turnover_ratio'])
        to_before = df_value['turnover_ratio'].values.mean()
        df_value = get_valuation(stockcode, start_date=Sign_date, end_date=T1_date, fields=['turnover_ratio'])
        to_T = df_value['turnover_ratio'].values.mean()
        df_value = get_valuation(stockcode, start_date=maxN_date, end_date=today_date, fields=['turnover_ratio'])
        to_after = df_value['turnover_ratio'].values.mean()
            
        to_TvsB = to_T/to_before
        to_AvsT = to_after/to_T
    
    ##选股6，八决策树选出买信，记录在brain中，并注明信号类型
    ##备关的决策判断条件，涉及时间周期减1
        if pb <0 or pb >10:
            continue
        
        if dur_MinD ==1 and dur_TD <20:
            if riselevel >1.1 and riselevel <1.8:
                if price_D >5 and price_D <40:
                    flag = 'buy_BL'
                    write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                    write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                    stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                    continue
        
        if up_level >1.08 and num_sellout <=2:
            if dur_MinD ==2 or dur_MinD ==3:
                if dur_TD >10 and dur_TD <20:
                    if index_rise >-0.005 and pop >82:
                        flag = 'buy_ML'
                        write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                        write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                        stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                        continue
                        
        if pb <2 and riselevel <1.4:
            if cir_m >27 and cir_m <60:
                if up_level >1.027 and index_rise >0.002:
                    if num_sellout <=1 and to_TvsB >2:
                        flag = 'buy_DG'
                        write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                        write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                        stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                        continue

        if to_TvsB >4 and to_TvsB <9 and to_AvsT>0.4:
            if cir_m <35 and price_D <10:
                if pe <70 and pe >-80:
                    if pop >66 and index_rise >0.002:
                        flag ='buy_BC'
                        write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                        write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                        stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                        continue
            
        if dur_TD <=6 and to_AvsT >0.75 and pop <94:
            if price_D >5 and price_D <40:
                flag = 'buy_JZ'
                write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                continue
                        
        if riselevel <1.1 and up_level >1.07:
            if to_TvsB >3 and pop <94 and index_rise >0.002:
                flag = 'buy_ZZ'
                write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                continue
                    
        if index_rise >0.01 and index_rise <0.03:
            if pop <70 and dur_TD <24:
                if dur_MinD <6 and cir_m >25:
                    flag = 'buy_HT'
                    write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
                    write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
                    stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
                    continue
        
        if riselevel <1.01 and up_level <1.1 and index_rise >0.002:
            flag ='buy_XD'
            write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s\n' % (today_date,flag,stockcode,pop,price_D)),append = True)
            write_file('Crouch_log.csv', str('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (T1_date,maxd_date,mind_date,today_date,flag,stockcode,
            stock_name,riselevel,cir_m,pe,pb,pop,price_D,up_level,index_rise,dur_TD,dur_MinD,num_sellout,to_TvsB,to_AvsT)),append = True)
            continue
    
    ##仓内卖信判断
    for stockcode in context.portfolio.positions:
        cost = context.portfolio.positions[stockcode].avg_cost
        price = context.portfolio.positions[stockcode].price
        value = context.portfolio.positions[stockcode].value
        intime= context.portfolio.positions[stockcode].init_time
        ret = price/cost - 1
        duration=len(get_trade_days(intime,today_date))
        rise_ratio = ret/duration
        
        #创板股提高盈速要求
        """
        if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
            rise_ratio = rise_ratio/2
            ret = ret/2
        """
        
        if ret < -0.1:
            write_file('Crouch_brain.csv',str('%s,sell,%s,ZS,%s\n' % (today_date,stockcode,price)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            write_file('Crouch_log.csv', str('%s,止损信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
        elif ret > 2:
            write_file('Crouch_brain.csv',str('%s,sell,%s,CY,%s\n' % (today_date,stockcode,price)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            write_file('Crouch_log.csv', str('%s,超盈信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
        
        if duration >=3 and ret <(-0.05+0.015*(duration-3))/g.roundratio:
            write_file('Crouch_brain.csv',str('%s,sell,%s,DQ,%s\n' % (today_date,stockcode,price)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            write_file('Crouch_log.csv', str('%s,短期信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
        
        if duration >=6 and g.roundratio*rise_ratio <(0.004*(2**(duration//4-1))):
            write_file('Crouch_brain.csv',str('%s,sell,%s,SS,%s\n' % (today_date,stockcode,price)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            write_file('Crouch_log.csv', str('%s,失速信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
        
        df_price = get_price(stockcode,count=3,end_date=today_date, frequency='daily', fields=['low_limit','high_limit','open','close','volume']) #先老后新
        df_value = get_valuation(stockcode, count=30, end_date=today_date, fields=['turnover_ratio'])#先新后老
        if df_price['close'].values[-1] <= df_price['low_limit'].values[-1]:
            #跌停中若当日量小于前两日，略过;反之就是只要有大就判
            if (df_price['volume'].values[-1] > df_price['volume'].values[-2]) or (df_price['volume'].values[-1] > df_price['volume'].values[-3]):
                write_file('Crouch_brain.csv',str('%s,sell,%s,DT,%s\n' % (today_date,stockcode,price)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
                write_file('Crouch_log.csv', str('%s,跌停信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
                continue
            
        to_30_max = df_value['turnover_ratio'].max()
        if (df_value['turnover_ratio'].values[0] > 2*df_value['turnover_ratio'].values[1]) and (df_value['turnover_ratio'].values[0] > 0.99*to_30_max):
            if (df_price['close'].values[-1] >= df_price['high_limit'].values[-1]) or (df_value['turnover_ratio'].values[0] <6) :   #天量中若当日涨停略过,换手绝对值低于6%也可略过
                continue
            write_file('Crouch_brain.csv',str('%s,sell,%s,TL,%s\n' % (today_date,stockcode,price)),append = True) #记录在任务清单中，方便明日盘前载入四大list执行
            write_file('Crouch_log.csv', str('%s,天量信号,%s,周期:%s,盈利:%s\n' % (today_date,stockcode,duration,ret)),append = True)
            continue
    
#判断三个月内是否爆炒过,两周内是否爆炒过。
def is_falls(context,stockcode):
    today_date = context.current_dt.date()    
    close_data = attribute_history(stockcode,60,'1d',['close'])
    close_min = min(close_data['close'])
    close_max = max(close_data['close'])
    close_dualweek = close_data['close'][-10]
    close_last = close_data['close'][-1]

    if (stockcode[0:3] == '688' or stockcode[0:3] == '300') and today_date >= datetime.date(2020,9,1):
        if (close_last >3*close_dualweek) or (close_max >4*close_min):
            return 0
    else:
        if (close_last >2*close_dualweek) or (close_max >3*close_min):
            return 0
            
    return close_last/close_min
    
# 通过基准涨跌和大盘跌停数量来判断风险
def risk_bench(context):
    today_date = context.current_dt.date()
    lastd_date = get_trade_days(end_date=today_date, count=2)[0]
    # 统计今日基准风险
    df_index = get_price(g.index, end_date=today_date,fields=['close'], count=3)
    index_rise = (df_index.close[-1] - df_index.close[-2]) / df_index.close[-2]
    
    return index_rise
    #log.info('昨日大盘涨幅：%.2f' % (index_rise))x
    # 统计跌停风险
    all_data = get_current_data()
    stocklist = list(get_all_securities(['stock'],date=today_date).index)
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].paused]
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].is_st]
    stocklist = [stockcode for stockcode in stocklist if'退' not in all_data[stockcode].name]
    stocklist = [stockcode for stockcode in stocklist if (today_date-get_security_info(stockcode).start_date).days>100]

    df_today_price = get_price(stocklist, end_date=today_date, frequency='daily', fields=['close', 'low_limit'], skip_paused=True, fq='pre', count=1, panel=False)
    df_lastd_price = get_price(stocklist, end_date=lastd_date, frequency='daily', fields=['close', 'low_limit'], skip_paused=True, fq='pre', count=1, panel=False)

    ##1.6.3 计算昨日和前日的跌停股数，若昨>前且>18，风大
    num_today_limit_down = len(df_today_price[df_today_price.close == df_today_price.low_limit])
    num_lastd_limit_down = len(df_lastd_price[df_lastd_price.close == df_lastd_price.low_limit])

    if (num_today_limit_down >= num_lastd_limit_down) and (num_today_limit_down > 18):
        print("风大扯呼,基准涨跌:%s,今日跌停:%s,昨日跌停%s" % (index_rise,num_today_limit_down,num_lastd_limit_down))
        return True
    else:
        return False
        
#测试时抓取未来价格做信号参考
def future_price(context,stockcode,flag,score):
    today_date = context.current_dt.date()
    
    fut_date_list = get_trade_days(start_date = today_date,end_date = '2020-12-11')
    if len(fut_date_list) >=30:
        fut30_date = get_trade_days(start_date= today_date)[29]
        fut10_date = get_trade_days(start_date= today_date)[9]
    elif len(fut_date_list) >=10:
        fut30_date = get_trade_days(start_date= today_date)[-1]
        fut10_date = get_trade_days(start_date= today_date)[9]
    else:
        fut30_date = get_trade_days(start_date= today_date)[-1]
        fut10_date = get_trade_days(start_date= today_date)[-1] 

    future10_price = get_price(stockcode, start_date=today_date, end_date=fut10_date, frequency='daily', fields=['open','close','pre_close'])
    future30_price = get_price(stockcode, start_date=today_date, end_date=fut30_date, frequency='daily', fields=['open','close'])
        
    next_catch = future10_price['open'].values[1]/future10_price['pre_close'].values[1] #隔日上车时的涨幅
    price_c = future10_price['open'].values[1]
    fut10_close_min = future10_price['close'].min()
    fut10_close_max = future10_price['close'].max()
    fut30_close_min = future30_price['close'].min()
    fut30_close_max = future30_price['close'].max()
    d10_drop = fut10_close_min/price_c
    d10_rise = fut10_close_max/price_c
    d30_drop = fut30_close_min/price_c
    d30_rise = fut30_close_max/price_c
    
    if next_catch <0.98:
        return
    else:
        if flag !='threebuy':
            write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s,%s,%s,%s\n' % (today_date,flag,score,stockcode,d10_drop,d10_rise,d30_drop,d30_rise)),append = True)
        else:
            if next_catch >0.99 and next_catch <1.03:
                write_file('Crouch_brain.csv', str('%s,%s,%s,%s,%s,%s,%s,%s\n' % (today_date,flag,score,stockcode,d10_drop,d10_rise,d30_drop,d30_rise)),append = True)


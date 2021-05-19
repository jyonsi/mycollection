# 克隆自聚宽文章：https://www.joinquant.com/post/32921
# 标题：价值选股
# 作者：逍遥222

# 导入函数库
from jqdata import *
import datetime as dt

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
    #持股数量
    g.stock_num=4

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    # 每月第5个交易日进行操作
    # 开盘前运行
    run_monthly(before_market_open,5,time='before_open', reference_security='000300.XSHG') 
    #run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    # 开盘时运行
    run_monthly(market_open,5,time='open', reference_security='000300.XSHG')
    #run_daily(market_open, time='open', reference_security='000300.XSHG')
    
    set_option('use_real_price',True)

## 开盘前运行函数
def before_market_open(context):
    log.info('###################################################')
    #获取满足条件的股票列表
    temp_list = get_stock_list(context)
    log.info('条件筛选：'+str(temp_list))
    #按市值进行排序
    g.buy_list = get_check_stocks_sort(context,temp_list)
    print("市值排序筛选："+str(g.buy_list));

## 开盘时运行函数
def market_open(context):
    #卖出不在买入列表中的股票
    sell(context,g.buy_list)
    #买入不在持仓中的股票，按要操作的股票平均资金
    buy(context,g.buy_list)

## 收盘后运行函数
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    #得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：'+str(_trade))
    log.info('一天结束')
    log.info('##############################################################')
    
#交易函数 - 买入
def buy(context, buy_lists):
    # 获取最终的 buy_lists 列表
    # 买入股票
    if len(buy_lists)>0:
        #分配资金
        cash = context.portfolio.available_cash/(len(buy_lists)*1.0)
        # 进行买入操作
        for s in buy_lists:
            order_value(s,cash)
            log.info('买入：'+str(s))
       
# 交易函数 - 出场
def sell(context, buy_lists):
    # 获取 sell_lists 列表
    hold_stock = context.portfolio.positions.keys()
    for s in hold_stock:
        #卖出不在买入列表中的股票
        if s not in buy_lists:
            order_target_value(s,0)  
            log.info('卖出：'+str(s))
    
#按市值进行排序   
#从大到小
def get_check_stocks_sort(context,check_out_lists):
    #过滤涨停
    check_out_lists = filter_limitup_stock(context, check_out_lists)
    df = get_fundamentals(query(valuation.circulating_cap,valuation.code,valuation.pe_ratio,valuation.market_cap).filter(valuation.code.in_(check_out_lists)),date=context.previous_date)
    #asc值为1，从小到大
    #df = df.sort('circulating_cap',ascending=1)
    out_lists = list(df['code'].values)[:g.stock_num]
    
    return out_lists

def get_stock_list(context):
    temp_list = list(get_all_securities(types=['stock']).index)  
    #temp_list=get_index_stocks('399932.XSHE')+get_index_stocks('000931.XSHG')+get_index_stocks('399997.XSHE')+get_index_stocks('399933.XSHE')
    #temp_list=get_index_stocks('399300.XSHE')
    #temp_list=["600519.XSHG"]
    #过滤停牌
    temp_list = filter_paused_stock(temp_list)
    #过滤ST股票
    temp_list = filter_st_stock(temp_list)
    #过滤黑名单
    temp_list = filter_blacklist_stock(context, temp_list)
    temp_list=set(temp_list)
    
    #获取当前月，如果当前月小于4月，则年报数据获取年为去年
    y=context.current_dt.year
    M=context.current_dt.month
    if M<=4:
        y=context.current_dt.year-1
    
    #近3年roe>15%
    roe = {}
    if len(temp_list)>0:
        df1 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,indicator.roe).filter(valuation.code.in_(temp_list)),statDate=str(y-1))
        df2 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,indicator.roe).filter(valuation.code.in_(temp_list)),statDate=str(y-2))
        df3 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,indicator.roe).filter(valuation.code.in_(temp_list)),statDate=str(y-3))
        df1 = df1[df1['roe']>15]['code'].values
        df2 = df2[df2['roe']>15]['code'].values
        df3 = df3[df3['roe']>15]['code'].values
        roe = list(set(df1)&set(df2)&set(df3))
    temp_list=set(roe)
    log.info('近3年roe>15%：'+str(len(temp_list)))
    
    #3年经营现金流净额>投资现金流净额
    XJL = {}
    if len(temp_list)>0:
        df1 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,cash_flow.net_operate_cash_flow,cash_flow.net_invest_cash_flow).filter(valuation.code.in_(temp_list)),statDate=str(y-1))
        df2 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,cash_flow.net_operate_cash_flow,cash_flow.net_invest_cash_flow).filter(valuation.code.in_(temp_list)),statDate=str(y-2))
        df3 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,cash_flow.net_operate_cash_flow,cash_flow.net_invest_cash_flow).filter(valuation.code.in_(temp_list)),statDate=str(y-3))
        df1 = df1[df1['net_operate_cash_flow']>df1['net_invest_cash_flow']]['code'].values
        df2 = df2[df2['net_operate_cash_flow']>df2['net_invest_cash_flow']]['code'].values
        df3 = df3[df3['net_operate_cash_flow']>df3['net_invest_cash_flow']]['code'].values
        XJL = list(set(df1)&set(df2)&set(df3))
    temp_list=set(XJL) 
    log.info('3年经营现金流净额>投资现金流净额：'+str(len(temp_list)))
    
    #4年核心利润>净利润 核心利润=营业收入－营业成本－税金及附加－销售费用－管理费用－财务费用-购建固定资产、无形资产和其他长期资产所支付的现金
    HXLR = {}
    if len(temp_list)>0:
        df1 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,income.net_profit,income.operating_revenue,income.operating_cost,income.operating_tax_surcharges,income.sale_expense,income.administration_expense,income.financial_expense,cash_flow.fix_intan_other_asset_acqui_cash).filter(valuation.code.in_(temp_list)),statDate=str(y-1))
        df2 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,income.net_profit,income.operating_revenue,income.operating_cost,income.operating_tax_surcharges,income.sale_expense,income.administration_expense,income.financial_expense,cash_flow.fix_intan_other_asset_acqui_cash).filter(valuation.code.in_(temp_list)),statDate=str(y-2))
        df3 = get_fundamentals(query(cash_flow.code,cash_flow.statDate,income.net_profit,income.operating_revenue,income.operating_cost,income.operating_tax_surcharges,income.sale_expense,income.administration_expense,income.financial_expense,cash_flow.fix_intan_other_asset_acqui_cash).filter(valuation.code.in_(temp_list)),statDate=str(y-3))
        df1 = df1[df1['operating_revenue']-df1['operating_cost']-df1['operating_tax_surcharges']-df1['sale_expense']-df1['administration_expense']-df1['financial_expense']-df1['fix_intan_other_asset_acqui_cash']>-df1['net_profit']]['code'].values
        df2 = df2[df2['operating_revenue']-df2['operating_cost']-df2['operating_tax_surcharges']-df2['sale_expense']-df2['administration_expense']-df2['financial_expense']-df2['fix_intan_other_asset_acqui_cash']>-df2['net_profit']]['code'].values
        df3 = df3[df3['operating_revenue']-df3['operating_cost']-df3['operating_tax_surcharges']-df3['sale_expense']-df3['administration_expense']-df3['financial_expense']-df3['fix_intan_other_asset_acqui_cash']>-df3['net_profit']]['code'].values
        HXLR = list(set(df1)&set(df2)&set(df3))
    temp_list=set(HXLR) 
    log.info('4年核心利润>净利润：'+str(len(temp_list)))


    #获取多期财务数据
    panel = get_data(temp_list,4)
        
    #5.近四季度净利润增长率介于15%至50%
    #计算必须是单季度，比如计算第四季度数据时必须减去三季度值与去年的四季度数据减去三季度数据对比
    l5 = {}
    for i in range(4):
        df_5 = panel.iloc[:,i,:]
        df_temp_5 = df_5[(df_5['inc_net_profit_year_on_year']>15)&(df_5['inc_net_profit_year_on_year']<50)]
        if i == 0:    
            l5 = set(df_temp_5.index)
        else:
            l_temp = df_temp_5.index
            l5 = l5 & set(l_temp)
    temp_list = set(l5)
    log.info('近四季度净利润增长率介于15%至50%：'+str(len(temp_list)))
    
    #4.获取最近一季度PEG，从小到大排序，PEG<1.2,
    PEG_list = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG(context, temp_list)
    # 将股票按PEG降序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=['peg'], ascending=[1])
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
            if df<75:
                PEG_list.append(df_sort_PEG.index[i])
    temp_list = set(PEG_list)
    log.info('最近一季度PEG<1.2：'+str(len(temp_list)))
        
    return temp_list

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
    
#获取多期财务数据内容
def get_data(pool, periods):
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

    q = query(valuation.code,valuation.code,valuation.circulating_market_cap,balance.total_current_assets,\
    indicator.inc_net_profit_year_on_year
              )
    stat_date_panels = { sd:None for sd in stat_dates }

    for sd in stat_dates:
        quarters = [sd[:4]+'q'+str(int(sd[5:7])/3)]
        for i in range(periods-1):
            quarters.append(quarter_push(quarters[-1]))
        nq = q.filter(valuation.code.in_(stat_date_stocks[sd]))
        pre_panel = { quarter:get_fundamentals(nq, statDate = quarter) for quarter in quarters }
        for thing in pre_panel.values():
            thing.index = thing.code.values
        panel = pd.Panel(pre_panel)
        panel.items = range(len(quarters))
        stat_date_panels[sd] = panel.transpose(2,0,1)

    final = pd.concat(stat_date_panels.values(), axis=2)
    return final.dropna(axis=2)
  
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
     
    
#过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list 
        if not current_data[stock].is_st 
        and 'ST' not in current_data[stock].name 
        and '*' not in current_data[stock].name 
        and '退' not in current_data[stock].name]
        
# 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]  
 
    
#过滤涨停的股票
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    
    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() 
        or last_prices[stock][-1] < current_data[stock].high_limit]
    
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
    
    
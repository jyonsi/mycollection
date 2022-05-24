
# import needed modules
import time
import datetime
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
start = datetime.date(2015,1,1)
end = datetime.date.today()

# ticker_info = yf.Ticker("9984.T")
# hist = ticker_info.history(period="max")
# VYM VTI VNQI VNQ
# tickerList = ['VT','BND','','GLD','','','VIG','']
tickerList = ['VT','BND','GLD','VIG','PDBC','VNQ']
data = yf.download(tickerList, start=start, end=end)["Adj Close"]
data

# calculate daily and annual returns of the stocks
#returns_daily = table.pct_change()
returns_daily = data.pct_change()
returns_annual = returns_daily.mean() * 250

# get daily and covariance of returns of the stock
cov_daily = returns_daily.cov()
cov_annual = cov_daily * 250

# empty lists to store returns, volatility and weights of imiginary portfolios
port_returns = []
port_volatility = []
sharpe_ratio = []
stock_weights = []

# set the number of combinations for imaginary portfolios
num_assets = len(tickerList)
num_portfolios = 50000

#set random seed for reproduction's sake
np.random.seed(101)

weights = np.random.random(num_assets)
weights /= np.sum(weights)

# populate the empty lists with each portfolios returns,risk and weights
for single_portfolio in range(num_portfolios):
   weights = np.random.random(num_assets)
   weights /= np.sum(weights)
   returns = np.dot(weights, returns_annual)
   volatility = np.sqrt(np.dot(weights.T, np.dot(cov_annual, weights)))
   sharpe = returns / volatility # decrease risk free return
   sharpe_ratio.append(sharpe)
   port_returns.append(returns)
   port_volatility.append(volatility)
   stock_weights.append(weights)
   
# a dictionary for Returns and Risk values of each portfolio
portfolio = {'Returns': port_returns,
            'Volatility': port_volatility,
            'Sharpe Ratio': sharpe_ratio}
            
# extend original dictionary to accomodate each ticker and weight in the portfolio
for counter,symbol in enumerate(tickerList):
   portfolio[symbol+' Weight'] = [Weight[counter] for Weight in stock_weights]

# make a nice dataframe of the extended dictionary
df = pd.DataFrame(portfolio)

# get better labels for desired arrangement of columns
column_order = ['Returns', 'Volatility', 'Sharpe Ratio'] + [stock+' Weight' for stock in tickerList]

# reorder dataframe columns
df = df[column_order]

# # plot frontier, max sharpe & min Volatility values with a scatterplot
# plt.style.use('seaborn-dark')
# df.plot.scatter(x='Volatility', y='Returns', c='Sharpe Ratio',
#                cmap='RdYlGn', edgecolors='black', figsize=(10, 8), grid=True)
# plt.xlabel('Volatility (Std. Deviation)')
# plt.ylabel('Expected Returns')
# plt.title('Efficient Frontier')
# plt.show()


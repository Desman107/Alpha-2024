# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 17:26:48 2024

@author: Dell
"""

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
from datetime import datetime
import random
import numpy as np
import seaborn as sns

path = 'E:/量化/因子/CFL9.xlsx'
data = pd.read_excel(path)
data=data.rename(columns={'时间':'Date','开盘':'Open','最高':'High','最低':'Low','收盘':'Close','成交量':'Volume'})

data['growth_5']=data['Close']-data['Close'].shift(5)
data['growth_10']=data['Close']-data['Close'].shift(10)
data['growth_20']=data['Close']-data['Close'].shift(20)



data['return_1d']=(data['Close']-data['Close'].shift(5))/data['Close'].shift(5)
# df=data.loc[:,['Date','Open','High','Low','Close','Volume']]
# # df=df.rename(columns={'时间':'Date','开盘':'Open','最高':'High','最低':'Low','收盘':'Close','成交量':'Volume'})
# df['Date'] = pd.to_datetime(df['Date'])
# df.set_index('Date', inplace=True)

# df1=df.iloc[:60,:]
# mpf.plot(df1, type='candle', style='yahoo', volume=True, show_nontrading=False)

# # 显示图形
# plt.show()

def bit(data):
    if data>=0:
        return 1
    elif data<0:
        return 0
    else:
        return None
    
def state(data):
    if data==0:
        return 'decreasing'
    elif data==7:
        return 'increasing'
    else:
        return 'flatuating'



data['growth_5_bit']=data['growth_5'].apply(bit)
data['growth_10_bit']=data['growth_10'].apply(bit)
data['growth_20_bit']=data['growth_20'].apply(bit)
data['growth_state']=data['growth_5_bit']*4+data['growth_10_bit']*2+data['growth_20_bit']
data['state']=data['growth_state'].apply(state)

colors = {'increasing': 'r', 'flatuating': 'b', 'decreasing': 'g'}
data['Color'] = data['state'].map(colors)


t=random.randint(100,4000)
df=data.iloc[t:t+600,:]
df['Date'] = pd.to_datetime(df['Date'])
plt.scatter(df['Date'], df['Close'],c=df['Color'],alpha=0.5)

# 添加图例
# plt.legend()

# 显示图形
plt.show()


def graph(data):
    # 计算状态转移概率
    data['next_growth_state'] = data['growth_state'].shift(-1)  # 为计算转移概率，添加下一个状态列
    transition_counts = data.groupby(['growth_state', 'next_growth_state']).size().reset_index(name='count')
    total_transitions_per_growth_state = transition_counts.groupby('growth_state')['count'].sum().reset_index(name='total_transitions')
    transition_counts = transition_counts.merge(total_transitions_per_growth_state, on='growth_state')
    transition_counts['transition_probability'] = transition_counts['count'] / transition_counts['total_transitions']
    return transition_counts
        
transition_counts = graph(data)
transition_mixtur = np.zeros([8,8])
for i in transition_counts.index:
    row=int(transition_counts.iloc[i,0])
    col=int(transition_counts.iloc[i,1])
    pro=transition_counts.iloc[i,4]
    transition_mixtur[row,col]=pro
sns.heatmap(transition_mixtur, annot=True, cmap='coolwarm')
plt.show()



increase_rate=data[data['growth_state']==7]['return_1d']
plt.hist(increase_rate,bins=60,edgecolor='black',color='lightskyblue')

import pandas as pd
from sqlalchemy import create_engine
import os
engine = create_engine('mysql://root:123456@localhost/data')
dir = '../data/factor_data'
file_list = os.listdir(dir)
table_name = 'price'
merge_df = pd.DataFrame()
for i in range(len(file_list)):
    file_path = os.path.join(dir, file_list[i])
    if os.path.isfile(file_path):
        column_name = os.path.split(file_path)[-1].split('.')[0].replace(' ', '_')  
        print(column_name)
        df = pd.read_csv(file_path,keep_default_na=False, encoding='utf-8')
        df = df.sort_index(axis = 1)
        df = df.set_index('Date')
        df = df.stack()
        df = df.reset_index()
        df.columns = ['Date','Code',column_name]
        if(column_name == 'adj_factor'):
            merge_df = df
        else:
            merge_df = pd.merge(merge_df, df,on=['Date','Code'],how = 'left')    
        print(merge_df.head)
        # df.to_sql(table_name, engine, if_exists='append', index=False)
merge_df.to_sql(table_name, engine, if_exists='append', index=False)




# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 22:30:15 2024

@author: Dell
"""
# -*- coding:utf-8 -*-
import csv
import os
import numpy as np
import pandas as pd
import pymysql
from pymysql import connect
 
 
class CsvToMysql(object):
    def __init__(self, hostname, port, user, passwd, db):
        self.dbname = db
        self.conn = connect(host=hostname, port=port, user=user, passwd=passwd, db=db)
        self.cursor = self.conn.cursor()
 
 
    def read_csv(self,filename):
        df = pd.read_csv(filename, keep_default_na=False, encoding='utf-8')
        df = df.sort_index(axis = 1) #根据列标签对datafame进行排序
        table_name = '`'+os.path.split(filename)[-1].split('.')[0].replace(' ', '_') + '`'
        self.csv2mysql(db_name=self.dbname,table_name=table_name, df=df )
 
    
    def read_df(self,table_name,df):
        # df = pd.read_csv(filename, keep_default_na=False, encoding='utf-8')
        # table_name = '`'+os.path.split(filename)[-1].split('.')[0].replace(' ', '_') + '`'
        self.csv2mysql(db_name=self.dbname,table_name=table_name, df=df )
        
 
    def make_table_sql(self,df):
        #将csv中的字段类型转换成mysql中的字段类型
        columns = df.columns.tolist()
        types = df.dtypes
        types = types.astype(str)
        print(types)
        make_table = []
        make_field = []
        for item in columns:
            item1 = '`'+item.replace(' ', '_').replace(':','')+'`'
            if 'datetime' in types[item] or 'Date' in item:
                char = item1 + ' DATETIME'
            elif 'int' in types[item]:
                char = item1 + ' INT'
            elif 'float' in types[item]:
                char = item1 +' FLOAT'
            elif 'object' in types[item]:
                char = item1 +' VARCHAR(255)'
            # elif 'datetime' in types[item] or 'Data' in item:
            #     char = item1 + ' DATETIME'
            else:
                char = item1 + ' VARCHAR(255)'
            #char = item1 + ' VARCHAR(255)'
            make_table.append(char)
            make_field.append(item1)
        return ','.join(make_table), ','.join(make_field)
 
 
    def csv2mysql(self,db_name,table_name,df):
        field1, field2 = self.make_table_sql(df)
        print( "create table {} (id int AUTO_INCREMENT not null primary key, {})".format(table_name,field1))
        self.cursor.execute('drop table if exists {}'.format(table_name))
        self.cursor.execute("create table {} (id int AUTO_INCREMENT not null primary key,{})".format(table_name, field1))
        values = df.values.tolist()
        s = ','.join(['%s' for _ in range(len(df.columns))])
        try:
            print( len(values[0]),len(s.split(',')))
            print( 'insert into {}({}) values ({})'.format(table_name, field2, s), values[0])
            self.cursor.executemany('insert into {}({}) values ({})'.format(table_name, field2, s), values)
        except Exception as e:
            print( e.message)
        finally:
            self.conn.commit()
    def read_folder(self,dir):
        file_list = os.listdir(dir)
        for i in range(len(file_list)):
            file_path = os.path.join(dir, file_list[i])
            if os.path.isfile(file_path):
                self.read_csv(file_path)
 
if __name__ == "__main__":
    hostname = '127.0.0.1'
    port = 3306
    user = 'root'
    passwd = '123456'
    db = 'data'
    M = CsvToMysql(hostname=hostname, port=port, user=user, passwd=passwd, db=db)
    


 
import duckdb
import pandas as pd
import os
from DuckDataModifier import DataSaver
import datetime
import time
class InfoDataReader:
    def __init__(self):
        factor_db_path = DataSaver.get_db_path()['factor_db']
        self.con = duckdb.connect(database=factor_db_path, read_only=True)


    def get_symbol_industry(self,symbol):
        
        query = f"""
        SELECT *
        FROM LC_ExgIndustry_SW_New
        WHERE SecuCode = '{symbol}' ;
        """
        result = self.con.execute(query)
        # df = pd.DataFrame(result.fetchall(), columns=result.description)
        df = result.fetchdf()
        return df
    

    # 获取查询日期时是st的股票
    def get_st_sql(self,date) -> pd.DataFrame:
        query = """
        WITH FilteredTrades AS (
        SELECT *,
                ROW_NUMBER() OVER (PARTITION BY InnerCode ORDER BY ChangeDate DESC) as rn
        FROM LC_SpecialTrade
        WHERE ChangeDate <= ?
        ),
        SelectedTrades AS (
        SELECT *
        FROM FilteredTrades
        WHERE rn = 1
        ),
        FinalSelection AS (
        SELECT SecuCode, SecuAbbr
        FROM SelectedTrades
        WHERE ChangeType IN (1, 3, 5, 7, 8, 9, 10) OR SecuAbbr LIKE '%ST%'
        )
        SELECT * FROM FinalSelection;
        """

        
        result = self.con.execute(query, (date,))


        df = result.fetch_df()
        return df
    
    # 获取查询日期时是st的股票
    def get_st_pd(self,date) -> pd.DataFrame:
        query = """
        SELECT * 
        FROM LC_SpecialTrade
        """
        result = self.con.execute(query)
        df = result.fetchdf()
        df = df[df['ChangeDate'] <= date]
        df = df.groupby("InnerCode", group_keys = None).tail(1)
        df = df[(df['ChangeType'].isin([1, 3, 5, 7, 8, 9, 10])) | (df['SecuAbbr'].str.contains('ST'))]
        df = df[['SecuCode', 'SecuAbbr']]
        return df
    
    #获取查询日期已上市的股票
    def get_a_shares(self,data) -> pd.DataFrame:
        query = """
        SELECT *
        FROM LC_ListStatus
        """
        result = self.con.execute(query)
        df = result.fetchdf()
        df = df[df['ChangeDate'] <= data]
        df = df.groupby("InnerCode", group_keys = None).tail(1)
        df = df[df.ChangeType.isin([1, 2, 3, 6])]
        df = df[['SecuCode', 'SecuAbbr']]
        return df

class FactorDataReader:
    def __init__(self):
        factor_db_path = DataSaver.get_db_path()['kline_sp_factor_db']
        self.con = duckdb.connect(database=factor_db_path, read_only=True)
        table_list = self.con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
        print('table available:{}'.format(table_list))
    def get_factor(self, factor_name, start_date, end_date, symbol_list):
        #查询时仅需要股票数字代码，不需要交易所代码
        prefix = 'A_'
        symbol_list = [prefix + symbol for symbol in symbol_list]
        symbol_list = [symbol + '_SH' if symbol[0] == '6' else symbol + '_SZ' for symbol in symbol_list]
        
        query = f"""
        SELECT Date,{','.join(symbol_list)}
        FROM {factor_name}
        WHERE Date BETWEEN '{start_date}' AND '{end_date}';
        """
        # print(query)
        result = self.con.execute(query)
        df = result.fetchdf()
        return df

    

if __name__ == '__main__':
    
    # reader = InfoDataReader()
    # df = reader.get_symbol_industry('000001.SZ')
    # print(df.head)
    # df1 = reader.get_st_sql('2021-01-01')
    # print(df1)
    # df2 = reader.get_st_pd('2021-01-01')
    # print(df2)
    # df3 = reader.get_a_shares('2021-01-01')
    # print(df3)
    # df3_fliter = df3[~df3['SecuCode'].isin(df2['SecuCode'])]
    # print(df3_fliter)
    start_time = time.time()
    factor_reader = FactorDataReader()
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")
    start_time = time.time()
    df4 = factor_reader.get_factor('money', '2017-01-01', '2017-02-28', ['000001','000004','000005'])
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")
    # print(df4)
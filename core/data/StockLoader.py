import pandas as pd
from sqlalchemy import create_engine

class StockDataLoader(object):
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:123456@localhost:3306/data')

    """
    securities: 字符串列表,代表股票代码，如['000001','600001']
    start_date: 字符串，代表开始日期
    end_date: 字符串，代表结束日期
    fields: 字符串列表，代表要查询的字段，如['close', 'open']
    """
    def get_price(self, securities: list, start_date: str, end_date: str, fields: list):
        df_read = pd.DataFrame()  # 初始化 DataFrame
        fields.insert(0,'Date')
        for security in securities:
            # 区分交易所
            if security[0] == '6':
                security = security + '_SH'
            else:
                security = security + '_SZ'

            # fields.insert(0,'Date')
            # SQL 查询语句
            sql_query = 'SELECT {} FROM {} WHERE Date BETWEEN "{}" AND "{}";'.format(','.join(fields), security, start_date, end_date)
            df = pd.read_sql_query(sql_query, self.engine)
            # df['code'] = security
            df.insert(0, 'code', security)
            # 执行查询并将结果读取到 DataFrame 中
            df_read = pd.concat([df_read, df],axis=0 , ignore_index=True)

        return df_read

#
if __name__ == '__main__':
    S = StockDataLoader()
    df = S.get_price(['600004','600006'],'2017-09-04','2017-09-05',['open','close','low'])
    print(df)

    print(df['code'].unique())
    
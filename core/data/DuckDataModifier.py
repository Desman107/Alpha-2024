import duckdb
import pandas as pd
import os
class DataSaver:
    def __init__(self):
        self.factor_db_path = self.get_db_path()['factor_db']
        self.kline_db_path = self.get_db_path()['kline_db']
        self.kline_sp_factor_data = self.get_db_path()['kline_sp_factor_db']
        self.kline_per_day_postfix = 'd_kline_'
        

    """
    静态方法，返回数据库路径
    建议使用绝对路径
    返回值是dict
    """
    @staticmethod
    def get_db_path() -> dict:
        db_path = {
            'factor_db' : 'E:/量化/QuantAlpha-2024/data/duckDB/stock.db',
            'kline_db' : 'E:/量化/QuantAlpha-2024/data/duckDB/kline.db',
            'kline_sp_factor_db' : 'E:/量化/QuantAlpha-2024/data/duckDB/kline_sp_factor.db',
        }
        return db_path
    
    @staticmethod
    def get_csv_path() -> dict:
        csv_path = {
            'factor_data' : 'E:/量化/QuantAlpha-2024/data/factor_data',
            'kline_data' : 'E:/量化/QuantAlpha-2024/data/kline_per_day',
            'kline_sp_factor_data' : 'E:/量化/QuantAlpha-2024/data/dkline_sp_factor',
        }
        return csv_path
    
    def factor_data_save(self):
        con = duckdb.connect(database=self.factor_db_path, read_only=False)
        factor_data_path = self.get_csv_path()['factor_data']
        for root, dirs, files in os.walk(factor_data_path):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    table_name = os.path.splitext(file)[0]
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{os.path.join(root, file)}');")
        con.close()

    def factor_data_save1(self):
        con = duckdb.connect(database=self.factor_db_path, read_only=False)
        factor_data_path = self.get_csv_path()['factor_data']
        for root, dirs, files in os.walk(factor_data_path):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    temp_table_name = "temp_table"
                    table_name = os.path.splitext(file)[0]
                    # 导入CSV数据到临时表
                    con.execute(f"CREATE TABLE {temp_table_name} AS SELECT * FROM read_csv_auto('{os.path.join(root, file)}');")
                    # 获取临时表的列名，并替换列名中的点为下划线
                    columns = con.execute(f"PRAGMA table_info({temp_table_name});").fetchall()
                    new_column_names = [col[1].replace('.', '_') for col in columns]  # 假设列名在返回的tuple的第二个位置
                    # 构建新的CREATE TABLE语句，包含更新后的列名
                    column_definitions = ", ".join([f"{new_name} {col[2]}" for new_name, col in zip(new_column_names, columns)])
                    con.execute(f"CREATE TABLE {table_name} ({column_definitions});")
                    # 将数据从临时表插入到新表，使用替换后的列名
                    column_names_joined = ", ".join(new_column_names)
                    temp_column_names_joined = ", ".join([col[1] for col in columns])
                    con.execute(f"INSERT INTO {table_name} ({column_names_joined}) SELECT {temp_column_names_joined} FROM {temp_table_name};")
                    # 删除临时表
                    con.execute(f"DROP TABLE {temp_table_name};")
        con.close()


    

    def kline_data_save(self):
    
        con = duckdb.connect(database=self.kline_db_path, read_only=False)
        kline_data_path = self.get_csv_path()['kline_data']
        
        for root, dirs, files in os.walk(kline_data_path):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':

                    # 生成表名
                    #duckdb不支持表名中有点号，所以需要替换
                    #duckdb不支持表名以数字开头，所以需要加前缀
                    table_name = self.kline_per_day_postfix + os.path.splitext(file)[0]
                    print(table_name)
                    # print(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{os.path.join(root, file)}');")
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{os.path.join(root, file)}');")
        
        con.close()
    
    #这个存储时会导致列名包含.的情况，现在已经作废
    def kline_sp_factor_data_save1(self):
        con = duckdb.connect(database=self.kline_sp_factor_data, read_only=False)
        kline_sp_factor_data_path = self.get_csv_path()['kline_sp_factor_data']
        for root, dirs, files in os.walk(kline_sp_factor_data_path):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    table_name = os.path.splitext(file)[0]
                    print(table_name)
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{os.path.join(root, file)}');")
        con.close()
    
    

    def kline_sp_factor_data_save(self):
        con = duckdb.connect(database=self.kline_sp_factor_data, read_only=False)
        factor_data_path = self.get_csv_path()['kline_sp_factor_data']
        prefix = 'A_'
        for root, dirs, files in os.walk(factor_data_path):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    print(file)
                    table_name = os.path.splitext(file)[0]
                    df = pd.read_csv(os.path.join(root, file))
                    col = df.columns
                    col_modify = [prefix+i.replace('.', '_') for i in col[1:]]
                    col_modify.insert(0,col[0])
                    df.columns = col_modify
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df;")
        con.close()

if __name__ == '__main__':
    modifier = DataSaver()
    # modifier.factor_data_save()
    modifier.kline_sp_factor_data_save()
    

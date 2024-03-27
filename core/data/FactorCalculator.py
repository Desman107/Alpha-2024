import pymysql


class FactorCalculator(object):
    def __init__(self) :
        
        # 建立数据库连接
        self.connection = pymysql.connect(
            host='localhost',  # 数据库主机名
            port=3306,               # 数据库端口号，默认为3306
            user='root',             # 数据库用户名
            passwd='123456',         # 数据库密码
            db='data',               # 数据库名称
            charset='utf8'           # 字符编码
        )
    def factorCalculate(self,formula : str, factor_name : str):
        # 创建游标对象
        cursor = self.connection.cursor()
        # 检查列是否存在
        cursor.execute("SHOW COLUMNS FROM factor LIKE %s", (factor_name,))
        existing_column = cursor.fetchone()

        if not existing_column:
            # 如果列不存在，则添加新列
            cursor.execute("ALTER TABLE factor ADD {} FLOAT;".format(factor_name))
        # 查询所有表格
        cursor.execute("SHOW TABLES")

        # 获取所有表格名
        tables = cursor.fetchall()

        # 遍历表格名
        for table in tables:
            if(len(table[0].split('_'))>1):
                if(table[0].split('_')[1] in ['sh','sz']):
                    print(table[0])
                    # INSERT INTO destination_table (column_a, column_b, column_c)
                    # SELECT column1, column2, column3
                    # FROM source_table
                    # WHERE condition;
                    # sql = "INSERT INTO factor {} SELECT {} FROM {} ;".format(factor_name, formula, str(table[0]))
                    sql = '''
                    INSERT INTO factor (code, Date, {})
                    SELECT
                        {},
                        Date,
                        CASE
                            WHEN (money) <> 0 THEN {}
                            ELSE NULL
                        END AS {}
                    FROM {};
                    '''.format(factor_name,"'{}'".format(table[0]),formula,factor_name,table[0])
                    cursor.execute(sql)


if __name__ == '__main__':
    formula = "((close - low) / (high - low)) * 100"    
    F = FactorCalculator()
    F.factorCalculate(formula, 'RSV')

    
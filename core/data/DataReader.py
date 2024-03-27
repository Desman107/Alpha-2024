import joblib
import pandas as pd
import os

class DataReader:
    # def __init__(self, data_path):
        # self.data_path = data_path
    @staticmethod
    def read(path):
        return joblib.load(data_path)
    
    @staticmethod
    def read_folder(path):
        for root,dirs, files in os.walk(path):
            for file in files:
                if len(file.split('.')) == 1:
                    df =  joblib.load(os.path.join(root, file))
                    df.to_csv(os.path.join(root,file + '.csv'))  

if __name__ == '__main__':
    data_path = 'data/factor_data'
    DataReader.read_folder(data_path)
    # print(os.path.abspath(data_path))
    # a = os.path.abspath(data_path)
    # for root, dirs, files in os.walk(data_path):
    #     print(root)
    #     print(dirs)
    #     print(files)
    #     print('*'*50)
    # reader = DataReader(data_path)
    # df = reader.read()
    # # print(reader)

    # df = joblib.load(data_path)
    # print(df.head)



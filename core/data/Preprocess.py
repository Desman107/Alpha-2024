import pandas as pd
import os
import numpy as np
import datetime
import duckdb
from DuckDataReader import InfoDataReader

def rankStandardize(df : pd.DataFrame):
    # 标准化处理
    df = df.rank(axis=1, method='max', pct=True)
    return df

def directStandardize(df : pd.DataFrame):
    # 标准化处理
    df = df.rank(axis=1, method='max', pct=True)
    return df
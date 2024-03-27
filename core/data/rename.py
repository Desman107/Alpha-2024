import os
   
path = 'E:/量化/QuantAlpha-2024/data/kline_per_day'   
 
#获取该目录下所有文件，存入列表中
f= os.listdir(path)
#print(os.path.abspath(path))
for i in f:
    
    #设置旧文件名（就是路径+文件名）
    old_name = os.path.join(path, i)
    
    
    #设置新文件名
    # new_name = os.path.join(path, i.split('_')[1])
    new_name = os.path.join(path, i.replace('.', '_', 1))
    #用os模块中的rename方法对文件改名
    os.rename(old_name,new_name)